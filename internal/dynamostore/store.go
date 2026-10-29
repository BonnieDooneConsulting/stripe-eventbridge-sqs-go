// Package dynamostore is an events.EventStore backed by a DynamoDB table, so
// every worker shares one record of which Stripe events are already handled.
//
// The table needs a string partition key named event_id, and TTL enabled on
// the expires_at attribute so old records clean themselves up:
//
//	event_id    (S)  partition key, the Stripe event ID (evt_...)
//	status      (S)  "in_progress" or "done"
//	lease_until (N)  unix seconds; an in-progress claim older than this is stale
//	expires_at  (N)  unix seconds; DynamoDB's TTL deletes the item after this
package dynamostore

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
)

const (
	statusInProgress = "in_progress"
	statusDone       = "done"
)

// API is the part of the DynamoDB client the store uses.
type API interface {
	PutItem(ctx context.Context, in *dynamodb.PutItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error)
	DeleteItem(ctx context.Context, in *dynamodb.DeleteItemInput, optFns ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error)
}

// Store implements events.EventStore.
type Store struct {
	client       API
	table        string
	ttl          time.Duration
	claimTimeout time.Duration
	now          func() time.Time
}

var _ events.EventStore = (*Store)(nil)

// New returns a Store using table. Processed event IDs are kept for ttl
// (make it longer than Stripe's three-day retry window), and an in-progress
// claim goes stale after claimTimeout so a crashed worker doesn't block the
// event forever. Set claimTimeout a little longer than the handler timeout.
func New(client API, table string, ttl, claimTimeout time.Duration) *Store {
	return &Store{client: client, table: table, ttl: ttl, claimTimeout: claimTimeout, now: time.Now}
}

// Claim writes an in-progress record with a conditional PutItem. The
// condition makes it atomic: if two workers race on the same event, DynamoDB
// accepts exactly one write and rejects the other.
func (s *Store) Claim(ctx context.Context, eventID string) (bool, error) {
	now := s.now()
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"event_id":    &types.AttributeValueMemberS{Value: eventID},
			"status":      &types.AttributeValueMemberS{Value: statusInProgress},
			"lease_until": unix(now.Add(s.claimTimeout)),
			"expires_at":  unix(now.Add(s.ttl)),
		},
		// Claim it if nobody has, or if the last claim was abandoned
		// mid-flight (the worker crashed without releasing it).
		ConditionExpression: aws.String("attribute_not_exists(event_id) OR (#status = :in_progress AND lease_until < :now)"),
		ExpressionAttributeNames: map[string]string{
			"#status": "status",
		},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":in_progress": &types.AttributeValueMemberS{Value: statusInProgress},
			":now":         unix(now),
		},
	})
	if err != nil {
		var failed *types.ConditionalCheckFailedException
		if errors.As(err, &failed) {
			return false, nil
		}
		return false, fmt.Errorf("dynamodb claim %s: %w", eventID, err)
	}
	return true, nil
}

// Complete marks the event as done and resets its expiry.
func (s *Store) Complete(ctx context.Context, eventID string) error {
	_, err := s.client.PutItem(ctx, &dynamodb.PutItemInput{
		TableName: aws.String(s.table),
		Item: map[string]types.AttributeValue{
			"event_id":   &types.AttributeValueMemberS{Value: eventID},
			"status":     &types.AttributeValueMemberS{Value: statusDone},
			"expires_at": unix(s.now().Add(s.ttl)),
		},
	})
	if err != nil {
		return fmt.Errorf("dynamodb complete %s: %w", eventID, err)
	}
	return nil
}

// Release deletes an in-progress claim so the next delivery can try again.
// It never deletes a completed record.
func (s *Store) Release(ctx context.Context, eventID string) error {
	_, err := s.client.DeleteItem(ctx, &dynamodb.DeleteItemInput{
		TableName: aws.String(s.table),
		Key: map[string]types.AttributeValue{
			"event_id": &types.AttributeValueMemberS{Value: eventID},
		},
		ConditionExpression:      aws.String("#status = :in_progress"),
		ExpressionAttributeNames: map[string]string{"#status": "status"},
		ExpressionAttributeValues: map[string]types.AttributeValue{
			":in_progress": &types.AttributeValueMemberS{Value: statusInProgress},
		},
	})
	if err != nil {
		var failed *types.ConditionalCheckFailedException
		if errors.As(err, &failed) {
			return nil
		}
		return fmt.Errorf("dynamodb release %s: %w", eventID, err)
	}
	return nil
}

func unix(t time.Time) *types.AttributeValueMemberN {
	return &types.AttributeValueMemberN{Value: strconv.FormatInt(t.Unix(), 10)}
}

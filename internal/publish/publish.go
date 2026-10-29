// Package publish puts Stripe events onto an EventBridge bus in the same
// shape Stripe's EventBridge destination uses. It's for local development:
// in production, Stripe does this part for you.
package publish

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
)

// LocalSource is the EventBridge source used for local events. Real Stripe
// events use aws.partner/stripe.com/<your destination ID>.
//
// Real AWS won't let you PutEvents with a source starting with "aws.", which
// is exactly why only Stripe can publish to a partner bus. LocalStack doesn't
// enforce that, which lets the local pipeline match production.
const LocalSource = "aws.partner/stripe.com/ed_local_development"

// API is the part of the EventBridge client used here.
type API interface {
	PutEvents(ctx context.Context, in *eventbridge.PutEventsInput, optFns ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error)
}

// Entry builds the PutEvents entry for a Stripe event: the event type goes in
// detail-type and the whole Stripe event JSON goes in detail.
func Entry(busName, source string, stripeEvent []byte) (types.PutEventsRequestEntry, error) {
	var head struct {
		Type string `json:"type"`
	}
	if err := json.Unmarshal(stripeEvent, &head); err != nil {
		return types.PutEventsRequestEntry{}, fmt.Errorf("decode stripe event: %w", err)
	}
	if head.Type == "" {
		return types.PutEventsRequestEntry{}, errors.New("stripe event has no type")
	}
	return types.PutEventsRequestEntry{
		EventBusName: aws.String(busName),
		Source:       aws.String(source),
		DetailType:   aws.String(head.Type),
		Detail:       aws.String(string(stripeEvent)),
	}, nil
}

// Put sends one Stripe event to the bus and returns the EventBridge event ID.
// PutEvents can return 200 with a failed entry inside, so that's checked too.
func Put(ctx context.Context, client API, busName, source string, stripeEvent []byte) (string, error) {
	entry, err := Entry(busName, source, stripeEvent)
	if err != nil {
		return "", err
	}
	out, err := client.PutEvents(ctx, &eventbridge.PutEventsInput{Entries: []types.PutEventsRequestEntry{entry}})
	if err != nil {
		return "", fmt.Errorf("put events: %w", err)
	}
	if out.FailedEntryCount > 0 || len(out.Entries) != 1 {
		if len(out.Entries) == 1 {
			e := out.Entries[0]
			return "", fmt.Errorf("put events: entry rejected: %s: %s", aws.ToString(e.ErrorCode), aws.ToString(e.ErrorMessage))
		}
		return "", fmt.Errorf("put events: %d failed entries", out.FailedEntryCount)
	}
	return aws.ToString(out.Entries[0].EventId), nil
}

package dynamostore

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
)

type fakeDynamo struct {
	putErr    error
	deleteErr error
	puts      []*dynamodb.PutItemInput
	deletes   []*dynamodb.DeleteItemInput
}

func (f *fakeDynamo) PutItem(_ context.Context, in *dynamodb.PutItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.PutItemOutput, error) {
	f.puts = append(f.puts, in)
	return &dynamodb.PutItemOutput{}, f.putErr
}

func (f *fakeDynamo) DeleteItem(_ context.Context, in *dynamodb.DeleteItemInput, _ ...func(*dynamodb.Options)) (*dynamodb.DeleteItemOutput, error) {
	f.deletes = append(f.deletes, in)
	return &dynamodb.DeleteItemOutput{}, f.deleteErr
}

func newStore(f *fakeDynamo) *Store {
	s := New(f, "dedupe", 96*time.Hour, 45*time.Second)
	s.now = func() time.Time { return time.Unix(1_000_000, 0) }
	return s
}

func TestClaimIsConditional(t *testing.T) {
	f := &fakeDynamo{}
	ok, err := newStore(f).Claim(context.Background(), "evt_1")
	if err != nil || !ok {
		t.Fatalf("Claim = %v, %v", ok, err)
	}
	in := f.puts[0]
	if !strings.Contains(aws.ToString(in.ConditionExpression), "attribute_not_exists(event_id)") {
		t.Fatalf("condition = %q", aws.ToString(in.ConditionExpression))
	}
	if got := in.Item["expires_at"].(*types.AttributeValueMemberN).Value; got != "1345600" {
		t.Fatalf("expires_at = %s, want now + 96h", got)
	}
	if got := in.Item["lease_until"].(*types.AttributeValueMemberN).Value; got != "1000045" {
		t.Fatalf("lease_until = %s", got)
	}
}

func TestClaimConditionFailureMeansDuplicate(t *testing.T) {
	f := &fakeDynamo{putErr: &types.ConditionalCheckFailedException{Message: aws.String("exists")}}
	ok, err := newStore(f).Claim(context.Background(), "evt_1")
	if err != nil || ok {
		t.Fatalf("Claim = %v, %v; want false, nil", ok, err)
	}
}

func TestClaimOtherErrorsPropagate(t *testing.T) {
	f := &fakeDynamo{putErr: errors.New("throttled")}
	if _, err := newStore(f).Claim(context.Background(), "evt_1"); err == nil {
		t.Fatal("want error")
	}
}

func TestReleaseIgnoresCompletedEvents(t *testing.T) {
	f := &fakeDynamo{deleteErr: &types.ConditionalCheckFailedException{Message: aws.String("done")}}
	if err := newStore(f).Release(context.Background(), "evt_1"); err != nil {
		t.Fatalf("Release = %v", err)
	}
}

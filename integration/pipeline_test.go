//go:build integration

// Package integration runs the whole pipeline against LocalStack:
// PutEvents -> EventBridge rule -> SQS -> worker -> DynamoDB dedupe table.
//
//	docker compose up -d --wait
//	go test -tags integration -count=1 ./integration/...
//
// Set AWS_ENDPOINT_URL if LocalStack isn't on http://localhost:4566.
package integration

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	dbtypes "github.com/aws/aws-sdk-go-v2/service/dynamodb/types"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stripe/stripe-go/v87"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/dynamostore"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/publish"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/samples"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/worker"
)

const (
	queueName = "stripe-events"
	tableName = "stripe-events-dedupe"
	busName   = publish.LocalSource
)

type clients struct {
	sqs *sqs.Client
	eb  *eventbridge.Client
	db  *dynamodb.Client
}

func setup(t *testing.T) clients {
	t.Helper()
	// Never let this test wander off to a real AWS account.
	setDefault(t, "AWS_ENDPOINT_URL", "http://localhost:4566")
	setDefault(t, "AWS_REGION", "us-east-1")
	setDefault(t, "AWS_ACCESS_KEY_ID", "test")
	setDefault(t, "AWS_SECRET_ACCESS_KEY", "test")

	cfg, err := config.LoadDefaultConfig(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return clients{sqs: sqs.NewFromConfig(cfg), eb: eventbridge.NewFromConfig(cfg), db: dynamodb.NewFromConfig(cfg)}
}

func setDefault(t *testing.T, key, value string) {
	if os.Getenv(key) == "" {
		t.Setenv(key, value)
	}
}

func queueURL(t *testing.T, c clients, name string) string {
	t.Helper()
	out, err := c.sqs.GetQueueUrl(context.Background(), &sqs.GetQueueUrlInput{QueueName: aws.String(name)})
	if err != nil {
		t.Fatalf("get queue url %s (is LocalStack running? try `make localstack-up`): %v", name, err)
	}
	return aws.ToString(out.QueueUrl)
}

// startWorker runs a worker until the test ends.
func startWorker(t *testing.T, client worker.SQSAPI, url string, process worker.ProcessFunc) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	w := worker.New(client, process, worker.Config{
		QueueURL:       url,
		Concurrency:    2,
		WaitTime:       time.Second,
		HandlerTimeout: 10 * time.Second,
	})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
}

func messagesLeft(t *testing.T, c clients, url string) int {
	t.Helper()
	out, err := c.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl: aws.String(url),
		AttributeNames: []types.QueueAttributeName{
			types.QueueAttributeNameApproximateNumberOfMessages,
			types.QueueAttributeNameApproximateNumberOfMessagesNotVisible,
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	visible, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessages)])
	hidden, _ := strconv.Atoi(out.Attributes[string(types.QueueAttributeNameApproximateNumberOfMessagesNotVisible)])
	return visible + hidden
}

func waitFor(t *testing.T, timeout time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", timeout, what)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

// A sample Stripe event goes in through EventBridge, comes out of SQS, and
// is processed exactly once even when it's delivered twice.
func TestEventBridgeToWorker(t *testing.T) {
	c := setup(t)
	url := queueURL(t, c, queueName)
	store := dynamostore.New(c.db, tableName, time.Hour, time.Minute)
	dispatcher := events.NewDispatcher(store)

	eventID := samples.NewEventID()
	var mu sync.Mutex
	calls := 0
	var got stripe.Event
	dispatcher.On(stripe.EventTypeCheckoutSessionCompleted, func(_ context.Context, e stripe.Event) error {
		if e.ID != eventID {
			return nil // leftovers from manual testing
		}
		mu.Lock()
		defer mu.Unlock()
		calls++
		got = e
		return nil
	})
	startWorker(t, c.sqs, url, func(ctx context.Context, m types.Message) error {
		return dispatcher.Handle(ctx, []byte(aws.ToString(m.Body)))
	})

	event, err := samples.Event("checkout.session.completed", eventID)
	if err != nil {
		t.Fatal(err)
	}
	ctx := context.Background()
	if _, err := publish.Put(ctx, c.eb, busName, publish.LocalSource, event); err != nil {
		t.Fatalf("put event: %v", err)
	}
	waitFor(t, 30*time.Second, "the worker to process the event", func() bool {
		mu.Lock()
		defer mu.Unlock()
		return calls > 0
	})
	if got.Type != stripe.EventTypeCheckoutSessionCompleted || got.Data == nil {
		t.Fatalf("handler got %+v", got)
	}

	// Same Stripe event again, as if EventBridge or SQS delivered it twice.
	if _, err := publish.Put(ctx, c.eb, busName, publish.LocalSource, event); err != nil {
		t.Fatalf("put duplicate: %v", err)
	}
	waitFor(t, 30*time.Second, "the queue to drain", func() bool { return messagesLeft(t, c, url) == 0 })

	mu.Lock()
	defer mu.Unlock()
	if calls != 1 {
		t.Fatalf("handler ran %d times, want 1", calls)
	}

	item, err := c.db.GetItem(ctx, &dynamodb.GetItemInput{
		TableName:      aws.String(tableName),
		Key:            map[string]dbtypes.AttributeValue{"event_id": &dbtypes.AttributeValueMemberS{Value: eventID}},
		ConsistentRead: aws.Bool(true),
	})
	if err != nil {
		t.Fatal(err)
	}
	status, _ := item.Item["status"].(*dbtypes.AttributeValueMemberS)
	if status == nil || status.Value != "done" {
		t.Fatalf("dedupe record = %v, want status done", item.Item)
	}
}

// The DynamoDB store's conditional write lets exactly one claim win.
func TestDynamoStoreClaims(t *testing.T) {
	c := setup(t)
	store := dynamostore.New(c.db, tableName, time.Hour, time.Minute)
	ctx := context.Background()
	id := samples.NewEventID()

	var wg sync.WaitGroup
	var mu sync.Mutex
	wins := 0
	for range 10 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ok, err := store.Claim(ctx, id)
			if err != nil {
				t.Error(err)
				return
			}
			if ok {
				mu.Lock()
				wins++
				mu.Unlock()
			}
		}()
	}
	wg.Wait()
	if wins != 1 {
		t.Fatalf("%d concurrent claims won, want 1", wins)
	}

	if err := store.Release(ctx, id); err != nil {
		t.Fatal(err)
	}
	if ok, err := store.Claim(ctx, id); err != nil || !ok {
		t.Fatalf("claim after release = %v, %v", ok, err)
	}
	if err := store.Complete(ctx, id); err != nil {
		t.Fatal(err)
	}
	if err := store.Release(ctx, id); err != nil {
		t.Fatal(err)
	}
	if ok, _ := store.Claim(ctx, id); ok {
		t.Fatal("completed event was claimed again")
	}
}

func TestQueueHasRedrivePolicy(t *testing.T) {
	c := setup(t)
	out, err := c.sqs.GetQueueAttributes(context.Background(), &sqs.GetQueueAttributesInput{
		QueueUrl:       aws.String(queueURL(t, c, queueName)),
		AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameRedrivePolicy, types.QueueAttributeNameVisibilityTimeout},
	})
	if err != nil {
		t.Fatal(err)
	}
	want := `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:stripe-events-dlq","maxReceiveCount":"5"}`
	if got := out.Attributes["RedrivePolicy"]; got != want && got != `{"deadLetterTargetArn":"arn:aws:sqs:us-east-1:000000000000:stripe-events-dlq","maxReceiveCount":5}` {
		t.Fatalf("RedrivePolicy = %s", got)
	}
	if out.Attributes["VisibilityTimeout"] != "60" {
		t.Fatalf("VisibilityTimeout = %s", out.Attributes["VisibilityTimeout"])
	}
}

// A message that keeps failing ends up in the DLQ. This uses a throwaway
// queue with a 1 second visibility timeout and maxReceiveCount 2 so it
// doesn't take five minutes.
func TestFailingMessageMovesToDLQ(t *testing.T) {
	c := setup(t)
	ctx := context.Background()
	suffix := strconv.FormatInt(time.Now().UnixNano(), 36)

	dlq, err := c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{QueueName: aws.String("it-dlq-" + suffix)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: dlq.QueueUrl}) })
	dlqAttrs, err := c.sqs.GetQueueAttributes(ctx, &sqs.GetQueueAttributesInput{
		QueueUrl: dlq.QueueUrl, AttributeNames: []types.QueueAttributeName{types.QueueAttributeNameQueueArn},
	})
	if err != nil {
		t.Fatal(err)
	}
	mainQ, err := c.sqs.CreateQueue(ctx, &sqs.CreateQueueInput{
		QueueName: aws.String("it-main-" + suffix),
		Attributes: map[string]string{
			"VisibilityTimeout": "1",
			"RedrivePolicy":     fmt.Sprintf(`{"deadLetterTargetArn":"%s","maxReceiveCount":"2"}`, dlqAttrs.Attributes["QueueArn"]),
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _, _ = c.sqs.DeleteQueue(ctx, &sqs.DeleteQueueInput{QueueUrl: mainQ.QueueUrl}) })

	// A body the parser will always reject.
	if _, err := c.sqs.SendMessage(ctx, &sqs.SendMessageInput{
		QueueUrl:    mainQ.QueueUrl,
		MessageBody: aws.String(`{"source":"not.stripe","detail-type":"x","detail":{}}`),
	}); err != nil {
		t.Fatal(err)
	}

	dispatcher := events.NewDispatcher(events.NewMemoryStore(time.Hour, time.Minute))
	var mu sync.Mutex
	attempts := 0
	startWorker(t, c.sqs, aws.ToString(mainQ.QueueUrl), func(ctx context.Context, m types.Message) error {
		mu.Lock()
		attempts++
		mu.Unlock()
		return dispatcher.Handle(ctx, []byte(aws.ToString(m.Body)))
	})

	dlqURL := aws.ToString(dlq.QueueUrl)
	waitFor(t, 30*time.Second, "the message to reach the DLQ", func() bool { return messagesLeft(t, c, dlqURL) == 1 })
	if left := messagesLeft(t, c, aws.ToString(mainQ.QueueUrl)); left != 0 {
		t.Fatalf("main queue still has %d messages", left)
	}
	mu.Lock()
	defer mu.Unlock()
	if attempts != 2 {
		t.Fatalf("attempts = %d, want 2 (maxReceiveCount)", attempts)
	}
}

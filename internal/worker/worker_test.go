package worker

import (
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
	"github.com/stripe/stripe-go/v87"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
)

// fakeSQS hands out queued messages and records deletes. When it's empty,
// ReceiveMessage blocks like a long poll until ctx is canceled or a short
// wait passes.
type fakeSQS struct {
	mu         sync.Mutex
	pending    []types.Message
	deleted    []string // receipt handles
	receives   int
	failNext   int // number of ReceiveMessage calls that should fail
	lastInput  *sqs.ReceiveMessageInput
	deleteCall int
}

func (f *fakeSQS) add(bodies ...string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	for _, b := range bodies {
		n := len(f.pending) + f.receives*100
		f.pending = append(f.pending, types.Message{
			MessageId:     aws.String(fmt.Sprintf("m-%d", n)),
			ReceiptHandle: aws.String("rh-" + b),
			Body:          aws.String(b),
		})
	}
}

func (f *fakeSQS) ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, _ ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error) {
	f.mu.Lock()
	f.receives++
	f.lastInput = in
	if f.failNext > 0 {
		f.failNext--
		f.mu.Unlock()
		return nil, errors.New("throttled")
	}
	if len(f.pending) > 0 {
		n := min(int(in.MaxNumberOfMessages), len(f.pending))
		batch := f.pending[:n]
		f.pending = f.pending[n:]
		f.mu.Unlock()
		return &sqs.ReceiveMessageOutput{Messages: batch}, nil
	}
	f.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-time.After(5 * time.Millisecond):
		return &sqs.ReceiveMessageOutput{}, nil
	}
}

func (f *fakeSQS) DeleteMessageBatch(_ context.Context, in *sqs.DeleteMessageBatchInput, _ ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.deleteCall++
	if len(in.Entries) > 10 {
		return nil, errors.New("too many entries in batch")
	}
	for _, e := range in.Entries {
		f.deleted = append(f.deleted, aws.ToString(e.ReceiptHandle))
	}
	return &sqs.DeleteMessageBatchOutput{}, nil
}

func (f *fakeSQS) deletedHandles() []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := append([]string(nil), f.deleted...)
	sort.Strings(out)
	return out
}

func (f *fakeSQS) pendingCount() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.pending)
}

// runUntil runs the worker until cond is true, then shuts it down.
func runUntil(t *testing.T, w *Worker, cond func() bool) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			cancel()
			<-done
			t.Fatal("timed out waiting for condition")
		}
		time.Sleep(2 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("worker did not stop after cancel")
	}
}

func newWorker(f *fakeSQS, fn ProcessFunc, cfg Config) *Worker {
	cfg.QueueURL = "http://localhost/000000000000/stripe-events"
	w := New(f, fn, cfg)
	w.sleep = func(context.Context, time.Duration) {}
	return w
}

func TestReceiveUsesLongPollingAndBatches(t *testing.T) {
	f := &fakeSQS{}
	w := newWorker(f, func(context.Context, types.Message) error { return nil }, Config{})
	runUntil(t, w, func() bool {
		f.mu.Lock()
		defer f.mu.Unlock()
		return f.receives > 0
	})
	if f.lastInput.WaitTimeSeconds != 20 || f.lastInput.MaxNumberOfMessages != 10 {
		t.Fatalf("receive input = wait %d, max %d; want 20, 10", f.lastInput.WaitTimeSeconds, f.lastInput.MaxNumberOfMessages)
	}
}

func TestSuccessDeletesAndFailureLeavesMessage(t *testing.T) {
	f := &fakeSQS{}
	f.add("ok-1", "bad-1", "ok-2", "bad-2", "ok-3")
	var mu sync.Mutex
	seen := map[string]bool{}
	w := newWorker(f, func(_ context.Context, m types.Message) error {
		mu.Lock()
		seen[aws.ToString(m.Body)] = true
		mu.Unlock()
		if aws.ToString(m.Body)[:3] == "bad" {
			return errors.New("handler failed")
		}
		return nil
	}, Config{Concurrency: 2})

	runUntil(t, w, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return len(seen) == 5 && len(f.deletedHandles()) == 3
	})

	got := f.deletedHandles()
	want := []string{"rh-ok-1", "rh-ok-2", "rh-ok-3"}
	if fmt.Sprint(got) != fmt.Sprint(want) {
		t.Fatalf("deleted = %v, want %v (failed messages must stay on the queue)", got, want)
	}
}

func TestPanicLeavesMessage(t *testing.T) {
	f := &fakeSQS{}
	f.add("boom", "ok")
	var processed sync.WaitGroup
	processed.Add(2)
	w := newWorker(f, func(_ context.Context, m types.Message) error {
		defer processed.Done()
		if aws.ToString(m.Body) == "boom" {
			panic("nil map")
		}
		return nil
	}, Config{})

	runUntil(t, w, func() bool { return len(f.deletedHandles()) == 1 })
	processed.Wait()
	if got := f.deletedHandles(); len(got) != 1 || got[0] != "rh-ok" {
		t.Fatalf("deleted = %v, want [rh-ok]", got)
	}
}

func TestReceiveErrorsAreRetried(t *testing.T) {
	f := &fakeSQS{failNext: 3}
	f.add("ok")
	w := newWorker(f, func(context.Context, types.Message) error { return nil }, Config{})
	runUntil(t, w, func() bool { return len(f.deletedHandles()) == 1 })
}

func TestDeletesStayWithinBatchLimit(t *testing.T) {
	f := &fakeSQS{}
	for i := range 35 {
		f.add(fmt.Sprintf("ok-%02d", i))
	}
	w := newWorker(f, func(context.Context, types.Message) error { return nil }, Config{Concurrency: 3})
	runUntil(t, w, func() bool { return len(f.deletedHandles()) == 35 })
}

func TestGracefulShutdownFinishesInFlight(t *testing.T) {
	f := &fakeSQS{}
	f.add("slow")
	started := make(chan struct{})
	release := make(chan struct{})
	var handlerCtxErr error
	w := newWorker(f, func(ctx context.Context, _ types.Message) error {
		close(started)
		<-release
		handlerCtxErr = ctx.Err()
		return nil
	}, Config{})

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		_ = w.Run(ctx)
		close(done)
	}()

	<-started
	cancel() // SIGTERM arrives while the message is being processed.

	select {
	case <-done:
		t.Fatal("Run returned before the in-flight message finished")
	case <-time.After(50 * time.Millisecond):
	}

	close(release)
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after the in-flight message finished")
	}

	if handlerCtxErr != nil {
		t.Fatalf("handler context was canceled by shutdown: %v", handlerCtxErr)
	}
	if got := f.deletedHandles(); len(got) != 1 {
		t.Fatalf("deleted = %v, want the in-flight message deleted", got)
	}
}

func TestShutdownStopsReceiving(t *testing.T) {
	f := &fakeSQS{}
	w := newWorker(f, func(context.Context, types.Message) error { return nil }, Config{Concurrency: 4})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := w.Run(ctx); err != nil {
		t.Fatal(err)
	}
	f.add("later")
	if f.pendingCount() != 1 {
		t.Fatal("message was received after shutdown")
	}
}

func TestHandlerTimeout(t *testing.T) {
	f := &fakeSQS{}
	f.add("stuck")
	var mu sync.Mutex
	var gotErr error
	w := newWorker(f, func(ctx context.Context, _ types.Message) error {
		<-ctx.Done()
		mu.Lock()
		gotErr = ctx.Err()
		mu.Unlock()
		return ctx.Err()
	}, Config{HandlerTimeout: 20 * time.Millisecond})

	runUntil(t, w, func() bool {
		mu.Lock()
		defer mu.Unlock()
		return gotErr != nil
	})
	if !errors.Is(gotErr, context.DeadlineExceeded) {
		t.Fatalf("handler ctx err = %v", gotErr)
	}
	if len(f.deletedHandles()) != 0 {
		t.Fatal("timed-out message was deleted")
	}
}

// End to end with the real dispatcher: the same Stripe event arriving in two
// SQS messages (a duplicate delivery) runs the handler once, and both
// messages are deleted.
func TestDuplicateMessageProcessedOnce(t *testing.T) {
	body := `{"version":"0","id":"eb-1","detail-type":"invoice.payment_failed","source":"aws.partner/stripe.com/ed_test",` +
		`"detail":{"id":"evt_dup","object":"event","type":"invoice.payment_failed","api_version":"` + stripe.APIVersion +
		`","data":{"object":{"id":"in_1","object":"invoice","customer":"cus_1"}}}}`
	f := &fakeSQS{}
	f.add(body)
	f.mu.Lock()
	dup := f.pending[0]
	dup.ReceiptHandle = aws.String("rh-duplicate")
	dup.MessageId = aws.String("m-duplicate")
	f.pending = append(f.pending, dup)
	f.mu.Unlock()

	d := events.NewDispatcher(events.NewMemoryStore(time.Hour, time.Minute))
	var mu sync.Mutex
	calls := 0
	d.On(stripe.EventTypeInvoicePaymentFailed, func(context.Context, stripe.Event) error {
		mu.Lock()
		calls++
		mu.Unlock()
		return nil
	})
	w := newWorker(f, func(ctx context.Context, m types.Message) error {
		return d.Handle(ctx, []byte(aws.ToString(m.Body)))
	}, Config{Concurrency: 2, BatchSize: 1})

	runUntil(t, w, func() bool { return len(f.deletedHandles()) == 2 })
	if calls != 1 {
		t.Fatalf("handler calls = %d, want 1", calls)
	}
}

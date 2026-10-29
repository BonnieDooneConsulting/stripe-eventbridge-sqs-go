package events

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v87"
)

const checkoutType = stripe.EventTypeCheckoutSessionCompleted

func countingHandler(calls *int, mu *sync.Mutex, err error) HandlerFunc {
	return func(_ context.Context, _ stripe.Event) error {
		mu.Lock()
		defer mu.Unlock()
		*calls++
		return err
	}
}

func TestHandleDispatchesByType(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute))
	var got stripe.Event
	d.On(checkoutType, func(_ context.Context, e stripe.Event) error {
		got = e
		return nil
	})

	if err := d.Handle(context.Background(), fixture(t, "eventbridge_checkout_completed.json")); err != nil {
		t.Fatalf("Handle: %v", err)
	}
	if got.ID != "evt_1SampleCheckout000001" {
		t.Fatalf("handler got %q", got.ID)
	}
}

func TestHandleAcknowledgesUnknownTypes(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute), WithIgnoreAPIVersionMismatch())
	var calls int
	var mu sync.Mutex
	d.On(checkoutType, countingHandler(&calls, &mu, nil))

	// customer.created has no handler registered.
	if err := d.Handle(context.Background(), fixture(t, "eventbridge_customer_created.json")); err != nil {
		t.Fatalf("Handle: %v, want nil so the message is deleted", err)
	}
	if calls != 0 {
		t.Fatalf("calls = %d, want 0", calls)
	}
}

func TestHandleSkipsDuplicates(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute))
	var calls int
	var mu sync.Mutex
	d.On(checkoutType, countingHandler(&calls, &mu, nil))
	body := fixture(t, "eventbridge_checkout_completed.json")

	for i := range 3 {
		if err := d.Handle(context.Background(), body); err != nil {
			t.Fatalf("delivery %d: %v", i, err)
		}
	}
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestHandleConcurrentDuplicatesRunOnce(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute))
	var calls int
	var mu sync.Mutex
	d.On(checkoutType, countingHandler(&calls, &mu, nil))
	body := fixture(t, "eventbridge_checkout_completed.json")

	var wg sync.WaitGroup
	for range 20 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			_ = d.Handle(context.Background(), body)
		}()
	}
	wg.Wait()
	if calls != 1 {
		t.Fatalf("calls = %d, want 1", calls)
	}
}

func TestHandleFailureReleasesClaimForRetry(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute))
	fail := true
	var calls int
	d.On(checkoutType, func(context.Context, stripe.Event) error {
		calls++
		if fail {
			return errors.New("database is down")
		}
		return nil
	})
	body := fixture(t, "eventbridge_checkout_completed.json")

	if err := d.Handle(context.Background(), body); err == nil {
		t.Fatal("Handle returned nil after the handler failed")
	}
	fail = false
	if err := d.Handle(context.Background(), body); err != nil {
		t.Fatalf("retry: %v", err)
	}
	if calls != 2 {
		t.Fatalf("calls = %d, want 2 (the retry must not be treated as a duplicate)", calls)
	}
}

type brokenStore struct{}

func (brokenStore) Claim(context.Context, string) (bool, error) {
	return false, errors.New("dynamodb unavailable")
}
func (brokenStore) Complete(context.Context, string) error { return nil }
func (brokenStore) Release(context.Context, string) error  { return nil }

func TestHandleStoreErrorRetries(t *testing.T) {
	d := NewDispatcher(brokenStore{})
	var calls int
	var mu sync.Mutex
	d.On(checkoutType, countingHandler(&calls, &mu, nil))

	if err := d.Handle(context.Background(), fixture(t, "eventbridge_checkout_completed.json")); err == nil {
		t.Fatal("Handle returned nil when the store failed")
	}
	if calls != 0 {
		t.Fatalf("handler ran without a claim")
	}
}

func TestHandleParseErrorRetries(t *testing.T) {
	d := NewDispatcher(NewMemoryStore(time.Hour, time.Minute))
	if err := d.Handle(context.Background(), []byte(`{"source":"somewhere.else","detail":{}}`)); err == nil {
		t.Fatal("Handle returned nil for a non-Stripe message, want an error so it ends up in the DLQ")
	}
}

func TestMemoryStoreStaleClaim(t *testing.T) {
	now := time.Unix(1000, 0)
	s := NewMemoryStore(time.Hour, time.Minute)
	s.now = func() time.Time { return now }
	ctx := context.Background()

	if ok, _ := s.Claim(ctx, "evt_1"); !ok {
		t.Fatal("first claim failed")
	}
	if ok, _ := s.Claim(ctx, "evt_1"); ok {
		t.Fatal("second claim succeeded while the first is in progress")
	}
	// The first worker crashed without releasing. After the claim timeout,
	// a redelivery can take over.
	now = now.Add(2 * time.Minute)
	if ok, _ := s.Claim(ctx, "evt_1"); !ok {
		t.Fatal("stale claim was not taken over")
	}
	_ = s.Complete(ctx, "evt_1")
	now = now.Add(30 * time.Minute)
	if ok, _ := s.Claim(ctx, "evt_1"); ok {
		t.Fatal("completed event was claimed again")
	}
	_ = s.Release(ctx, "evt_1")
	if ok, _ := s.Claim(ctx, "evt_1"); ok {
		t.Fatal("Release removed a completed event")
	}
	// After the TTL the record is forgotten.
	now = now.Add(2 * time.Hour)
	if ok, _ := s.Claim(ctx, "evt_1"); !ok {
		t.Fatal("expired record still blocks claims")
	}
}

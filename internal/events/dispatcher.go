package events

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/stripe/stripe-go/v87"
)

// HandlerFunc processes one Stripe event. Return an error to leave the
// message on the queue so it's retried later.
type HandlerFunc func(ctx context.Context, event stripe.Event) error

// Dispatcher parses SQS message bodies and routes each Stripe event to the
// handler registered for its type.
type Dispatcher struct {
	store    EventStore
	handlers map[stripe.EventType]HandlerFunc
	logger   *slog.Logger
	parse    ParseOptions
}

// Option configures a Dispatcher.
type Option func(*Dispatcher)

// WithLogger sets the logger. The default discards logs.
func WithLogger(l *slog.Logger) Option {
	return func(d *Dispatcher) { d.logger = l }
}

// WithIgnoreAPIVersionMismatch accepts events whose API version differs from
// the one this stripe-go release was built for. Only turn this on if you've
// checked that the fields you read haven't changed between versions.
func WithIgnoreAPIVersionMismatch() Option {
	return func(d *Dispatcher) { d.parse.IgnoreAPIVersionMismatch = true }
}

// NewDispatcher returns a Dispatcher that records processed events in store.
func NewDispatcher(store EventStore, opts ...Option) *Dispatcher {
	d := &Dispatcher{
		store:    store,
		handlers: make(map[stripe.EventType]HandlerFunc),
		logger:   slog.New(slog.DiscardHandler),
	}
	for _, opt := range opts {
		opt(d)
	}
	return d
}

// On registers fn for events of type t. Events with no registered handler
// are acknowledged and dropped, so they don't pile up in the queue.
func (d *Dispatcher) On(t stripe.EventType, fn HandlerFunc) {
	d.handlers[t] = fn
}

// Handle processes one SQS message body. A nil error means the message is
// finished with (handled, a duplicate, or a type nobody cares about) and can
// be deleted. Any error means "leave it on the queue": SQS makes it visible
// again after the visibility timeout, and after enough failed receives the
// redrive policy moves it to the dead-letter queue.
func (d *Dispatcher) Handle(ctx context.Context, body []byte) error {
	event, env, err := Parse(body, d.parse)
	if err != nil {
		// A message we can't parse won't parse next time either. Returning an
		// error parks it in the DLQ after maxReceiveCount tries, which keeps
		// it around for a person to inspect instead of silently deleting it.
		d.logger.Error("could not parse message", "eventbridge_id", env.ID, "error", err)
		return fmt.Errorf("parse: %w", err)
	}

	log := d.logger.With("event_id", event.ID, "event_type", event.Type)
	if env.DetailType != "" && env.DetailType != string(event.Type) {
		log.Warn("detail-type doesn't match event type, using the event type", "detail_type", env.DetailType)
	}

	fn, ok := d.handlers[event.Type]
	if !ok {
		log.Debug("ignoring unhandled event type")
		return nil
	}

	claimed, err := d.store.Claim(ctx, event.ID)
	if err != nil {
		log.Error("could not claim event", "error", err)
		return fmt.Errorf("claim %s: %w", event.ID, err)
	}
	if !claimed {
		// Already handled, or another worker has it right now. Either way
		// this copy can go.
		log.Info("skipping duplicate event")
		return nil
	}

	if err := fn(ctx, event); err != nil {
		log.Error("event handler failed, message will be retried", "error", err)
		// Use a fresh context: if ctx timed out, we still want the claim gone
		// so the retry isn't mistaken for a duplicate.
		if relErr := d.store.Release(context.WithoutCancel(ctx), event.ID); relErr != nil {
			log.Error("could not release event", "error", relErr)
		}
		return fmt.Errorf("handle %s: %w", event.ID, err)
	}

	if err := d.store.Complete(context.WithoutCancel(ctx), event.ID); err != nil {
		// The work is done. Failing here would only cause a duplicate run.
		log.Error("could not mark event complete", "error", err)
	}
	log.Info("processed event")
	return nil
}

// Package events turns SQS message bodies into Stripe events and sends each
// one to the handler registered for its type, skipping events it has already
// processed.
//
// Stripe's Amazon EventBridge destination wraps the normal Stripe Event JSON
// in an EventBridge envelope. The Stripe event sits under the top-level
// "detail" key, and "detail-type" repeats the event type:
//
//	{
//	  "version": "0",
//	  "id": "17e8dff5-...",                      // EventBridge's ID, not Stripe's
//	  "detail-type": "customer.created",
//	  "source": "aws.partner/stripe.com/ed_...",
//	  "account": "506417113029",
//	  "time": "2024-03-07T18:27:56Z",
//	  "region": "us-west-2",
//	  "resources": ["arn:aws:events:...:event-source/aws.partner/stripe.com/ed_..."],
//	  "detail": { "id": "evt_...", "object": "event", "type": "customer.created", ... }
//	}
//
// See https://docs.stripe.com/event-destinations/eventbridge#event-structure.
package events

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"strings"

	"github.com/stripe/stripe-go/v87"
)

// StripeSourcePrefix is the start of the EventBridge "source" field on every
// event Stripe sends. The rest is the event destination ID (ed_...).
const StripeSourcePrefix = "aws.partner/stripe.com"

// Envelope is the EventBridge wrapper around a Stripe event.
type Envelope struct {
	Version    string          `json:"version"`
	ID         string          `json:"id"`
	DetailType string          `json:"detail-type"`
	Source     string          `json:"source"`
	Account    string          `json:"account"`
	Time       string          `json:"time"`
	Region     string          `json:"region"`
	Resources  []string        `json:"resources"`
	Detail     json.RawMessage `json:"detail"`
}

// Errors returned by Parse. They all mean the message will never succeed no
// matter how many times it's retried, so the worker leaves it for the
// dead-letter queue where a person can look at it.
var (
	ErrNotStripe          = errors.New("message is not a Stripe event")
	ErrThinEvent          = errors.New("thin events are not supported, configure the destination for snapshot events")
	ErrMissingFields      = errors.New("stripe event is missing id or type")
	ErrAPIVersionMismatch = errors.New("stripe event API version doesn't match this stripe-go release")
)

// ParseOptions controls how strict Parse is.
type ParseOptions struct {
	// IgnoreAPIVersionMismatch accepts events whose API version is from a
	// different release than the one stripe-go was built for. Only turn this
	// on if you've checked that the fields you read haven't changed.
	IgnoreAPIVersionMismatch bool
}

// Parse reads an SQS message body and returns the Stripe event inside it.
//
// It's a little more forgiving than strictly necessary. Besides the
// documented EventBridge envelope, it also accepts a bare Stripe event (what
// you get if someone adds an input transformer of "$.detail" to the rule),
// and it trusts detail.type over detail-type if the two ever disagree.
func Parse(body []byte, opts ParseOptions) (stripe.Event, Envelope, error) {
	var env Envelope
	if err := json.Unmarshal(body, &env); err != nil {
		return stripe.Event{}, env, fmt.Errorf("decode envelope: %w", err)
	}

	detail := env.Detail
	switch {
	case len(env.Detail) > 0:
		if !strings.HasPrefix(env.Source, StripeSourcePrefix) {
			return stripe.Event{}, env, fmt.Errorf("%w: source %q", ErrNotStripe, env.Source)
		}
	case looksLikeStripeEvent(body):
		// No envelope at all. Treat the whole body as the event.
		detail = body
	default:
		return stripe.Event{}, env, fmt.Errorf("%w: no detail field", ErrNotStripe)
	}

	if trimmed := bytes.TrimSpace(detail); len(trimmed) == 0 || trimmed[0] != '{' {
		return stripe.Event{}, env, fmt.Errorf("%w: detail is not a JSON object", ErrNotStripe)
	}

	var head struct {
		Object string `json:"object"`
	}
	if err := json.Unmarshal(detail, &head); err != nil {
		return stripe.Event{}, env, fmt.Errorf("decode detail: %w", err)
	}
	if head.Object == "v2.core.event" {
		return stripe.Event{}, env, ErrThinEvent
	}

	var event stripe.Event
	if err := json.Unmarshal(detail, &event); err != nil {
		return stripe.Event{}, env, fmt.Errorf("decode stripe event: %w", err)
	}
	if event.ID == "" || event.Type == "" {
		return stripe.Event{}, env, ErrMissingFields
	}
	if event.Data == nil || len(event.Data.Raw) == 0 {
		return stripe.Event{}, env, fmt.Errorf("%w: data.object", ErrMissingFields)
	}
	if !opts.IgnoreAPIVersionMismatch && !compatibleAPIVersion(stripe.APIVersion, event.APIVersion) {
		return stripe.Event{}, env, fmt.Errorf("%w: got %s, want the %s release", ErrAPIVersionMismatch, event.APIVersion, stripe.APIVersion)
	}
	return event, env, nil
}

func looksLikeStripeEvent(body []byte) bool {
	var head struct {
		Object string `json:"object"`
		ID     string `json:"id"`
	}
	if err := json.Unmarshal(body, &head); err != nil {
		return false
	}
	return head.Object == "event" && strings.HasPrefix(head.ID, "evt_")
}

// compatibleAPIVersion mirrors the check stripe-go's webhook package does:
// versions look like 2026-09-30.endive, and events from the same named
// release (the part after the dot) are safe to decode.
func compatibleAPIVersion(sdk, event string) bool {
	_, sdkTrain, ok := strings.Cut(sdk, ".")
	if !ok {
		return sdk == event
	}
	_, eventTrain, ok := strings.Cut(event, ".")
	if !ok {
		// Versions from before named releases can't match a modern SDK.
		return false
	}
	if sdkTrain == "preview" {
		return sdk == event
	}
	return sdkTrain == eventTrain
}

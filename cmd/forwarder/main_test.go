package main

import (
	"bytes"
	"context"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge/types"
	"github.com/stripe/stripe-go/v87"
	"github.com/stripe/stripe-go/v87/webhook"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/publish"
)

const secret = "whsec_test_secret"

type fakeEB struct{ entries []types.PutEventsRequestEntry }

func (f *fakeEB) PutEvents(_ context.Context, in *eventbridge.PutEventsInput, _ ...func(*eventbridge.Options)) (*eventbridge.PutEventsOutput, error) {
	f.entries = append(f.entries, in.Entries...)
	return &eventbridge.PutEventsOutput{Entries: []types.PutEventsResultEntry{{EventId: aws.String("eb-1")}}}, nil
}

func newForwarder(eb *fakeEB) *forwarder {
	return &forwarder{secret: secret, client: eb, bus: "bus", source: publish.LocalSource, logger: slog.New(slog.DiscardHandler)}
}

func TestForwardsVerifiedWebhook(t *testing.T) {
	eb := &fakeEB{}
	payload := []byte(`{"id":"evt_1","object":"event","type":"invoice.paid","api_version":"` + stripe.APIVersion + `","data":{"object":{}}}`)
	signed := webhook.GenerateTestSignedPayload(&webhook.UnsignedPayload{Payload: payload, Secret: secret})

	req := httptest.NewRequest(http.MethodPost, "/stripe", bytes.NewReader(payload))
	req.Header.Set("Stripe-Signature", signed.Header)
	rec := httptest.NewRecorder()
	newForwarder(eb).ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d", rec.Code)
	}
	if len(eb.entries) != 1 {
		t.Fatalf("entries = %d", len(eb.entries))
	}
	e := eb.entries[0]
	if aws.ToString(e.DetailType) != "invoice.paid" || aws.ToString(e.Detail) != string(payload) || aws.ToString(e.Source) != publish.LocalSource {
		t.Fatalf("entry = %+v", e)
	}
}

func TestRejectsBadSignature(t *testing.T) {
	eb := &fakeEB{}
	req := httptest.NewRequest(http.MethodPost, "/stripe", bytes.NewReader([]byte(`{"id":"evt_1","type":"invoice.paid"}`)))
	req.Header.Set("Stripe-Signature", "t=1,v1=nope")
	rec := httptest.NewRecorder()
	newForwarder(eb).ServeHTTP(rec, req)

	if rec.Code != http.StatusBadRequest || len(eb.entries) != 0 {
		t.Fatalf("status = %d, entries = %d; want 400 and nothing forwarded", rec.Code, len(eb.entries))
	}
}

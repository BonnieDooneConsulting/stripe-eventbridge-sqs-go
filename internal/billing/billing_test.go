package billing

import (
	"context"
	"testing"
	"time"

	"github.com/stripe/stripe-go/v87"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
)

type recorder struct {
	calls []string
}

func (r *recorder) Activate(_ context.Context, customerID, subscriptionID, reference string) error {
	r.calls = append(r.calls, "activate "+customerID+" "+subscriptionID+" "+reference)
	return nil
}

func (r *recorder) MarkPaymentFailed(_ context.Context, customerID, invoiceID string, _ int64) error {
	r.calls = append(r.calls, "failed "+customerID+" "+invoiceID)
	return nil
}

func (r *recorder) Deactivate(_ context.Context, customerID, subscriptionID string) error {
	r.calls = append(r.calls, "deactivate "+customerID+" "+subscriptionID)
	return nil
}

// deliver wraps a Stripe event in an EventBridge envelope, like Stripe does,
// and runs it through the dispatcher.
func deliver(t *testing.T, d *events.Dispatcher, eventType, object string) {
	t.Helper()
	body := []byte(`{"version":"0","id":"eb-` + eventType + `","detail-type":"` + eventType +
		`","source":"aws.partner/stripe.com/ed_test","detail":{"id":"evt_` + eventType +
		`","object":"event","type":"` + eventType + `","api_version":"` + stripe.APIVersion +
		`","data":{"object":` + object + `}}}`)
	if err := d.Handle(context.Background(), body); err != nil {
		t.Fatalf("%s: %v", eventType, err)
	}
}

func setup() (*events.Dispatcher, *recorder) {
	d := events.NewDispatcher(events.NewMemoryStore(time.Hour, time.Minute))
	r := &recorder{}
	Register(d, r)
	return d, r
}

func TestCheckoutCompletedActivatesAccount(t *testing.T) {
	d, r := setup()
	deliver(t, d, "checkout.session.completed",
		`{"id":"cs_1","object":"checkout.session","customer":"cus_1","subscription":"sub_1","client_reference_id":"user_42","payment_status":"paid"}`)

	want := "activate cus_1 sub_1 user_42"
	if len(r.calls) != 1 || r.calls[0] != want {
		t.Fatalf("calls = %v, want [%s]", r.calls, want)
	}
}

func TestUnpaidCheckoutWaits(t *testing.T) {
	d, r := setup()
	deliver(t, d, "checkout.session.completed",
		`{"id":"cs_2","object":"checkout.session","customer":"cus_1","payment_status":"unpaid"}`)

	if len(r.calls) != 0 {
		t.Fatalf("calls = %v, want none until the payment clears", r.calls)
	}
}

func TestInvoicePaymentFailedIsRecorded(t *testing.T) {
	d, r := setup()
	deliver(t, d, "invoice.payment_failed",
		`{"id":"in_1","object":"invoice","customer":"cus_1","attempt_count":2}`)

	if len(r.calls) != 1 || r.calls[0] != "failed cus_1 in_1" {
		t.Fatalf("calls = %v", r.calls)
	}
}

func TestSubscriptionDeletedDeactivates(t *testing.T) {
	d, r := setup()
	deliver(t, d, "customer.subscription.deleted",
		`{"id":"sub_1","object":"subscription","customer":"cus_1","status":"canceled"}`)

	if len(r.calls) != 1 || r.calls[0] != "deactivate cus_1 sub_1" {
		t.Fatalf("calls = %v", r.calls)
	}
}

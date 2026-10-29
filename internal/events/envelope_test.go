package events

import (
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func fixture(t *testing.T, name string) []byte {
	t.Helper()
	b, err := os.ReadFile(filepath.Join("testdata", name))
	if err != nil {
		t.Fatal(err)
	}
	return b
}

// The customer.created fixture is copied from Stripe's EventBridge docs, so
// this test pins the parser to the documented shape.
func TestParseDocumentedEnvelope(t *testing.T) {
	body := fixture(t, "eventbridge_customer_created.json")

	event, env, err := Parse(body, ParseOptions{IgnoreAPIVersionMismatch: true})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if event.ID != "evt_1OrlfcFvFEcV7KhhYdemHC4q" {
		t.Errorf("event.ID = %q", event.ID)
	}
	if event.Type != "customer.created" {
		t.Errorf("event.Type = %q", event.Type)
	}
	if event.APIVersion != "2023-10-16" || !event.Livemode {
		t.Errorf("api_version/livemode = %q/%v", event.APIVersion, event.Livemode)
	}
	if env.ID != "17e8dff5-d6cd-3770-ace9-aeac02b6ac3f" || env.DetailType != "customer.created" {
		t.Errorf("envelope = %+v", env)
	}
	if env.Source != "aws.partner/stripe.com/ed_61PgtRTG5aTCIz98516PLsRGLISQK0Otk6FWKjBrcDia" {
		t.Errorf("source = %q", env.Source)
	}
	if len(env.Resources) != 1 {
		t.Errorf("resources = %v", env.Resources)
	}

	var customer struct {
		ID   string `json:"id"`
		Name string `json:"name"`
	}
	if err := json.Unmarshal(event.Data.Raw, &customer); err != nil {
		t.Fatal(err)
	}
	if customer.ID != "cus_Ph9zopzZYifGvU" || customer.Name != "coolest customer" {
		t.Errorf("data.object = %+v", customer)
	}
}

func TestParseRejectsOtherAPIReleasesByDefault(t *testing.T) {
	_, _, err := Parse(fixture(t, "eventbridge_customer_created.json"), ParseOptions{})
	if !errors.Is(err, ErrAPIVersionMismatch) {
		t.Fatalf("err = %v, want ErrAPIVersionMismatch", err)
	}
}

func TestParseCurrentAPIVersion(t *testing.T) {
	event, _, err := Parse(fixture(t, "eventbridge_checkout_completed.json"), ParseOptions{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if event.ID != "evt_1SampleCheckout000001" || event.Type != "checkout.session.completed" {
		t.Errorf("event = %s %s", event.ID, event.Type)
	}
}

func TestParseBareStripeEvent(t *testing.T) {
	// What the queue gets if the rule has an input transformer of $.detail.
	var env Envelope
	if err := json.Unmarshal(fixture(t, "eventbridge_checkout_completed.json"), &env); err != nil {
		t.Fatal(err)
	}
	event, _, err := Parse(env.Detail, ParseOptions{})
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if event.ID != "evt_1SampleCheckout000001" {
		t.Errorf("event.ID = %q", event.ID)
	}
}

func TestParseThinEvent(t *testing.T) {
	_, _, err := Parse(fixture(t, "eventbridge_thin_event.json"), ParseOptions{})
	if !errors.Is(err, ErrThinEvent) {
		t.Fatalf("err = %v, want ErrThinEvent", err)
	}
}

func TestParseRejectsBadMessages(t *testing.T) {
	tests := []struct {
		name string
		body string
		want error
	}{
		{"not json", `hello`, nil},
		{"other source", `{"source":"my.app","detail-type":"x","detail":{"id":"evt_1","object":"event","type":"x","data":{"object":{}}}}`, ErrNotStripe},
		{"no detail", `{"source":"aws.partner/stripe.com/ed_1","detail-type":"x"}`, ErrNotStripe},
		{"detail is a string", `{"source":"aws.partner/stripe.com/ed_1","detail":"{}"}`, ErrNotStripe},
		{"missing id", `{"source":"aws.partner/stripe.com/ed_1","detail":{"object":"event","type":"x","data":{"object":{}}}}`, ErrMissingFields},
		{"missing data", `{"source":"aws.partner/stripe.com/ed_1","detail":{"id":"evt_1","object":"event","type":"x"}}`, ErrMissingFields},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			_, _, err := Parse([]byte(tt.body), ParseOptions{IgnoreAPIVersionMismatch: true})
			if err == nil {
				t.Fatal("Parse succeeded, want an error")
			}
			if tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("err = %v, want %v", err, tt.want)
			}
		})
	}
}

func TestCompatibleAPIVersion(t *testing.T) {
	tests := []struct {
		sdk, event string
		want       bool
	}{
		{"2026-09-30.endive", "2026-09-30.endive", true},
		{"2026-10-28.endive", "2026-09-30.endive", true},
		{"2026-09-30.endive", "2026-08-26.dahlia", false},
		{"2026-09-30.endive", "2023-10-16", false},
		{"2026-09-30.preview", "2026-09-30.preview", true},
		{"2026-09-30.preview", "2026-08-30.preview", false},
	}
	for _, tt := range tests {
		if got := compatibleAPIVersion(tt.sdk, tt.event); got != tt.want {
			t.Errorf("compatibleAPIVersion(%q, %q) = %v, want %v", tt.sdk, tt.event, got, tt.want)
		}
	}
}

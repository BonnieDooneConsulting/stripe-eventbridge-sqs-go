// Package samples holds realistic Stripe snapshot events for local testing.
package samples

import (
	"crypto/rand"
	"embed"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/stripe/stripe-go/v87"
)

//go:embed data/*.json
var files embed.FS

// Types lists the event types that have a sample.
func Types() []string {
	entries, _ := files.ReadDir("data")
	out := make([]string, 0, len(entries))
	for _, e := range entries {
		out = append(out, strings.TrimSuffix(e.Name(), ".json"))
	}
	sort.Strings(out)
	return out
}

// Event returns a sample Stripe event of the given type. If id is empty a
// random evt_ ID is generated, so each call is a new event. The API version
// is set to the one this stripe-go release expects.
func Event(eventType, id string) ([]byte, error) {
	raw, err := files.ReadFile("data/" + eventType + ".json")
	if err != nil {
		return nil, fmt.Errorf("no sample for %q (have %s)", eventType, strings.Join(Types(), ", "))
	}
	var event map[string]any
	if err := json.Unmarshal(raw, &event); err != nil {
		return nil, fmt.Errorf("decode sample %s: %w", eventType, err)
	}
	if id == "" {
		id = NewEventID()
	}
	event["id"] = id
	event["api_version"] = stripe.APIVersion
	event["created"] = time.Now().Unix()
	return json.Marshal(event)
}

// NewEventID returns a random ID that looks like a Stripe event ID.
func NewEventID() string {
	b := make([]byte, 12)
	_, _ = rand.Read(b)
	return "evt_local_" + hex.EncodeToString(b)
}

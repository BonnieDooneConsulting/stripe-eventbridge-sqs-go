package samples

import (
	"testing"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/publish"
)

// Every sample, wrapped the way the forwarder and sample command wrap it,
// must parse cleanly with the default (strict) options.
func TestSamplesParse(t *testing.T) {
	types := Types()
	if len(types) != 3 {
		t.Fatalf("Types() = %v", types)
	}
	for _, typ := range types {
		event, err := Event(typ, "")
		if err != nil {
			t.Fatal(err)
		}
		entry, err := publish.Entry("bus", publish.LocalSource, event)
		if err != nil {
			t.Fatal(err)
		}
		body := `{"version":"0","id":"eb","detail-type":"` + *entry.DetailType + `","source":"` + *entry.Source + `","detail":` + *entry.Detail + `}`
		parsed, _, err := events.Parse([]byte(body), events.ParseOptions{})
		if err != nil {
			t.Fatalf("%s: %v", typ, err)
		}
		if string(parsed.Type) != typ {
			t.Fatalf("type = %s, want %s", parsed.Type, typ)
		}
	}
}

func TestUnknownSample(t *testing.T) {
	if _, err := Event("nope", ""); err == nil {
		t.Fatal("want error")
	}
}

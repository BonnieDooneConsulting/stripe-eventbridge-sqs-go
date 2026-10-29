// Command sample puts a realistic Stripe event onto the local EventBridge bus
// so you can try the pipeline without the Stripe CLI.
//
//	go run ./cmd/sample -type invoice.payment_failed
//
// Run it twice with the same -id to watch the worker skip the duplicate.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/publish"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/samples"
)

func main() {
	eventType := flag.String("type", "checkout.session.completed", "event type, one of: "+strings.Join(samples.Types(), ", "))
	id := flag.String("id", "", "Stripe event ID to use (default: a new random one)")
	bus := flag.String("bus", envOr("EVENT_BUS_NAME", publish.LocalSource), "EventBridge bus name")
	source := flag.String("source", envOr("EVENT_SOURCE", publish.LocalSource), "EventBridge source")
	flag.Parse()

	if err := run(*eventType, *id, *bus, *source); err != nil {
		fmt.Fprintln(os.Stderr, "sample:", err)
		os.Exit(1)
	}
}

func run(eventType, id, bus, source string) error {
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()

	if id == "" {
		id = samples.NewEventID()
	}
	event, err := samples.Event(eventType, id)
	if err != nil {
		return err
	}
	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}
	ebID, err := publish.Put(ctx, eventbridge.NewFromConfig(awsCfg), bus, source, event)
	if err != nil {
		return err
	}
	fmt.Printf("sent %s %s to %s (eventbridge id %s)\n", eventType, id, bus, ebID)
	return nil
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

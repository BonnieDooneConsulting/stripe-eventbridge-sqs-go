// Command forwarder is for local development only. Stripe can't deliver to
// LocalStack, so this server stands in for Stripe's EventBridge destination:
// it receives webhooks from `stripe listen`, checks their signatures, and
// puts them on the local EventBridge bus wrapped the same way Stripe does.
//
//	stripe listen --forward-to localhost:8081/stripe
//
// Configuration comes from the environment:
//
//	STRIPE_WEBHOOK_SECRET  required, the whsec_... printed by `stripe listen`
//	EVENT_BUS_NAME         the local bus (default aws.partner/stripe.com/ed_local_development)
//	EVENT_SOURCE           the source to stamp on events (default the same)
//	PORT                   default 8081
//	STRIPE_IGNORE_API_VERSION_MISMATCH  set to true to forward events from other API releases
//
// AWS_REGION and AWS_ENDPOINT_URL are read by the AWS SDK.
package main

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/eventbridge"
	"github.com/stripe/stripe-go/v87/webhook"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/publish"
)

// maxBodyBytes matches EventBridge's 256 KB event size limit.
const maxBodyBytes = 256 << 10

type forwarder struct {
	secret  string
	options webhook.ConstructEventOptions
	client  publish.API
	bus     string
	source  string
	logger  *slog.Logger
}

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))

	secret := os.Getenv("STRIPE_WEBHOOK_SECRET")
	if secret == "" {
		logger.Error("STRIPE_WEBHOOK_SECRET is required (copy the whsec_... that `stripe listen` prints)")
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		logger.Error("load AWS config", "error", err)
		os.Exit(1)
	}

	f := &forwarder{
		secret: secret,
		options: webhook.ConstructEventOptions{
			IgnoreAPIVersionMismatch: os.Getenv("STRIPE_IGNORE_API_VERSION_MISMATCH") == "true",
		},
		client: eventbridge.NewFromConfig(awsCfg),
		bus:    envOr("EVENT_BUS_NAME", publish.LocalSource),
		source: envOr("EVENT_SOURCE", publish.LocalSource),
		logger: logger,
	}

	mux := http.NewServeMux()
	mux.Handle("POST /stripe", f)
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	addr := ":" + envOr("PORT", "8081")
	srv := &http.Server{
		Addr:              addr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      30 * time.Second,
	}

	go func() {
		logger.Info("forwarding Stripe webhooks to EventBridge", "addr", addr, "bus", f.bus, "source", f.source)
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			logger.Error("server failed", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		logger.Error("shutdown failed", "error", err)
	}
}

func (f *forwarder) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	payload, err := io.ReadAll(http.MaxBytesReader(w, r.Body, maxBodyBytes))
	if err != nil {
		http.Error(w, "could not read body", http.StatusBadRequest)
		return
	}

	// Verify first, even locally. It's the same check your production
	// webhook code would do, and it catches a stale whsec_ right away.
	event, err := webhook.ConstructEventWithOptions(payload, r.Header.Get("Stripe-Signature"), f.secret, f.options)
	if err != nil {
		f.logger.Warn("rejected webhook", "error", err)
		http.Error(w, "invalid signature", http.StatusBadRequest)
		return
	}

	// Forward the verified payload untouched: Stripe's EventBridge
	// destination puts the same Event JSON under "detail".
	id, err := publish.Put(r.Context(), f.client, f.bus, f.source, payload)
	if err != nil {
		f.logger.Error("could not put event on EventBridge", "event_id", event.ID, "error", err)
		http.Error(w, "could not forward event", http.StatusBadGateway)
		return
	}
	f.logger.Info("forwarded event", "event_id", event.ID, "event_type", event.Type, "eventbridge_id", id)
	w.WriteHeader(http.StatusOK)
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}

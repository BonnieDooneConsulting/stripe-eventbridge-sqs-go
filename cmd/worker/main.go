// Command worker reads Stripe events from an SQS queue fed by EventBridge and
// processes them.
//
// Configuration comes from the environment:
//
//	QUEUE_URL         required, the queue the EventBridge rule targets
//	AWS_REGION        the queue's region
//	AWS_ENDPOINT_URL  optional, e.g. http://localhost:4566 for LocalStack
//	DEDUPE_TABLE      optional DynamoDB table for idempotency; in-memory if unset
//	CONCURRENCY       optional number of polling loops (default 2)
//	LOG_LEVEL         optional: debug, info (default), warn, error
//
// AWS_REGION and AWS_ENDPOINT_URL are read by the AWS SDK itself, along with
// the usual credential settings.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/service/dynamodb"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"

	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/billing"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/dynamostore"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/events"
	"github.com/BonnieDooneConsulting/stripe-eventbridge-sqs-go/internal/worker"
)

const (
	// Stripe retries for up to three days, so remember processed events a
	// bit longer than that.
	dedupeTTL = 4 * 24 * time.Hour
	// handlerTimeout must stay below the queue's visibility timeout (60s in
	// the LocalStack setup and the Terraform example).
	handlerTimeout = 30 * time.Second
	// claimTimeout is how long an in-progress claim blocks duplicates before
	// it's considered abandoned.
	claimTimeout = handlerTimeout + 15*time.Second
)

func main() {
	logger := slog.New(slog.NewJSONHandler(os.Stdout, &slog.HandlerOptions{Level: logLevel()}))
	if err := run(logger); err != nil {
		logger.Error("worker failed", "error", err)
		os.Exit(1)
	}
}

func run(logger *slog.Logger) error {
	queueURL := os.Getenv("QUEUE_URL")
	if queueURL == "" {
		return fmt.Errorf("QUEUE_URL is required")
	}
	concurrency, err := intEnv("CONCURRENCY", 2)
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	awsCfg, err := config.LoadDefaultConfig(ctx)
	if err != nil {
		return fmt.Errorf("load AWS config: %w", err)
	}

	var store events.EventStore
	if table := os.Getenv("DEDUPE_TABLE"); table != "" {
		store = dynamostore.New(dynamodb.NewFromConfig(awsCfg), table, dedupeTTL, claimTimeout)
		logger.Info("using DynamoDB event store", "table", table)
	} else {
		store = events.NewMemoryStore(dedupeTTL, claimTimeout)
		logger.Warn("DEDUPE_TABLE not set, using in-memory event store (fine locally, not with more than one worker)")
	}

	dispatcher := events.NewDispatcher(store, events.WithLogger(logger))
	billing.Register(dispatcher, billing.LogAccounts{Logger: logger})

	w := worker.New(sqs.NewFromConfig(awsCfg), func(ctx context.Context, msg types.Message) error {
		return dispatcher.Handle(ctx, []byte(aws.ToString(msg.Body)))
	}, worker.Config{
		QueueURL:       queueURL,
		Concurrency:    concurrency,
		HandlerTimeout: handlerTimeout,
		Logger:         logger,
	})

	// Run returns after SIGINT/SIGTERM once in-flight messages are done.
	return w.Run(ctx)
}

func intEnv(key string, fallback int) (int, error) {
	v := os.Getenv(key)
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil || n < 1 {
		return 0, fmt.Errorf("%s must be a positive integer, got %q", key, v)
	}
	return n, nil
}

func logLevel() slog.Level {
	var l slog.Level
	if err := l.UnmarshalText([]byte(os.Getenv("LOG_LEVEL"))); err != nil {
		return slog.LevelInfo
	}
	return l
}

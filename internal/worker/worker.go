// Package worker pulls messages from an SQS queue, hands each one to a
// ProcessFunc, and deletes the ones that succeeded.
//
// The retry story lives almost entirely in SQS, so it's worth spelling out:
//
//   - ReceiveMessage hides each message for the queue's visibility timeout.
//   - If ProcessFunc returns nil, the worker deletes the message. Done.
//   - If ProcessFunc returns an error, the worker does nothing. When the
//     visibility timeout runs out, SQS makes the message visible again and
//     some worker receives it again.
//   - Every receive bumps the message's receive count. Once it passes the
//     redrive policy's maxReceiveCount, SQS moves it to the dead-letter queue
//     instead of handing it out again.
//
// So "leave the message alone" is the retry mechanism. There's no retry loop
// in this code on purpose.
package worker

import (
	"context"
	"fmt"
	"log/slog"
	"strconv"
	"sync"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/sqs"
	"github.com/aws/aws-sdk-go-v2/service/sqs/types"
)

// SQSAPI is the part of the SQS client the worker uses. *sqs.Client
// satisfies it, and tests use a fake.
type SQSAPI interface {
	ReceiveMessage(ctx context.Context, in *sqs.ReceiveMessageInput, optFns ...func(*sqs.Options)) (*sqs.ReceiveMessageOutput, error)
	DeleteMessageBatch(ctx context.Context, in *sqs.DeleteMessageBatchInput, optFns ...func(*sqs.Options)) (*sqs.DeleteMessageBatchOutput, error)
}

// ProcessFunc handles one message. Return nil to delete it, or an error to
// leave it on the queue for a retry.
type ProcessFunc func(ctx context.Context, msg types.Message) error

// Config holds the worker's settings. Zero values get sensible defaults.
type Config struct {
	// QueueURL is the queue to read from. Required.
	QueueURL string
	// Concurrency is the number of polling loops. Each loop receives up to
	// BatchSize messages and processes them in parallel, so the most
	// messages in flight at once is Concurrency * BatchSize. Default 1.
	Concurrency int
	// BatchSize is MaxNumberOfMessages per receive, 1 to 10. Default 10.
	BatchSize int32
	// WaitTime is the long-poll wait per receive. Default (and max) 20s.
	// Long polling means an idle worker makes one cheap call every 20
	// seconds instead of hammering the API.
	WaitTime time.Duration
	// HandlerTimeout bounds each ProcessFunc call. Keep it comfortably
	// below the queue's visibility timeout, or a slow message can become
	// visible again while it's still being worked on. Default 30s.
	HandlerTimeout time.Duration
	// Logger receives worker logs. The default discards them.
	Logger *slog.Logger
}

// Worker consumes one SQS queue.
type Worker struct {
	client  SQSAPI
	process ProcessFunc
	cfg     Config
	log     *slog.Logger
	// sleep is swapped out in tests to skip the error backoff.
	sleep func(ctx context.Context, d time.Duration)
}

// New returns a Worker. It panics if cfg.QueueURL is empty, since that's a
// programming error rather than something to handle at runtime.
func New(client SQSAPI, process ProcessFunc, cfg Config) *Worker {
	if cfg.QueueURL == "" {
		panic("worker: QueueURL is required")
	}
	if cfg.Concurrency <= 0 {
		cfg.Concurrency = 1
	}
	if cfg.BatchSize <= 0 || cfg.BatchSize > 10 {
		cfg.BatchSize = 10
	}
	if cfg.WaitTime <= 0 || cfg.WaitTime > 20*time.Second {
		cfg.WaitTime = 20 * time.Second
	}
	if cfg.HandlerTimeout <= 0 {
		cfg.HandlerTimeout = 30 * time.Second
	}
	log := cfg.Logger
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	return &Worker{client: client, process: process, cfg: cfg, log: log, sleep: sleepCtx}
}

// Run polls until ctx is canceled, then waits for in-flight messages to
// finish and returns. Cancel ctx on SIGINT or SIGTERM for a graceful
// shutdown: no new messages are received, but anything already received is
// processed and deleted (or left for retry) before Run returns.
func (w *Worker) Run(ctx context.Context) error {
	w.log.Info("worker starting", "queue_url", w.cfg.QueueURL, "concurrency", w.cfg.Concurrency, "batch_size", w.cfg.BatchSize)
	var wg sync.WaitGroup
	for i := range w.cfg.Concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			w.poll(ctx, i)
		}()
	}
	wg.Wait()
	w.log.Info("worker stopped")
	return nil
}

func (w *Worker) poll(ctx context.Context, id int) {
	log := w.log.With("poller", id)
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		out, err := w.client.ReceiveMessage(ctx, &sqs.ReceiveMessageInput{
			QueueUrl:            aws.String(w.cfg.QueueURL),
			MaxNumberOfMessages: w.cfg.BatchSize,
			WaitTimeSeconds:     int32(w.cfg.WaitTime / time.Second), //nolint:gosec // New caps WaitTime at 20s

			MessageSystemAttributeNames: []types.MessageSystemAttributeName{types.MessageSystemAttributeNameApproximateReceiveCount},
		})
		if err != nil {
			if ctx.Err() != nil {
				// Shutting down; the long poll was interrupted.
				return
			}
			log.Error("receive failed, backing off", "error", err, "backoff", backoff)
			w.sleep(ctx, backoff)
			backoff = min(backoff*2, 30*time.Second)
			continue
		}
		backoff = time.Second
		if len(out.Messages) == 0 {
			continue
		}
		// Received messages are ours now. Finish them even if shutdown
		// starts halfway through, so they don't sit invisible until the
		// visibility timeout for no reason.
		w.handleBatch(context.WithoutCancel(ctx), log, out.Messages)
	}
}

func (w *Worker) handleBatch(ctx context.Context, log *slog.Logger, msgs []types.Message) {
	var (
		mu   sync.Mutex
		done []types.DeleteMessageBatchRequestEntry
		wg   sync.WaitGroup
	)
	for i, msg := range msgs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := w.processOne(ctx, log, msg); err != nil {
				return
			}
			mu.Lock()
			done = append(done, types.DeleteMessageBatchRequestEntry{
				// Batch entry IDs only need to be unique within the request.
				Id:            aws.String(strconv.Itoa(i)),
				ReceiptHandle: msg.ReceiptHandle,
			})
			mu.Unlock()
		}()
	}
	wg.Wait()
	w.deleteBatch(ctx, log, done)
}

func (w *Worker) processOne(ctx context.Context, log *slog.Logger, msg types.Message) (err error) {
	log = log.With("message_id", aws.ToString(msg.MessageId), "receive_count", msg.Attributes[string(types.MessageSystemAttributeNameApproximateReceiveCount)])
	ctx, cancel := context.WithTimeout(ctx, w.cfg.HandlerTimeout)
	defer cancel()

	defer func() {
		// A panic in one handler shouldn't take down every in-flight
		// message. Treat it like any other failure.
		if r := recover(); r != nil {
			err = fmt.Errorf("panic: %v", r)
			log.Error("message handler panicked, leaving message for retry", "panic", r)
		}
	}()

	if err := w.process(ctx, msg); err != nil {
		log.Warn("message failed, leaving it for retry", "error", err)
		return err
	}
	return nil
}

func (w *Worker) deleteBatch(ctx context.Context, log *slog.Logger, entries []types.DeleteMessageBatchRequestEntry) {
	if len(entries) == 0 {
		return
	}
	ctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	out, err := w.client.DeleteMessageBatch(ctx, &sqs.DeleteMessageBatchInput{
		QueueUrl: aws.String(w.cfg.QueueURL),
		Entries:  entries,
	})
	if err != nil {
		// The messages will come back after the visibility timeout. The
		// event store recognizes them as duplicates, so this costs a little
		// extra work but no double side effects.
		log.Error("delete batch failed, messages will be redelivered", "error", err, "count", len(entries))
		return
	}
	for _, f := range out.Failed {
		log.Error("delete failed, message will be redelivered", "entry", aws.ToString(f.Id), "code", aws.ToString(f.Code), "message", aws.ToString(f.Message))
	}
}

func sleepCtx(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}

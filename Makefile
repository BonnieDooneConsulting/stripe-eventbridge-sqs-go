# Everything here runs against LocalStack. Set LOCALSTACK_PORT if 4566 is
# already taken on your machine.
LOCALSTACK_PORT ?= 4566

export LOCALSTACK_PORT
export AWS_ENDPOINT_URL ?= http://localhost:$(LOCALSTACK_PORT)
export AWS_REGION ?= us-east-1
export AWS_ACCESS_KEY_ID ?= test
export AWS_SECRET_ACCESS_KEY ?= test
export QUEUE_URL ?= $(AWS_ENDPOINT_URL)/000000000000/stripe-events
export DEDUPE_TABLE ?= stripe-events-dedupe

TYPE ?= checkout.session.completed
GOLANGCI_LINT ?= go run github.com/golangci/golangci-lint/v2/cmd/golangci-lint@latest

.PHONY: localstack-up localstack-down run-worker run-forwarder sample dlq test test-integration lint

localstack-up: ## Start LocalStack and create the bus, rule, queues, and table
	docker compose up -d --wait

localstack-down: ## Stop LocalStack
	docker compose down

run-worker: ## Run the worker against LocalStack
	go run ./cmd/worker

run-forwarder: ## Forward `stripe listen` webhooks onto the local bus (needs STRIPE_WEBHOOK_SECRET)
	go run ./cmd/forwarder

sample: ## Send a sample Stripe event (TYPE=invoice.payment_failed, ID=evt_... to test duplicates)
	go run ./cmd/sample -type $(TYPE) $(if $(ID),-id $(ID))

dlq: ## Peek at the dead-letter queue
	docker compose exec -T localstack awslocal sqs receive-message \
		--queue-url http://localhost:4566/000000000000/stripe-events-dlq \
		--max-number-of-messages 10 --visibility-timeout 0

test: ## Unit tests
	go vet ./...
	go test -race ./...

test-integration: ## Integration tests (needs `make localstack-up`)
	go test -tags integration -race -count=1 ./integration/...

lint: ## golangci-lint v2
	$(GOLANGCI_LINT) run ./...

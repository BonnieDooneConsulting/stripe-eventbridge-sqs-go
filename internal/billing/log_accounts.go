package billing

import (
	"context"
	"log/slog"
)

// LogAccounts is a stand-in Accounts that only logs what it would do. It
// keeps the example runnable without a database.
type LogAccounts struct {
	Logger *slog.Logger
}

func (a LogAccounts) Activate(_ context.Context, customerID, subscriptionID, reference string) error {
	a.Logger.Info("activate account", "customer", customerID, "subscription", subscriptionID, "reference", reference)
	return nil
}

func (a LogAccounts) MarkPaymentFailed(_ context.Context, customerID, invoiceID string, attempt int64) error {
	a.Logger.Info("payment failed", "customer", customerID, "invoice", invoiceID, "attempt", attempt)
	return nil
}

func (a LogAccounts) Deactivate(_ context.Context, customerID, subscriptionID string) error {
	a.Logger.Info("deactivate account", "customer", customerID, "subscription", subscriptionID)
	return nil
}

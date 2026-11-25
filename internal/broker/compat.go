package broker

import (
	"context"
	"errors"
	"time"
)

// Small stdlib aliases kept so call sites stay terse.

func contextWithTimeout(parent context.Context, d time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(parent, d)
}

func errorsIs(err, target error) bool { return errors.Is(err, target) }

// ctxErr reports whether err is (or wraps) a context cancellation: session
// teardown on shutdown, not an operational failure worth logging as warn.
func ctxErr(err error) error {
	if err == nil {
		return nil
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	return nil
}

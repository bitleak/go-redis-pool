package pool

import (
	"context"
	"net"

	redis "github.com/go-redis/redis/v7"
)

type failureHook struct {
	*client
}

func newFailureHook(c *client) *failureHook {
	return &failureHook{
		client: c,
	}
}

func (hook *failureHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (hook *failureHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if isNetworkError(cmd.Err()) {
		hook.onFailure()
	} else {
		hook.onSuccess()
	}
	return nil
}

func (hook *failureHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (hook *failureHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	for _, cmd := range cmds {
		if isNetworkError(cmd.Err()) {
			hook.client.onFailure()
			return nil
		}
	}
	hook.onSuccess()
	return nil
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// Network error
	_, ok := err.(net.Error)
	return ok
}

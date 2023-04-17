package pool

import (
	"context"
	"net"

	"github.com/redis/go-redis/v9"
)

type failureHook struct {
	*client
}

var _ redis.Hook = failureHook{}

func newFailureHook(c *client) *failureHook {
	return &failureHook{
		client: c,
	}
}

func (h failureHook) DialHook(next redis.DialHook) redis.DialHook {
	return func(ctx context.Context, network, addr string) (net.Conn, error) {
		conn, err := next(ctx, network, addr)

		if isNetworkError(err) {
			h.onFailure()
		}

		return conn, err
	}
}

func (h failureHook) ProcessHook(next redis.ProcessHook) redis.ProcessHook {
	return func(ctx context.Context, cmd redis.Cmder) error {
		err := next(ctx, cmd)

		if !isNetworkError(err) {
			h.onSuccess()
		}

		return err
	}
}

func (h failureHook) ProcessPipelineHook(next redis.ProcessPipelineHook) redis.ProcessPipelineHook {
	return func(ctx context.Context, cmds []redis.Cmder) error {
		err := next(ctx, cmds)

		if !isNetworkError(err) {
			h.onSuccess()
		}

		return err
	}
}

func isNetworkError(err error) bool {
	if err == nil {
		return false
	}
	// Network error
	_, ok := err.(net.Error)
	return ok
}

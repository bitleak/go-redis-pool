package pool

import (
	"context"
	"net"
	"strings"
	"sync/atomic"
	"time"

	"github.com/go-redis/redis"
)

type autoEjectHostHook struct {
	failureCount           int32
	serverFailureLimit     int32
	retryTicker            *time.Ticker
	stopChan               chan struct{}
	afterReachFailureLimit func()
	tryRejoin              func()
}

func newAutoEjectHostHook(serverRetryTimeout time.Duration, serverFailureLimit int32,
	afterReachFailureLimit, tryRejoin func()) *autoEjectHostHook {
	hook := &autoEjectHostHook{
		failureCount:           0,
		serverFailureLimit:     serverFailureLimit,
		retryTicker:            time.NewTicker(serverRetryTimeout),
		stopChan:               make(chan struct{}, 0),
		afterReachFailureLimit: afterReachFailureLimit,
		tryRejoin:              tryRejoin,
	}
	return hook
}

func (hook *autoEjectHostHook) startRetryTick() {
	go func() {
		for {
			select {
			case <-hook.stopChan:
				return
			case <-hook.retryTicker.C:
				if hook.failureCount >= hook.serverFailureLimit {
					atomic.StoreInt32(&hook.failureCount, hook.serverFailureLimit-1)
					hook.tryRejoin()
					continue
				}
				if hook.failureCount > 0 {
					atomic.AddInt32(&hook.failureCount, -1)
				}
			}
		}
	}()
}

func (hook *autoEjectHostHook) incFailure() {
	if atomic.AddInt32(&hook.failureCount, 1) == hook.serverFailureLimit {
		hook.afterReachFailureLimit()
	}
}

func (hook *autoEjectHostHook) isHostAvailable() bool {
	return hook.failureCount < hook.serverFailureLimit
}

func (hook *autoEjectHostHook) close() {
	hook.stopChan <- struct{}{}
}

func (hook *autoEjectHostHook) BeforeProcess(ctx context.Context, cmd redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (hook *autoEjectHostHook) AfterProcess(ctx context.Context, cmd redis.Cmder) error {
	if isBrokenError(cmd.Err()) {
		hook.incFailure()
	}
	return nil
}

func (hook *autoEjectHostHook) BeforeProcessPipeline(ctx context.Context, cmds []redis.Cmder) (context.Context, error) {
	return ctx, nil
}

func (hook *autoEjectHostHook) AfterProcessPipeline(ctx context.Context, cmds []redis.Cmder) error {
	for _, cmd := range cmds {
		if isBrokenError(cmd.Err()) {
			hook.incFailure()
			break
		}
	}
	return nil
}

func isBrokenError(err error) bool {
	if err == nil {
		return false
	}

	// Network error
	if _, ok := err.(net.Error); ok {
		return true
	}

	s := err.Error()
	if strings.HasPrefix(s, "CLUSTERDOWN ") {
		return true
	}

	return false
}

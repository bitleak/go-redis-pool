package pool

import (
	"context"
	"net"
	"time"

	"github.com/go-redis/redis/v7"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Hooks", func() {

	Describe("AutoEjectHostHook", func() {

		var hook *autoEjectHostHook
		var executeAfterReachFailureLimitTimes, executeTryRejointTimes int

		BeforeEach(func() {
			executeAfterReachFailureLimitTimes, executeTryRejointTimes = 0, 0
			hook = newAutoEjectHostHook(50*time.Millisecond, 3, func() {
				executeAfterReachFailureLimitTimes++
			}, func() {
				executeTryRejointTimes++
			})
		})

		It("Eject And Rejoin", func() {
			Expect(hook.isHostAvailable()).To(Equal(true))
			cmd := redis.NewCmd()
			cmd.SetErr(&net.AddrError{})
			for i := 0; i < 3; i++ {
				_ = hook.AfterProcess(context.Background(), cmd)
			}
			Expect(hook.isHostAvailable()).To(Equal(false))
			Expect(executeAfterReachFailureLimitTimes).To(Equal(1))
			time.Sleep(60 * time.Millisecond)
			Expect(hook.isHostAvailable()).To(Equal(true))
			Expect(executeTryRejointTimes).To(Equal(1))
		})

		It("Eject And Rejoin (pipeline)", func() {
			Expect(hook.isHostAvailable()).To(Equal(true))
			cmd := redis.NewCmd()
			cmd.SetErr(&net.AddrError{})
			for i := 0; i < 3; i++ {
				_ = hook.AfterProcessPipeline(context.Background(), []redis.Cmder{cmd})
			}
			Expect(hook.isHostAvailable()).To(Equal(false))
			Expect(executeAfterReachFailureLimitTimes).To(Equal(1))
			time.Sleep(60 * time.Millisecond)
			Expect(hook.isHostAvailable()).To(Equal(true))
			Expect(executeTryRejointTimes).To(Equal(1))
		})

	})
})

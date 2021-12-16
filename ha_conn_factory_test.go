package pool

import (
	"errors"
	"testing"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("client pool", func() {})

func TestClientPool(t *testing.T) {
	It("eject/rejoin host", func() {
		cfg := &HAConfig{
			Master:             "addr:0",
			Slaves:             []string{"addr:0:100", "addr:0:200", "addr:0:300"},
			AutoEjectHost:      true,
			ServerRetryTimeout: 100 * time.Millisecond,
			ServerFailureLimit: 3,
		}
		_ = cfg.init()
		p := newClientPool(cfg)

		Expect(p.alives).To(Equal(p.slaves))
		p.slaves[0].failureCount = p.serverFailureLimit
		time.Sleep(120 * time.Millisecond)
		// the slave 0 should be evicted
		Expect(p.alives).To(Equal(p.slaves[1:]))
		time.Sleep(120 * time.Millisecond)
		// the slave 0 should be rejoined
		Expect(p.alives[0].failureCount).To(Equal(p.serverFailureLimit - 1))
		Expect(p.alives).To(Equal(p.slaves))
	})

	It("not eject/rejoin host", func() {
		cfg := &HAConfig{
			Master:             "addr:0",
			Slaves:             []string{"addr:0:100", "addr:0:200", "addr:0:300"},
			AutoEjectHost:      false,
			ServerRetryTimeout: 100 * time.Millisecond,
			ServerFailureLimit: 3,
		}
		_ = cfg.init()
		p := newClientPool(cfg)

		Expect(p.alives).To(Equal(p.slaves))
		p.slaves[0].failureCount = p.serverFailureLimit
		time.Sleep(120 * time.Millisecond)
		Expect(p.alives).To(Equal(p.slaves))
		time.Sleep(120 * time.Millisecond)
		Expect(p.alives[0].failureCount).To(Equal(p.serverFailureLimit))
		Expect(p.alives).To(Equal(p.slaves))
	})

	It("get conn", func() {
		cfg := &HAConfig{
			Master:             "addr:0",
			Slaves:             []string{"addr:0:100", "addr:0:200", "addr:0:300"},
			AutoEjectHost:      true,
			ServerRetryTimeout: 100 * time.Millisecond,
			ServerFailureLimit: 3,
			PollType:           PollByWeight,
		}
		_ = cfg.init()
		p := newClientPool(cfg)
		for i := range p.slaves {
			p.slaves[i].failureCount = p.serverFailureLimit
		}
		time.Sleep(120 * time.Millisecond)
		_, err := p.getConn()
		Expect(err).To(Equal(errors.New("no alive slaves")))
		for i := range p.slaves {
			p.slaves[i].failureCount = p.serverFailureLimit - 1
		}
		_, err = p.getConn()
		Expect(err).NotTo(Equal(HaveOccurred()))
	})

	It("min server num", func() {
		cfg := &HAConfig{
			Master:             "addr:0",
			Slaves:             []string{"addr:0:100", "addr:0:200", "addr:0:300"},
			AutoEjectHost:      true,
			ServerRetryTimeout: 100 * time.Millisecond,
			ServerFailureLimit: 3,
			PollType:           PollByWeight,
			MinServerNum:       2,
		}
		_ = cfg.init()
		p := newClientPool(cfg)
		time.Sleep(120 * time.Millisecond)
		Expect(p.alives).To(Equal(p.slaves))
		p.slaves[0].failureCount = p.serverFailureLimit
		p.slaves[1].failureCount = p.serverFailureLimit
		time.Sleep(120 * time.Millisecond)
		Expect(len(p.alives)).To(Equal(p.minServerNum))
	})
}

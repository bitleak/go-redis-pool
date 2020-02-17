package pool

import (
	"errors"
	"math/rand"
	"time"

	redis "github.com/go-redis/redis/v7"
)

const (
	// DistRandom selects the slave factory by random index
	DistRandom = iota + 1
	// DistByWeight selecst the slave factory by weight
	DistByWeight
	// DistByWeight selecst the slave factory by round robin
	DistRR
)

type NodeConfig struct {
	Addr     string
	Password string
	Weight   int
}

// TODO: supports sentinel
type HAConfig struct {
	Master   *NodeConfig
	Slaves   []*NodeConfig
	Options  *redis.Options
	DistType int
}

// HAConnFactory impls the read/write splits between master and slaves
type HAConnFactory struct {
	cfg    *HAConfig
	master *redis.Client
	slaves []*redis.Client

	ind          int
	rand         *rand.Rand
	weightRanges []int
}

func (cfg *HAConfig) init() {
	if cfg.DistType < DistRandom || cfg.DistType > DistRR {
		cfg.DistType = DistRR
	}
	if cfg.DistType == DistByWeight {
		if cfg.Master.Weight <= 0 {
			cfg.Master.Weight = 100
		}
		for i, slave := range cfg.Slaves {
			if slave.Weight <= 0 {
				cfg.Slaves[i].Weight = 100
			}
		}
	}
}

// NewHAConnFactory create new ha factory
func NewHAConnFactory(cfg *HAConfig) (*HAConnFactory, error) {
	if cfg == nil {
		return nil, errors.New("factory cfg shouldn't be empty")
	}
	cfg.init()

	factory := new(HAConnFactory)
	factory.ind = 0
	factory.rand = rand.New(rand.NewSource(time.Now().UnixNano()))
	factory.cfg = cfg
	options := cfg.Options
	if options == nil {
		options = &redis.Options{}
	}
	options.Addr = cfg.Master.Addr
	options.Password = cfg.Master.Password
	factory.master = redis.NewClient(options)
	factory.slaves = make([]*redis.Client, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaveOptions := *options
		slaveOptions.Addr = slave.Addr
		slaveOptions.Password = slave.Password
		factory.slaves[i] = redis.NewClient(&slaveOptions)
	}
	if cfg.DistType == DistByWeight {
		factory.weightRanges = make([]int, len(cfg.Slaves))
		for i, slave := range cfg.Slaves {
			if i == 0 {
				factory.weightRanges[0] = slave.Weight
			} else {
				factory.weightRanges[i] = factory.weightRanges[i-1] + slave.Weight
			}
		}
	}
	return factory, nil
}

func (factory *HAConnFactory) close() {
	factory.master.Close()
	for _, slave := range factory.slaves {
		slave.Close()
	}
}

// GetSlaveConnByKey get slave connection
func (factory *HAConnFactory) getSlaveConn(key ...string) (*redis.Client, error) {
	if len(factory.slaves) == 0 {
		return nil, errors.New("no alive slave")
	}
	switch factory.cfg.DistType {
	case DistRandom:
		return factory.slaves[factory.rand.Intn(len(factory.slaves))], nil
	case DistRR:
		factory.ind = (factory.ind + 1) % len(factory.slaves)
		return factory.slaves[factory.ind], nil
	case DistByWeight:
		n := len(factory.slaves)
		if n == 1 {
			return factory.slaves[0], nil
		}
		r := factory.rand.Intn(factory.weightRanges[n-1])
		for i, weightRange := range factory.weightRanges {
			if r <= weightRange {
				return factory.slaves[i], nil
			}
		}
	default:
		return nil, errors.New("unsupported distribution type")
	}
	// no reached
	panic("failed to get slave conn")
}

// GetMasterConnByKey get master connection
func (factory *HAConnFactory) getMasterConn(key ...string) (*redis.Client, error) {
	return factory.master, nil
}

package pool

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
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

// TODO: supports sentinel
type HAConfig struct {
	Master           string
	Slaves           []string
	Password         string
	ReadonlyPassword string
	Options          *redis.Options
	DistType         int

	weights []int64
}

// HAConnFactory impls the read/write splits between master and slaves
type HAConnFactory struct {
	cfg    *HAConfig
	master *redis.Client
	slaves []*redis.Client

	ind          int
	rand         *rand.Rand
	weightRanges []int64
}

func (cfg *HAConfig) init() error {
	var err error

	if cfg.DistType < DistRandom || cfg.DistType > DistRR {
		cfg.DistType = DistRR
	}
	if cfg.Options == nil {
		cfg.Options = &redis.Options{}
	}
	cfg.weights = make([]int64, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		elems := strings.Split(slave, ":")
		cfg.weights[i] = 100
		if len(elems) == 3 {
			cfg.weights[i], err = strconv.ParseInt(elems[2], 10, 64)
			if err != nil {
				return errors.New("the weight should be integer")
			}
		}
	}
	return nil
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
	options.Addr = cfg.Master
	options.Password = cfg.Password
	factory.master = redis.NewClient(options)
	slavePassword := cfg.Password
	if cfg.ReadonlyPassword != "" {
		slavePassword = cfg.ReadonlyPassword
	}
	factory.slaves = make([]*redis.Client, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaveOptions := *options
		slaveOptions.Addr = slave
		slaveOptions.Password = slavePassword
		factory.slaves[i] = redis.NewClient(&slaveOptions)
	}
	if cfg.DistType == DistByWeight {
		factory.weightRanges = make([]int64, len(cfg.Slaves))
		factory.weightRanges[0] = cfg.weights[0]
		for i := 1; i < len(cfg.Slaves); i++ {
			factory.weightRanges[i] = factory.weightRanges[i-1] + cfg.weights[i]
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
		r := factory.rand.Int63n(factory.weightRanges[n-1])
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

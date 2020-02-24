package pool

import (
	"errors"
	"math/rand"
	"strconv"
	"strings"
	"time"

	"github.com/go-redis/redis/v7"
)

const (
	// PollByRandom selects the slave factory by random index
	PollByRandom = iota + 1
	// PollByWeight selects the slave factory by weight
	PollByWeight
	// PollByRoundRobin selects the slave with round-robin order
	PollByRoundRobin
)

// TODO: supports sentinel
type HAConfig struct {
	Master           string
	Slaves           []string
	Password         string
	ReadonlyPassword string
	Options          *redis.Options
	PollType         int

	AutoEjectHost      bool
	ServerRetryTimeout time.Duration
	ServerFailureLimit int32

	weights []int64
}

// HAConnFactory impls the read/write splits between master and slaves
type HAConnFactory struct {
	cfg             *HAConfig
	master          *redis.Client
	slaves          []*hAClient
	availableSlaves *clientPool
}

type hAClient struct {
	*redis.Client
	autoEjectHostHook *autoEjectHostHook
}

type clientPool struct {
	ind          int
	rand         *rand.Rand
	weightRanges []int64
	size         int
	pollType     int

	clients []*redis.Client
}

func (cfg *HAConfig) init() error {
	var err error

	if cfg.PollType < PollByRandom || cfg.PollType > PollByRoundRobin {
		cfg.PollType = PollByRoundRobin
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
	if err := cfg.init(); err != nil {
		return nil, err
	}

	factory := new(HAConnFactory)
	factory.cfg = cfg
	options := cfg.Options
	options.Addr = cfg.Master
	options.Password = cfg.Password
	factory.master = redis.NewClient(options)
	slavePassword := cfg.Password
	if cfg.ReadonlyPassword != "" {
		slavePassword = cfg.ReadonlyPassword
	}
	if len(cfg.Slaves) == 0 {
		cfg.Slaves = append(cfg.Slaves, cfg.Master)
	}
	factory.slaves = make([]*hAClient, len(cfg.Slaves))
	for i, slave := range cfg.Slaves {
		slaveOptions := *options
		slaveOptions.Addr = slave
		slaveOptions.Password = slavePassword
		factory.slaves[i] = newHaClient(&slaveOptions, cfg, factory.refreshAvailableSlaves, factory.refreshAvailableSlaves)
	}
	factory.refreshAvailableSlaves()
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
	return factory.availableSlaves.get(key...)
}

// GetMasterConnByKey get master connection
func (factory *HAConnFactory) getMasterConn(key ...string) (*redis.Client, error) {
	return factory.master, nil
}

func (factory *HAConnFactory) refreshAvailableSlaves() {
	availables := make([]*redis.Client, 0)
	for i := 0; i < len(factory.slaves); i++ {
		if factory.slaves[i].isAvailable() {
			availables = append(availables, factory.slaves[i].Client)
		}
	}
	if factory.availableSlaves == nil || !factory.availableSlaves.clientsEqual(availables) {
		factory.availableSlaves = newClientPool(availables, factory.cfg)
	}
}

func newHaClient(options *redis.Options, cfg *HAConfig, afterReachFailureLimit, tryRejoin func()) *hAClient {
	haClient := &hAClient{
		Client: redis.NewClient(options),
	}
	if cfg.AutoEjectHost {
		haClient.autoEjectHostHook = newAutoEjectHostHook(cfg.ServerRetryTimeout, cfg.ServerFailureLimit, afterReachFailureLimit, tryRejoin)
	}
	return haClient
}

func (client *hAClient) isAvailable() bool {
	return client.autoEjectHostHook == nil || client.autoEjectHostHook.isHostAvailable()
}

func (client *hAClient) Close() error {
	if client.autoEjectHostHook != nil {
		client.autoEjectHostHook.close()
	}
	return client.Client.Close()
}

func newClientPool(clients []*redis.Client, cfg *HAConfig) *clientPool {
	pool := &clientPool{
		clients:  clients,
		rand:     rand.New(rand.NewSource(time.Now().UnixNano())),
		size:     len(clients),
		pollType: cfg.PollType,
	}
	if pool.pollType == PollByWeight {
		pool.weightRanges = make([]int64, len(cfg.Slaves))
		pool.weightRanges[0] = cfg.weights[0]
		for i := 1; i < len(cfg.Slaves); i++ {
			pool.weightRanges[i] = pool.weightRanges[i-1] + cfg.weights[i]
		}
	}
	return pool
}

func (pool *clientPool) get(key ...string) (*redis.Client, error) {
	if pool.size == 0 {
		return nil, errors.New("no alive clients")
	}
	if pool.size == 1 {
		return pool.clients[0], nil
	}

	switch pool.pollType {
	case PollByRandom:
		return pool.clients[pool.rand.Intn(pool.size)], nil
	case PollByRoundRobin:
		pool.ind = (pool.ind + 1) % pool.size
		return pool.clients[pool.ind], nil
	case PollByWeight:
		r := pool.rand.Int63n(pool.weightRanges[pool.size-1])
		for i, weightRange := range pool.weightRanges {
			if r <= weightRange {
				return pool.clients[i], nil
			}
		}
		// no reached
		panic("failed to get available slave conn")
	default:
		return nil, errors.New("unsupported distribution type")
	}
}

func (pool *clientPool) clientsEqual(list []*redis.Client) bool {
	if pool.size != len(list) {
		return false
	}
	for i := 0; i < pool.size; i++ {
		if pool.clients[i] != list[i] {
			return false
		}
	}
	return true
}

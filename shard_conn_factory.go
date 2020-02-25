package pool

import (
	"errors"
	"hash/crc32"
	"sync"

	redis "github.com/go-redis/redis/v7"
	"github.com/meitu/go-redis-pool/hashkit"
)

const (
	// DistributeByModular selects the sharding factory by modular
	DistributeByModular = iota + 1
	// DistributeByKetama selects the sharding factory by ketama consistent algorithm
	DistributeByKetama
)

var (
	errMoreThanOneParam      = errors.New("the number of params shouldn't be greater than 1")
	errPartialCommandFailure = errors.New("partital failure in command")
)

type ShardConfig struct {
	Shards         []*HAConfig
	DistributeType int // distribution type of the shards, supports `DistributeByModular` or `DistributeByKetama`
	HashFn         func(key []byte) uint32
}

type ShardConnFactory struct {
	cfg    *ShardConfig
	shards []*HAConnFactory
	hash   hashkit.HashKit
}

func NewShardConnFactory(cfg *ShardConfig) (*ShardConnFactory, error) {
	factory := &ShardConnFactory{
		cfg:    cfg,
		shards: make([]*HAConnFactory, len(cfg.Shards)),
	}
	if factory.cfg.DistributeType < DistributeByModular || factory.cfg.DistributeType > DistributeByKetama {
		cfg.DistributeType = DistributeByModular
	}
	var err error
	for i, shard := range cfg.Shards {
		if factory.shards[i], err = NewHAConnFactory(shard); err != nil {
			return nil, err
		}
	}
	if factory.cfg.HashFn == nil {
		factory.cfg.HashFn = crc32.ChecksumIEEE
	}
	if cfg.DistributeType == DistributeByKetama {
		servers := make([]*hashkit.Server, 0)
		for idx, shard := range factory.shards {
			servers = append(servers, &hashkit.Server{
				Name:   shard.cfg.Master,
				Weight: 1,
				Index:  uint32(idx),
			})
		}
		factory.hash = hashkit.NewKetama(servers, factory.cfg.HashFn)
	}
	return factory, nil
}

func (factory *ShardConnFactory) close() {
	for _, shard := range factory.shards {
		shard.close()
	}
}

func (factory *ShardConnFactory) getShardIndex(key string) uint32 {
	key = extractHashPrefix(key)
	switch factory.cfg.DistributeType {
	case DistributeByKetama:
		return factory.hash.Dispatch(key)
	default:
		return factory.cfg.HashFn([]byte(key)) % uint32(len(factory.shards))
	}
}

func (factory *ShardConnFactory) getSlaveConn(key ...string) (*redis.Client, error) {
	if len(key) > 1 {
		return nil, errMoreThanOneParam
	}
	var ind uint32
	ind = 0
	if len(key) > 0 {
		ind = factory.getShardIndex(key[0])
	}
	return factory.shards[ind].getSlaveConn()
}

func (factory *ShardConnFactory) getMasterConn(key ...string) (*redis.Client, error) {
	if len(key) > 1 {
		return nil, errMoreThanOneParam
	}
	var ind uint32
	ind = 0
	if len(key) > 0 {
		ind = factory.getShardIndex(key[0])
	}
	return factory.shards[ind].getMasterConn()
}

func (factory *ShardConnFactory) groupKeysByInd(keys ...string) map[uint32][]string {
	index2Keys := make(map[uint32][]string, 0)
	for i := 0; i < len(keys); i++ {
		ind := factory.getShardIndex(keys[i])
		if _, exists := index2Keys[ind]; !exists {
			index2Keys[ind] = make([]string, 0)
		}
		index2Keys[ind] = append(index2Keys[ind], keys[i])
	}
	return index2Keys
}

func (factory *ShardConnFactory) isCrossMultiShards(keys ...string) bool {
	var ind uint32
	for i := 0; i < len(keys); i++ {
		newInd := factory.getShardIndex(keys[i])
		if i == 0 {
			ind = newInd
		} else if newInd != ind {
			return true
		}
	}
	return false
}

type multiKeyFn func(factory *ShardConnFactory, keys ...string) redis.Cmder

func (factory *ShardConnFactory) doMultiKeys(fn multiKeyFn, keys ...string) []redis.Cmder {
	if len(keys) == 1 {
		return []redis.Cmder{fn(factory, keys...)}
	}
	index2Keys := factory.groupKeysByInd(keys...)
	if len(index2Keys) == 1 {
		return []redis.Cmder{fn(factory, keys...)}
	}

	var mu sync.Mutex
	var wg sync.WaitGroup
	var results []redis.Cmder
	for _, keyList := range index2Keys {
		wg.Add(1)
		go func(keyList []string) {
			defer wg.Done()
			result := fn(factory, keyList...)
			mu.Lock()
			results = append(results, result)
			mu.Unlock()
		}(keyList)
	}
	wg.Wait()
	return results
}

func (factory *ShardConnFactory) doMultiIntCommand(fn multiKeyFn, keys ...string) (int64, error) {
	var err error
	total := int64(0)
	results := factory.doMultiKeys(fn, keys...)
	for _, result := range results {
		cmd := result.(*redis.IntCmd)
		if cmd.Err() != nil {
			err = cmd.Err()
			continue
		}
		total += cmd.Val()
	}
	return total, err
}

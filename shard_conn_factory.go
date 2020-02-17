package pool

import (
	"errors"
	"hash/crc32"
	"sync"

	redis "github.com/go-redis/redis/v7"
)

var (
	errMoreThanOneParam      = errors.New("the number of params shouldn't be greater than 1")
	errPartialCommandFailure = errors.New("partital failure in command")
)

type ShardConfig struct {
	Shards []*HAConfig
	HashFn func(key []byte) uint32
}

type ShardConnFactory struct {
	cfg    *ShardConfig
	shards []*HAConnFactory
}

func NewShardConnFactory(cfg *ShardConfig) (*ShardConnFactory, error) {
	factory := &ShardConnFactory{
		cfg:    cfg,
		shards: make([]*HAConnFactory, len(cfg.Shards)),
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
	return factory, nil
}

func (factory *ShardConnFactory) close() {
	for _, shard := range factory.shards {
		shard.close()
	}
}

func (factory *ShardConnFactory) getSlaveConn(key ...string) (*redis.Client, error) {
	if len(key) > 1 {
		return nil, errMoreThanOneParam
	}
	var ind uint32
	ind = 0
	if len(key) > 0 {
		ind = factory.cfg.HashFn([]byte(key[0])) % uint32(len(factory.shards))
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
		ind = factory.cfg.HashFn([]byte(key[0])) % uint32(len(factory.shards))
	}
	return factory.shards[ind].getMasterConn()
}

func (factory *ShardConnFactory) groupKeysByInd(keys ...string) map[uint32][]string {
	index2Keys := make(map[uint32][]string, 0)
	for i := 0; i < len(keys); i++ {
		ind := factory.cfg.HashFn([]byte(keys[i])) % uint32(len(factory.shards))
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
		newInd := factory.cfg.HashFn([]byte(keys[i])) % uint32(len(factory.shards))
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

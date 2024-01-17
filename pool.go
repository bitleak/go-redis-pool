package pool

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	bitOpAnd = iota + 1
	bitOpOr
	bitOpXor
)

var (
	errWrongArgumentsNumber       = errors.New("wrong number of arguments")
	errWrongNumberValuesArguments = errors.New("wrong number of values for arguments")
	errShardPoolUnSupported       = errors.New("shard pool didn't support the command")
	errCrossMultiShards           = errors.New("cross multi shards was not allowed")
)

type ConnFactory interface {
	getSlaveConn(key ...string) (*redis.Client, error)
	getMasterConn(key ...string) (*redis.Client, error)
	stats() map[string]*redis.PoolStats
	close()
}

func newErrorMapStringIntCmd(err error) *redis.MapStringIntCmd {
	cmd := &redis.MapStringIntCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorBoolSliceCmd(err error) *redis.BoolSliceCmd {
	cmd := &redis.BoolSliceCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorIntCmd(err error) *redis.IntCmd {
	cmd := &redis.IntCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorFloatCmd(err error) *redis.FloatCmd {
	cmd := &redis.FloatCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorSliceCmd(err error) *redis.SliceCmd {
	cmd := &redis.SliceCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorStringStringMapCmd(err error) *redis.MapStringStringCmd {
	cmd := &redis.MapStringStringCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorIntSliceCmd(err error) *redis.IntSliceCmd {
	cmd := &redis.IntSliceCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorDurationCmd(err error) *redis.DurationCmd {
	cmd := &redis.DurationCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorBoolCmd(err error) *redis.BoolCmd {
	cmd := &redis.BoolCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorStatusCmd(err error) *redis.StatusCmd {
	cmd := &redis.StatusCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorStringCmd(err error) *redis.StringCmd {
	cmd := &redis.StringCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorStringSliceCmd(err error) *redis.StringSliceCmd {
	cmd := &redis.StringSliceCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorStringStructMapCmd(err error) *redis.StringStructMapCmd {
	cmd := &redis.StringStructMapCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorZSliceCmd(err error) *redis.ZSliceCmd {
	cmd := &redis.ZSliceCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorScanCmd(err error) *redis.ScanCmd {
	cmd := &redis.ScanCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorGeoCmd(err error) *redis.GeoPosCmd {
	cmd := &redis.GeoPosCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorGeoLocationCmd(err error) *redis.GeoLocationCmd {
	cmd := &redis.GeoLocationCmd{}
	cmd.SetErr(err)
	return cmd
}

func newErrorCmd(err error) *redis.Cmd {
	cmd := &redis.Cmd{}
	cmd.SetErr(err)
	return cmd
}

type Pool struct {
	connFactory ConnFactory
}

func NewHA(cfg *HAConfig) (*Pool, error) {
	factory, err := NewHAConnFactory(cfg)
	if err != nil {
		return nil, err
	}
	return &Pool{
		connFactory: factory,
	}, nil
}

func NewShard(cfg *ShardConfig) (*Pool, error) {
	factory, err := NewShardConnFactory(cfg)
	if err != nil {
		return nil, err
	}
	return &Pool{
		connFactory: factory,
	}, nil
}

func (p *Pool) Stats() map[string]*redis.PoolStats {
	return p.connFactory.stats()
}

func (p *Pool) Close() {
	p.connFactory.close()
}

func (p *Pool) WithMaster(key ...string) (*redis.Client, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		if len(key) != 1 {
			return nil, errWrongNumberValuesArguments
		}
		return p.connFactory.getMasterConn(key...)
	}
	return p.connFactory.getMasterConn()
}

// WithMasterShards returns master connects for shards with executable keys for them
func (p *Pool) WithMasterShards(keys ...string) (map[*redis.Client][]string, error) {
	mapClients := make(map[*redis.Client][]string, 0)
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		if len(keys) == 0 {
			return nil, errWrongNumberValuesArguments
		}
		index2Keys := factory.groupKeysByInd(keys...)
		for ind, keys := range index2Keys {
			conn, _ := factory.shards[ind].getMasterConn()
			mapClients[conn] = keys
		}
		return mapClients, nil
	}
	conn, _ := p.connFactory.getMasterConn()
	mapClients[conn] = keys
	return mapClients, nil
}

// Pipeline returns pipeline for HA configuration, to get a pipeline for shards use WithMasterShards
func (p *Pool) Pipeline() (redis.Pipeliner, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Pipeline(), nil
}

func (p *Pool) Pipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Pipelined(ctx, fn)
}

// TxPipeline returns pipeline (with MULTI/EXEC) for HA configuration, to get a pipeline for shards use WithMasterShards
func (p *Pool) TxPipeline() (redis.Pipeliner, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.TxPipeline(), nil
}

func (p *Pool) TxPipelined(ctx context.Context, fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.TxPipelined(ctx, fn)
}

func (p *Pool) Ping(ctx context.Context) *redis.StatusCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Ping(ctx)
	}
	var result *redis.StatusCmd
	factory := p.connFactory.(*ShardConnFactory)
	for _, shard := range factory.shards {
		conn, _ := shard.getMasterConn()
		result = conn.Ping(ctx)
		if result.Err() != nil {
			return result
		}
	}
	return result
}

func (p *Pool) Get(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.Get(ctx, key)
}

func (p *Pool) Set(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Set(ctx, key, value, expiration)
}

func (p *Pool) SetNX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SetNX(ctx, key, value, expiration)
}

func (p *Pool) SetXX(ctx context.Context, key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SetXX(ctx, key, value, expiration)
}

func (p *Pool) SetRange(ctx context.Context, key string, offset int64, value string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SetRange(ctx, key, offset, value)
}

func (p *Pool) SetArgs(ctx context.Context, key string, value interface{}, a redis.SetArgs) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.SetArgs(ctx, key, value, a)
}

func (p *Pool) StrLen(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.StrLen(ctx, key)
}

func (p *Pool) Echo(ctx context.Context, message interface{}) *redis.StringCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Echo(ctx, message)
}

func (p *Pool) Del(ctx context.Context, keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Del(ctx, keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Del(ctx, keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Unlink(ctx context.Context, keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Unlink(ctx, keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Unlink(ctx, keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Touch(ctx context.Context, keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Touch(ctx, keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Touch(ctx, keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

// MGetWithGD is like MGet but returns all the values that it managed to get
func (p *Pool) MGetWithGD(ctx context.Context, keys ...string) ([]interface{}, map[string]error) {
	keyErrors := make(map[string]error, 0)

	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getSlaveConn()
		vals, err := conn.MGet(ctx, keys...).Result()
		if err != nil {
			for _, key := range keys {
				keyErrors[key] = err
			}
			return nil, keyErrors
		}
		return vals, nil
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, err := factory.getSlaveConn(keyList[0])
		if err != nil {
			args := make([]interface{}, 1+len(keyList))
			args[0] = "mget"
			for i, key := range keyList {
				args[1+i] = key
			}
			cmd := redis.NewSliceCmd(ctx, args...)
			cmd.SetErr(err)
			return cmd
		}
		return conn.MGet(ctx, keyList...)
	}

	factory := p.connFactory.(*ShardConnFactory)
	results := factory.doMultiKeys(fn, keys...)
	keyVals := make(map[string]interface{}, 0)
	for _, result := range results {
		args := result.Args()
		vals, err := result.(*redis.SliceCmd).Result()
		if err != nil {
			for i, arg := range args {
				if i == 0 {
					continue
				}
				keyErrors[arg.(string)] = err
			}
			continue
		}
		for i, val := range vals {
			keyVals[args[i+1].(string)] = val
		}
	}
	vals := make([]interface{}, len(keys))
	for i, key := range keys {
		vals[i] = nil
		if val, ok := keyVals[key]; ok {
			vals[i] = val
		}
	}
	return vals, keyErrors
}

func (p *Pool) MGet(ctx context.Context, keys ...string) ([]interface{}, error) {
	vals, keyErrors := p.MGetWithGD(ctx, keys...)
	if len(keyErrors) != 0 {
		for _, err := range keyErrors {
			return nil, err
		}
	}
	return vals, nil
}

func appendArgs(dst, src []interface{}) []interface{} {
	if len(src) == 1 {
		switch v := src[0].(type) {
		case []string:
			for _, s := range v {
				dst = append(dst, s)
			}
			return dst
		case map[string]interface{}:
			for k, v := range v {
				dst = append(dst, k, v)
			}
			return dst
		}
	}

	dst = append(dst, src...)
	return dst
}

// MSetWithGD is like MSet but gives the result for each group of keys
func (p *Pool) MSetWithGD(ctx context.Context, values ...interface{}) []*redis.StatusCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return []*redis.StatusCmd{conn.MSet(ctx, values...)}
	}

	args := make([]interface{}, 0, len(values))
	args = appendArgs(args, values)
	if len(args) == 0 || len(args)%2 != 0 {
		return []*redis.StatusCmd{newErrorStatusCmd(errWrongArgumentsNumber)}
	}
	factory := p.connFactory.(*ShardConnFactory)
	index2Values := make(map[uint32][]interface{})
	for i := 0; i < len(args); i += 2 {
		ind := factory.getShardIndex(fmt.Sprint(args[i]))
		if _, ok := index2Values[ind]; !ok {
			index2Values[ind] = make([]interface{}, 0)
		}
		index2Values[ind] = append(index2Values[ind], args[i], args[i+1])
	}

	var wg sync.WaitGroup
	result := make([]*redis.StatusCmd, len(index2Values))
	var i int
	for ind, vals := range index2Values {
		wg.Add(1)
		conn, _ := factory.shards[ind].getMasterConn()
		go func(i int, conn *redis.Client, vals ...interface{}) {
			status := conn.MSet(ctx, vals...)
			result[i] = status
			wg.Done()
		}(i, conn, vals...)
		i++
	}
	wg.Wait()
	return result
}

// MSet is like Set but accepts multiple values:
//   - MSet("key1", "value1", "key2", "value2")
//   - MSet([]string{"key1", "value1", "key2", "value2"})
//   - MSet(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (p *Pool) MSet(ctx context.Context, values ...interface{}) *redis.StatusCmd {
	var result *redis.StatusCmd
	statuses := p.MSetWithGD(ctx, values...)
	for _, status := range statuses {
		if result == nil || status.Err() != nil {
			result = status
		}
	}
	return result
}

// MSetNX is like SetNX but accepts multiple values:
//   - MSetNX("key1", "value1", "key2", "value2")
//   - MSetNX([]string{"key1", "value1", "key2", "value2"})
//   - MSetNX(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (p *Pool) MSetNX(ctx context.Context, values ...interface{}) *redis.BoolCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.MSetNX(ctx, values...)
	}

	args := make([]interface{}, 0, len(values))
	args = appendArgs(args, values)
	if len(args) == 0 || len(args)%2 != 0 {
		return newErrorBoolCmd(errWrongArgumentsNumber)
	}

	factory := p.connFactory.(*ShardConnFactory)
	keys := make([]string, len(args)/2)
	for i := 0; i < len(args); i += 2 {
		keys[i/2] = fmt.Sprint(args[i])
	}
	if factory.isCrossMultiShards(keys...) {
		// we can't guarantee the atomic when msetnx across multi shards
		return newErrorBoolCmd(errCrossMultiShards)
	}
	conn, _ := factory.getMasterConn(keys[0])
	return conn.MSetNX(ctx, values...)
}

func (p *Pool) Dump(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.Dump(ctx, key)
}

func (p *Pool) Exists(ctx context.Context, keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Exists(ctx, keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, err := factory.getSlaveConn(keyList[0])
		if err != nil {
			return newErrorCmd(err)
		}
		return conn.Exists(ctx, keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Expire(ctx context.Context, key string, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.Expire(ctx, key, expiration)
}

// MExpire gives the result for each group of keys
func (p *Pool) MExpire(ctx context.Context, expiration time.Duration, keys ...string) map[string]error {
	keyErrorsMap := func(results []redis.Cmder) map[string]error {
		if len(results) == 0 {
			return nil
		}
		keyErrors := make(map[string]error, 0)
		for _, result := range results {
			if result.Err() != nil {
				args := result.Args()
				for i, arg := range args {
					if i == 0 || i == 2 {
						continue
					}
					keyErrors[arg.(string)] = result.Err()
				}
			}
		}
		return keyErrors
	}

	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		pipe := conn.Pipeline()
		for _, key := range keys {
			pipe.Expire(ctx, key, expiration)
		}
		results, err := pipe.Exec(ctx)
		if err != nil {
			for _, res := range results {
				res.SetErr(err)
			}
			return keyErrorsMap(results)
		}
		return nil
	}

	factory := p.connFactory.(*ShardConnFactory)
	index2Keys := make(map[uint32][]string)
	for _, key := range keys {
		ind := factory.getShardIndex(key)
		if _, ok := index2Keys[ind]; !ok {
			index2Keys[ind] = make([]string, 0)
		}
		index2Keys[ind] = append(index2Keys[ind], key)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []redis.Cmder
	for ind, keys := range index2Keys {
		wg.Add(1)
		conn, _ := factory.shards[ind].getMasterConn()
		go func(conn *redis.Client, keys ...string) {
			pipe := conn.Pipeline()
			for _, key := range keys {
				pipe.Expire(ctx, key, expiration)
			}
			result, err := pipe.Exec(ctx)
			if err != nil {
				for _, res := range result {
					res.SetErr(err)
				}
				mu.Lock()
				results = append(results, result...)
				mu.Unlock()
			}
			wg.Done()
		}(conn, keys...)
	}
	wg.Wait()

	return keyErrorsMap(results)
}

func (p *Pool) ExpireAt(ctx context.Context, key string, tm time.Time) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.ExpireAt(ctx, key, tm)
}

// MExpireAt gives the result for each group of keys
func (p *Pool) MExpireAt(ctx context.Context, tm time.Time, keys ...string) map[string]error {
	keyErrorsMap := func(results []redis.Cmder) map[string]error {
		if len(results) == 0 {
			return nil
		}
		keyErrors := make(map[string]error, 0)
		for _, result := range results {
			if result.Err() != nil {
				args := result.Args()
				for i, arg := range args {
					if i == 0 || i == 2 {
						continue
					}
					keyErrors[arg.(string)] = result.Err()
				}
			}
		}
		return keyErrors
	}

	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		pipe := conn.Pipeline()
		for _, key := range keys {
			pipe.ExpireAt(ctx, key, tm)
		}
		results, err := pipe.Exec(ctx)
		if err != nil {
			for _, res := range results {
				res.SetErr(err)
			}
			return keyErrorsMap(results)
		}
		return nil
	}

	factory := p.connFactory.(*ShardConnFactory)
	index2Keys := make(map[uint32][]string)
	for _, key := range keys {
		ind := factory.getShardIndex(key)
		if _, ok := index2Keys[ind]; !ok {
			index2Keys[ind] = make([]string, 0)
		}
		index2Keys[ind] = append(index2Keys[ind], key)
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var results []redis.Cmder
	for ind, keys := range index2Keys {
		wg.Add(1)
		conn, _ := factory.shards[ind].getMasterConn()
		go func(conn *redis.Client, keys ...string) {
			pipe := conn.Pipeline()
			for _, key := range keys {
				pipe.ExpireAt(ctx, key, tm)
			}
			result, err := pipe.Exec(ctx)
			if err != nil {
				for _, res := range result {
					res.SetErr(err)
				}
				mu.Lock()
				results = append(results, result...)
				mu.Unlock()
			}
			wg.Done()
		}(conn, keys...)
	}
	wg.Wait()

	return keyErrorsMap(results)
}

func (p *Pool) TTL(ctx context.Context, key string) *redis.DurationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorDurationCmd(err)
	}
	return conn.TTL(ctx, key)
}

func (p *Pool) ObjectRefCount(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ObjectRefCount(ctx, key)
}

func (p *Pool) ObjectEncoding(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.ObjectEncoding(ctx, key)
}

func (p *Pool) ObjectIdleTime(ctx context.Context, key string) *redis.DurationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorDurationCmd(err)
	}
	return conn.ObjectIdleTime(ctx, key)
}

func (p *Pool) Rename(ctx context.Context, key, newkey string) *redis.StatusCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		if factory.isCrossMultiShards(key, newkey) {
			return newErrorStatusCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Rename(ctx, key, newkey)
}

func (p *Pool) RenameNX(ctx context.Context, key, newkey string) *redis.BoolCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		ind := factory.cfg.HashFn([]byte(key)) % uint32(len(factory.shards))
		newInd := factory.cfg.HashFn([]byte(newkey)) % uint32(len(factory.shards))
		if ind != newInd {
			return newErrorBoolCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.RenameNX(ctx, key, newkey)
}

func (p *Pool) Sort(ctx context.Context, key string, sort *redis.Sort) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.Sort(ctx, key, sort)
}

func (p *Pool) SortStore(ctx context.Context, key, store string, sort *redis.Sort) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SortStore(ctx, key, store, sort)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(key, store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SortStore(ctx, key, store, sort)
}

func (p *Pool) SortInterfaces(ctx context.Context, key string, sort *redis.Sort) *redis.SliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorSliceCmd(err)
	}
	return conn.SortInterfaces(ctx, key, sort)
}

func (p *Pool) Eval(ctx context.Context, script string, keys []string, args ...interface{}) *redis.Cmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Eval(ctx, script, keys, args...)
}

func (p *Pool) EvalSha(ctx context.Context, sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.EvalSha(ctx, sha1, keys, args...)
}

func (p *Pool) ScriptExists(ctx context.Context, hashes ...string) *redis.BoolSliceCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorBoolSliceCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptExists(ctx, hashes...)
}

func (p *Pool) ScriptFlush(ctx context.Context) *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptFlush(ctx)
}

func (p *Pool) ScriptKill(ctx context.Context) *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptKill(ctx)
}

func (p *Pool) ScriptLoad(ctx context.Context, script string) *redis.StringCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptLoad(ctx, script)
}

func (p *Pool) DebugObject(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.DebugObject(ctx, key)
}

func (p *Pool) MemoryUsage(ctx context.Context, key string, samples ...int) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.MemoryUsage(ctx, key, samples...)
}

func (p *Pool) Publish(ctx context.Context, channel string, message interface{}) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Publish(ctx, channel, message)
}

func (p *Pool) PubSubChannels(ctx context.Context, pattern string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringSliceCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.PubSubChannels(ctx, pattern)
}

func (p *Pool) PubSubNumSub(ctx context.Context, channels ...string) *redis.MapStringIntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorMapStringIntCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorMapStringIntCmd(err)
	}
	return conn.PubSubNumSub(ctx, channels...)
}

func (p *Pool) PubSubNumPat(ctx context.Context) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.PubSubNumPat(ctx)
}

func (p *Pool) Type(ctx context.Context, key string) *redis.StatusCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Type(ctx, key)
}

func (p *Pool) Scan(ctx context.Context, cursor uint64, match string, count int64) *redis.ScanCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorScanCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getMasterConn()
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.Scan(ctx, cursor, match, count)
}

func (p *Pool) SScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.SScan(ctx, key, cursor, match, count)
}

func (p *Pool) HScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.HScan(ctx, key, cursor, match, count)
}

func (p *Pool) ZScan(ctx context.Context, key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.ZScan(ctx, key, cursor, match, count)
}

func (p *Pool) Append(ctx context.Context, key, value string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.Append(ctx, key, value)
}

func (p *Pool) GetRange(ctx context.Context, key string, start, end int64) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.GetRange(ctx, key, start, end)
}

func (p *Pool) GetSet(ctx context.Context, key string, value interface{}) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.GetSet(ctx, key, value)
}

func (p *Pool) BitCount(ctx context.Context, key string, bitCount *redis.BitCount) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitCount(ctx, key, bitCount)
}

func (p *Pool) BitPos(ctx context.Context, key string, bit int64, pos ...int64) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitPos(ctx, key, bit, pos...)
}

func (p *Pool) BitField(ctx context.Context, key string, args ...interface{}) *redis.IntSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntSliceCmd(err)
	}
	return conn.BitField(ctx, key, args...)
}

func (p *Pool) GetBit(ctx context.Context, key string, offset int64) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.GetBit(ctx, key, offset)
}

func (p *Pool) SetBit(ctx context.Context, key string, offset int64, value int) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SetBit(ctx, key, offset, value)
}

func (p *Pool) BitOp(ctx context.Context, op int, destKey string, keys ...string) *redis.IntCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		allKeys := append(keys, destKey)
		if factory.isCrossMultiShards(allKeys...) {
			return newErrorIntCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(destKey)
	if err != nil {
		return newErrorIntCmd(err)
	}
	switch op {
	case bitOpAnd:
		return conn.BitOpAnd(ctx, destKey, keys...)
	case bitOpOr:
		return conn.BitOpOr(ctx, destKey, keys...)
	case bitOpXor:
		return conn.BitOpXor(ctx, destKey, keys...)
	default:
		return newErrorIntCmd(errors.New("unknown op type"))
	}
}

func (p *Pool) BitOpAnd(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(ctx, bitOpAnd, destKey, keys...)
}

func (p *Pool) BitOpOr(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(ctx, bitOpOr, destKey, keys...)
}

func (p *Pool) BitOpXor(ctx context.Context, destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(ctx, bitOpXor, destKey, keys...)
}

func (p *Pool) BitOpNot(ctx context.Context, destKey string, key string) *redis.IntCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		if factory.isCrossMultiShards(destKey, key) {
			return newErrorIntCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(destKey)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitOpNot(ctx, destKey, key)
}

func (p *Pool) Decr(ctx context.Context, key string) *redis.IntCmd {
	return p.DecrBy(ctx, key, 1)
}

func (p *Pool) Incr(ctx context.Context, key string) *redis.IntCmd {
	return p.DecrBy(ctx, key, -1)
}

func (p *Pool) IncrBy(ctx context.Context, key string, increment int64) *redis.IntCmd {
	return p.DecrBy(ctx, key, -1*increment)
}

func (p *Pool) DecrBy(ctx context.Context, key string, decrement int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.DecrBy(ctx, key, decrement)
}

func (p *Pool) IncrByFloat(ctx context.Context, key string, value float64) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.IncrByFloat(ctx, key, value)
}

func (p *Pool) HSet(ctx context.Context, key, field string, value interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HSet(ctx, key, field, value)
}

func (p *Pool) HDel(ctx context.Context, key string, fields ...string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HDel(ctx, key, fields...)
}

func (p *Pool) HExists(ctx context.Context, key, field string) *redis.BoolCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HExists(ctx, key, field)
}

func (p *Pool) HGet(ctx context.Context, key, field string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.HGet(ctx, key, field)
}

func (p *Pool) HGetAll(ctx context.Context, key string) *redis.MapStringStringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringStringMapCmd(err)
	}
	return conn.HGetAll(ctx, key)
}

func (p *Pool) HIncrBy(ctx context.Context, key, field string, incr int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HIncrBy(ctx, key, field, incr)
}

func (p *Pool) HIncrByFloat(ctx context.Context, key, field string, incr float64) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.HIncrByFloat(ctx, key, field, incr)
}

func (p *Pool) HKeys(ctx context.Context, key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.HKeys(ctx, key)
}

func (p *Pool) HLen(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HLen(ctx, key)
}

func (p *Pool) HMGet(ctx context.Context, key string, fields ...string) *redis.SliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorSliceCmd(err)
	}
	return conn.HMGet(ctx, key, fields...)
}

func (p *Pool) HMSet(ctx context.Context, key string, values ...interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HMSet(ctx, key, values...)
}

func (p *Pool) HSetNX(ctx context.Context, key, field string, value interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HSetNX(ctx, key, field, value)
}

func (p *Pool) HVals(ctx context.Context, key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.HVals(ctx, key)
}

func (p *Pool) BLPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BLPop(ctx, timeout, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.BLPop(ctx, timeout, keys...)
}

func (p *Pool) BRPop(ctx context.Context, timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BRPop(ctx, timeout, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.BRPop(ctx, timeout, keys...)
}

func (p *Pool) BRPopLPush(ctx context.Context, source, destination string, timeout time.Duration) *redis.StringCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BRPopLPush(ctx, source, destination, timeout)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorStringCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.BRPopLPush(ctx, source, destination, timeout)
}

func (p *Pool) LIndex(ctx context.Context, key string, index int64) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.LIndex(ctx, key, index)
}

func (p *Pool) LInsert(ctx context.Context, key, op string, pivot, value interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LInsert(ctx, key, op, pivot, value)
}

func (p *Pool) LInsertBefore(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return p.LInsert(ctx, key, "BEFORE", pivot, value)
}

func (p *Pool) LInsertAfter(ctx context.Context, key string, pivot, value interface{}) *redis.IntCmd {
	return p.LInsert(ctx, key, "AFTER", pivot, value)
}

func (p *Pool) LLen(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LLen(ctx, key)
}

func (p *Pool) LPop(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.LPop(ctx, key)
}

func (p *Pool) LPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LPush(ctx, key, values...)
}

func (p *Pool) LPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LPushX(ctx, key, values...)
}

func (p *Pool) LRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.LRange(ctx, key, start, stop)
}

func (p *Pool) LRem(ctx context.Context, key string, count int64, value interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LRem(ctx, key, count, value)
}

func (p *Pool) LSet(ctx context.Context, key string, index int64, value interface{}) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.LSet(ctx, key, index, value)
}

func (p *Pool) LTrim(ctx context.Context, key string, start, stop int64) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.LTrim(ctx, key, start, stop)
}

func (p *Pool) RPop(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.RPop(ctx, key)
}

func (p *Pool) RPopLPush(ctx context.Context, source, destination string) *redis.StringCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.RPopLPush(ctx, source, destination)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorStringCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.RPopLPush(ctx, source, destination)
}

func (p *Pool) RPush(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.RPush(ctx, key, values...)
}

func (p *Pool) RPushX(ctx context.Context, key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.RPushX(ctx, key, values...)
}

func (p *Pool) SAdd(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SAdd(ctx, key, members...)
}

func (p *Pool) SCard(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SCard(ctx, key)
}

func (p *Pool) SDiff(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SDiff(ctx, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SDiff(ctx, keys...)
}

func (p *Pool) SDiffStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SDiffStore(ctx, destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)

	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SDiffStore(ctx, destination, keys...)
}

func (p *Pool) SInter(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SInter(ctx, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SInter(ctx, keys...)
}

func (p *Pool) SInterStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SInterStore(ctx, destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SInterStore(ctx, destination, keys...)
}

func (p *Pool) SIsMember(ctx context.Context, key string, member interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SIsMember(ctx, key, member)
}

func (p *Pool) SMembers(ctx context.Context, key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SMembers(ctx, key)
}

func (p *Pool) SMembersMap(ctx context.Context, key string) *redis.StringStructMapCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringStructMapCmd(err)
	}
	return conn.SMembersMap(ctx, key)
}

func (p *Pool) SMove(ctx context.Context, source, destination string, member interface{}) *redis.BoolCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SMove(ctx, source, destination, member)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorBoolCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.SMove(ctx, source, destination, member)
}

func (p *Pool) SPop(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.SPop(ctx, key)
}

func (p *Pool) SPopN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SPopN(ctx, key, count)
}

func (p *Pool) SRandMember(ctx context.Context, key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.SRandMember(ctx, key)
}

func (p *Pool) SRandMemberN(ctx context.Context, key string, count int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SRandMemberN(ctx, key, count)
}

func (p *Pool) SRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SRem(ctx, key, members...)
}

func (p *Pool) SUnion(ctx context.Context, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SUnion(ctx, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SUnion(ctx, keys...)
}

func (p *Pool) SUnionStore(ctx context.Context, destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SUnionStore(ctx, destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SUnionStore(ctx, destination, keys...)
}

func (p *Pool) ZAdd(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAdd(ctx, key, members...)
}

func (p *Pool) ZAddNX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddNX(ctx, key, members...)
}

func (p *Pool) ZAddXX(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddXX(ctx, key, members...)
}

func (p *Pool) ZAddGT(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddGT(ctx, key, members...)
}

func (p *Pool) ZAddLT(ctx context.Context, key string, members ...redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddLT(ctx, key, members...)
}

func (p *Pool) ZAddCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	membersArg := make([]redis.Z, len(members))
	for i, m := range members {
		membersArg[i] = *m
	}
	return conn.ZAddArgs(ctx, key, redis.ZAddArgs{
		Ch:      true,
		Members: membersArg,
	})
}

func (p *Pool) ZAddNXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	membersArg := make([]redis.Z, len(members))
	for i, m := range members {
		membersArg[i] = *m
	}
	return conn.ZAddArgs(ctx, key, redis.ZAddArgs{
		NX:      true,
		Ch:      true,
		Members: membersArg,
	})
}

func (p *Pool) ZAddXXCh(ctx context.Context, key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	membersArg := make([]redis.Z, len(members))
	for i, m := range members {
		membersArg[i] = *m
	}
	return conn.ZAddArgs(ctx, key, redis.ZAddArgs{
		XX:      true,
		Ch:      true,
		Members: membersArg,
	})
}

func (p *Pool) ZIncr(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZAddArgsIncr(ctx, key, redis.ZAddArgs{
		Members: []redis.Z{*member},
	})
}

func (p *Pool) ZIncrNX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZAddArgsIncr(ctx, key, redis.ZAddArgs{
		NX:      true,
		Members: []redis.Z{*member},
	})
}

func (p *Pool) ZIncrXX(ctx context.Context, key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZAddArgsIncr(ctx, key, redis.ZAddArgs{
		XX:      true,
		Members: []redis.Z{*member},
	})
}

func (p *Pool) ZCard(ctx context.Context, key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZCard(ctx, key)
}

func (p *Pool) ZCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZCount(ctx, key, min, max)
}

func (p *Pool) ZLexCount(ctx context.Context, key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZLexCount(ctx, key, min, max)
}

func (p *Pool) ZIncrBy(ctx context.Context, key string, increment float64, member string) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZIncrBy(ctx, key, increment, member)
}

func (p *Pool) ZPopMax(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZPopMax(ctx, key, count...)
}

func (p *Pool) ZPopMin(ctx context.Context, key string, count ...int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZPopMin(ctx, key, count...)
}

func (p *Pool) ZRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRange(ctx, key, start, stop)
}

func (p *Pool) ZRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRangeWithScores(ctx, key, start, stop)
}

func (p *Pool) ZRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRangeByScore(ctx, key, opt)
}

func (p *Pool) ZRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRangeByLex(ctx, key, opt)
}

func (p *Pool) ZRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRangeByScoreWithScores(ctx, key, opt)
}

func (p *Pool) ZRank(ctx context.Context, key, member string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRank(ctx, key, member)
}

func (p *Pool) ZRem(ctx context.Context, key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRem(ctx, key, members...)
}

func (p *Pool) ZRemRangeByRank(ctx context.Context, key string, start, stop int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByRank(ctx, key, start, stop)
}

func (p *Pool) ZRemRangeByScore(ctx context.Context, key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByScore(ctx, key, min, max)
}

func (p *Pool) ZRemRangeByLex(ctx context.Context, key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByLex(ctx, key, min, max)
}

func (p *Pool) ZRevRange(ctx context.Context, key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRange(ctx, key, start, stop)
}

func (p *Pool) ZRevRangeWithScores(ctx context.Context, key string, start, stop int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRevRangeWithScores(ctx, key, start, stop)
}

func (p *Pool) ZRevRangeByScore(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRangeByScore(ctx, key, opt)
}

func (p *Pool) ZRevRangeByLex(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRangeByLex(ctx, key, opt)
}

func (p *Pool) ZRevRangeByScoreWithScores(ctx context.Context, key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRevRangeByScoreWithScores(ctx, key, opt)
}

func (p *Pool) ZRevRank(ctx context.Context, key, member string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRevRank(ctx, key, member)
}

func (p *Pool) ZScore(ctx context.Context, key, member string) *redis.FloatCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZScore(ctx, key, member)
}

func (p *Pool) ZUnionStore(ctx context.Context, dest string, store *redis.ZStore) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.ZUnionStore(ctx, dest, store)
	}
	factory := p.connFactory.(*ShardConnFactory)
	keys := append(store.Keys, dest)
	if factory.isCrossMultiShards(keys...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.ZUnionStore(ctx, dest, store)
}

func (p *Pool) ZInterStore(ctx context.Context, destination string, store *redis.ZStore) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.ZInterStore(ctx, destination, store)
	}
	factory := p.connFactory.(*ShardConnFactory)
	keys := append(store.Keys, destination)
	if factory.isCrossMultiShards(keys...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.ZInterStore(ctx, destination, store)
}

func (p *Pool) GeoAdd(ctx context.Context, key string, geoLocation ...*redis.GeoLocation) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.GeoAdd(ctx, key, geoLocation...)
}

func (p *Pool) GeoPos(ctx context.Context, key string, members ...string) *redis.GeoPosCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoCmd(err)
	}
	return conn.GeoPos(ctx, key, members...)
}

func (p *Pool) GeoRadius(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoLocationCmd(err)
	}
	return conn.GeoRadius(ctx, key, longitude, latitude, query)
}

func (p *Pool) GeoRadiusStore(ctx context.Context, key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.GeoRadiusStore(ctx, key, longitude, latitude, query)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if query.Store != "" && factory.isCrossMultiShards(key, query.Store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	if query.StoreDist != "" && factory.isCrossMultiShards(key, query.StoreDist) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(key)
	return conn.GeoRadiusStore(ctx, key, longitude, latitude, query)
}

func (p *Pool) GeoRadiusByMember(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoLocationCmd(err)
	}
	return conn.GeoRadiusByMember(ctx, key, member, query)
}

func (p *Pool) GeoRadiusByMemberStore(ctx context.Context, key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.GeoRadiusByMemberStore(ctx, key, member, query)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if query.Store != "" && factory.isCrossMultiShards(key, query.Store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	if query.StoreDist != "" && factory.isCrossMultiShards(key, query.StoreDist) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(key)
	return conn.GeoRadiusByMemberStore(ctx, key, member, query)
}

func (p *Pool) GeoDist(ctx context.Context, key string, member1, member2, unit string) *redis.FloatCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.GeoDist(ctx, key, member1, member2, unit)
}

func (p *Pool) GeoHash(ctx context.Context, key string, members ...string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.GeoHash(ctx, key, members...)
}

func (p *Pool) PFAdd(ctx context.Context, key string, els ...interface{}) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.PFAdd(ctx, key, els...)
}

func (p *Pool) PFCount(ctx context.Context, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.PFCount(ctx, keys...)
}

func (p *Pool) PFMerge(ctx context.Context, dest string, keys ...string) *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.PFMerge(ctx, dest, keys...)
}

func (p *Pool) FlushDB(ctx context.Context) *redis.StatusCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.FlushDB(ctx)
	}
	var result *redis.StatusCmd
	factory := p.connFactory.(*ShardConnFactory)
	for _, shard := range factory.shards {
		conn, _ := shard.getMasterConn()
		result = conn.FlushDB(ctx)
		if result.Err() != nil {
			return result
		}
	}
	return result
}

func (p *Pool) FlushDBAsync(ctx context.Context) *redis.StatusCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.FlushDBAsync(ctx)
	}
	var result *redis.StatusCmd
	factory := p.connFactory.(*ShardConnFactory)
	for _, shard := range factory.shards {
		conn, _ := shard.getMasterConn()
		result = conn.FlushDBAsync(ctx)
		if result.Err() != nil {
			return result
		}
	}
	return result
}

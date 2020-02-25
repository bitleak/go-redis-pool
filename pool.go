package pool

import (
	"errors"
	"fmt"
	"sync"
	"time"

	redis "github.com/go-redis/redis/v7"
)

const (
	statusCmdType = iota + 1
	stringCmdType
	intCmdType
	floatCmdType
	boolCmdType
)

const (
	bitOpAnd = iota + 1
	bitOpOr
	bitOpXor
)

var (
	errWrongArguments       = errors.New("wrong number of arguments")
	errShardPoolUnSupported = errors.New("shard pool didn't support the command")
	errCrossMultiShards     = errors.New("cross multi shards was not allowed")
)

type ConnFactory interface {
	getSlaveConn(key ...string) (*redis.Client, error)
	getMasterConn(key ...string) (*redis.Client, error)
	close()
}

func newErrorStringIntMapCmd(err error) *redis.StringIntMapCmd {
	cmd := &redis.StringIntMapCmd{}
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

func newErrorStringStringMapCmd(err error) *redis.StringStringMapCmd {
	cmd := &redis.StringStringMapCmd{}
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

func (p *Pool) Close() {
	p.connFactory.close()
}

func (p *Pool) WithMaster(key ...string) (*redis.Client, error) {
	return p.connFactory.getMasterConn(key...)
}

func (p *Pool) Pipeline() (redis.Pipeliner, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Pipeline(), nil
}

func (p *Pool) Pipelined(fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Pipelined(fn)
}

func (p *Pool) TxPipeline() (redis.Pipeliner, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.TxPipeline(), nil
}

func (p *Pool) TxPipelined(fn func(redis.Pipeliner) error) ([]redis.Cmder, error) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return nil, errShardPoolUnSupported
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.TxPipelined(fn)
}

func (p *Pool) Ping() *redis.StatusCmd {
	// FIXME: use config to determine whether no key would access the master
	conn, err := p.connFactory.getMasterConn()
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Ping()
}

func (p *Pool) Get(key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.Get(key)
}

func (p *Pool) Set(key string, value interface{}, expiration time.Duration) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Set(key, value, expiration)
}

func (p *Pool) SetNX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SetNX(key, value, expiration)
}

func (p *Pool) SetXX(key string, value interface{}, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SetXX(key, value, expiration)
}

func (p *Pool) SetRange(key string, offset int64, value string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SetRange(key, offset, value)
}

func (p *Pool) StrLen(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.StrLen(key)
}

func (p *Pool) Echo(message interface{}) *redis.StringCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Echo(message)
}

func (p *Pool) Del(keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Del(keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Del(keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Unlink(keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Unlink(keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Unlink(keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Touch(keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Touch(keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, _ := factory.getMasterConn(keyList[0])
		return conn.Touch(keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) MGet(keys ...string) ([]interface{}, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.MGet(keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, err := factory.getSlaveConn(keyList[0])
		if err != nil {
			return newErrorCmd(err)
		}
		return conn.MGet(keyList...)
	}

	factory := p.connFactory.(*ShardConnFactory)
	results := factory.doMultiKeys(fn, keys...)
	keyVals := make(map[string]interface{}, 0)
	for _, result := range results {
		vals, err := result.(*redis.SliceCmd).Result()
		if err != nil {
			return nil, err
		}
		for i, val := range vals {
			args := result.Args()
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

// MSet is like Set but accepts multiple values:
//   - MSet("key1", "value1", "key2", "value2")
//   - MSet([]string{"key1", "value1", "key2", "value2"})
//   - MSet(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (p *Pool) MSet(values ...interface{}) *redis.StatusCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.MSet(values...)
	}

	args := make([]interface{}, 0, len(values))
	args = appendArgs(args, values)
	if len(args) == 0 || len(args)%2 != 0 {
		return newErrorStatusCmd(errWrongArguments)
	}
	factory := p.connFactory.(*ShardConnFactory)
	index2Values := make(map[uint32][]interface{})
	for i := 0; i < len(args); i += 2 {
		ind := factory.cfg.HashFn([]byte(fmt.Sprint(args[i]))) % uint32(len(factory.shards))
		if _, ok := index2Values[ind]; !ok {
			index2Values[ind] = make([]interface{}, 0)
		}
		index2Values[ind] = append(index2Values[ind], args[i], args[i+1])
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var result *redis.StatusCmd
	for ind, vals := range index2Values {
		wg.Add(1)
		conn, _ := factory.shards[ind].getMasterConn()
		go func(conn *redis.Client, vals ...interface{}) {
			defer wg.Done()
			status := conn.MSet(vals...)
			mu.Lock()
			if result == nil || status.Err() != nil {
				result = status
			}
			mu.Unlock()
		}(conn, vals...)
	}
	wg.Wait()
	return result
}

// MSetNX is like SetNX but accepts multiple values:
//   - MSetNX("key1", "value1", "key2", "value2")
//   - MSetNX([]string{"key1", "value1", "key2", "value2"})
//   - MSetNX(map[string]interface{}{"key1": "value1", "key2": "value2"})
func (p *Pool) MSetNX(values ...interface{}) *redis.BoolCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.MSetNX(values...)
	}

	args := make([]interface{}, 0, len(values))
	args = appendArgs(args, values)
	if len(args) == 0 || len(args)%2 != 0 {
		return newErrorBoolCmd(errWrongArguments)
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
	return conn.MSetNX(values...)
}

func (p *Pool) Dump(key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.Dump(key)
}

func (p *Pool) Exists(keys ...string) (int64, error) {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.Exists(keys...).Result()
	}

	fn := func(factory *ShardConnFactory, keyList ...string) redis.Cmder {
		conn, err := factory.getSlaveConn(keyList[0])
		if err != nil {
			return newErrorCmd(err)
		}
		return conn.Exists(keyList...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	return factory.doMultiIntCommand(fn, keys...)
}

func (p *Pool) Expire(key string, expiration time.Duration) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.Expire(key, expiration)
}

func (p *Pool) ExpireAt(key string, tm time.Time) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.ExpireAt(key, tm)
}

func (p *Pool) TTL(key string) *redis.DurationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorDurationCmd(err)
	}
	return conn.TTL(key)
}

func (p *Pool) ObjectRefCount(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ObjectRefCount(key)
}

func (p *Pool) ObjectEncoding(key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.ObjectEncoding(key)
}

func (p *Pool) ObjectIdleTime(key string) *redis.DurationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorDurationCmd(err)
	}
	return conn.ObjectIdleTime(key)
}

func (p *Pool) Rename(key, newkey string) *redis.StatusCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		if factory.isCrossMultiShards(key, newkey) {
			return newErrorStatusCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Rename(key, newkey)
}

func (p *Pool) RenameNX(key, newkey string) *redis.BoolCmd {
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
	return conn.RenameNX(key, newkey)
}

func (p *Pool) Sort(key string, sort *redis.Sort) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.Sort(key, sort)
}

func (p *Pool) SortStore(key, store string, sort *redis.Sort) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SortStore(key, store, sort)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(key, store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SortStore(key, store, sort)
}

func (p *Pool) SortInterfaces(key string, sort *redis.Sort) *redis.SliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorSliceCmd(err)
	}
	return conn.SortInterfaces(key, sort)
}

func (p *Pool) Eval(script string, keys []string, args ...interface{}) *redis.Cmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Eval(script, keys, args...)
}

func (p *Pool) EvalSha(sha1 string, keys []string, args ...interface{}) *redis.Cmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.EvalSha(sha1, keys, args...)
}

func (p *Pool) ScriptExists(hashes ...string) *redis.BoolSliceCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorBoolSliceCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptExists(hashes...)
}

func (p *Pool) ScriptFlush() *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptFlush()
}

func (p *Pool) ScriptKill() *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptKill()
}

func (p *Pool) ScriptLoad(script string) *redis.StringCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.ScriptLoad(script)
}

func (p *Pool) DebugObject(key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.DebugObject(key)
}

func (p *Pool) MemoryUsage(key string, samples ...int) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.MemoryUsage(key, samples...)
}

func (p *Pool) Publish(channel string, message interface{}) *(redis.IntCmd) {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.Publish(channel, message)
}

func (p *Pool) PubSubChannels(pattern string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringSliceCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.PubSubChannels(pattern)
}

func (p *Pool) PubSubNumSub(channels ...string) *redis.StringIntMapCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStringIntMapCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorStringIntMapCmd(err)
	}
	return conn.PubSubNumSub(channels...)
}

func (p *Pool) PubSubNumPat() *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.PubSubNumPat()
}

func (p *Pool) Type(key string) *redis.StatusCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.Type(key)
}

func (p *Pool) Scan(cursor uint64, match string, count int64) *redis.ScanCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorScanCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getMasterConn()
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.Scan(cursor, match, count)
}

func (p *Pool) SScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.SScan(key, cursor, match, count)
}

func (p *Pool) HScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.HScan(key, cursor, match, count)
}

func (p *Pool) ZScan(key string, cursor uint64, match string, count int64) *redis.ScanCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorScanCmd(err)
	}
	return conn.ZScan(key, cursor, match, count)
}

func (p *Pool) Append(key, value string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.Append(key, value)
}

func (p *Pool) GetRange(key string, start, end int64) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.GetRange(key, start, end)
}

func (p *Pool) GetSet(key string, value interface{}) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.GetSet(key, value)
}

func (p *Pool) BitCount(key string, bitCount *redis.BitCount) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitCount(key, bitCount)
}

func (p *Pool) BitPos(key string, bit int64, pos ...int64) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitPos(key, bit, pos...)
}

func (p *Pool) BitField(key string, args ...interface{}) *redis.IntSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntSliceCmd(err)
	}
	return conn.BitField(key, args...)
}

func (p *Pool) GetBit(key string, offset int64) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.GetBit(key, offset)
}

func (p *Pool) SetBit(key string, offset int64, value int) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SetBit(key, offset, value)
}

func (p *Pool) BitOp(op int, destKey string, keys ...string) *redis.IntCmd {
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
		return conn.BitOpAnd(destKey, keys...)
	case bitOpOr:
		return conn.BitOpOr(destKey, keys...)
	case bitOpXor:
		return conn.BitOpXor(destKey, keys...)
	default:
		return newErrorIntCmd(errors.New("unknown op type"))
	}
}

func (p *Pool) BitOpAnd(destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(bitOpAnd, destKey, keys...)
}

func (p *Pool) BitOpOr(destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(bitOpOr, destKey, keys...)
}

func (p *Pool) BitOpXor(destKey string, keys ...string) *redis.IntCmd {
	return p.BitOp(bitOpXor, destKey, keys...)
}

func (p *Pool) BitOpNot(destKey string, key string) *redis.IntCmd {
	if factory, ok := p.connFactory.(*ShardConnFactory); ok {
		if factory.isCrossMultiShards(destKey, key) {
			return newErrorIntCmd(errCrossMultiShards)
		}
	}
	conn, err := p.connFactory.getMasterConn(destKey)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.BitOpNot(destKey, key)
}

func (p *Pool) Decr(key string) *redis.IntCmd {
	return p.DecrBy(key, 1)
}

func (p *Pool) Incr(key string) *redis.IntCmd {
	return p.DecrBy(key, -1)
}

func (p *Pool) IncrBy(key string, increment int64) *redis.IntCmd {
	return p.DecrBy(key, -1*increment)
}

func (p *Pool) DecrBy(key string, decrement int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.DecrBy(key, decrement)
}

func (p *Pool) IncrByFloat(key string, value float64) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.IncrByFloat(key, value)
}

func (p *Pool) HSet(key, field string, value interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HSet(key, field, value)
}

func (p *Pool) HDel(key string, fields ...string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HDel(key, fields...)
}

func (p *Pool) HExists(key, field string) *redis.BoolCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HExists(key, field)
}

func (p *Pool) HGet(key, field string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.HGet(key, field)
}

func (p *Pool) HGetAll(key string) *redis.StringStringMapCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringStringMapCmd(err)
	}
	return conn.HGetAll(key)
}

func (p *Pool) HIncrBy(key, field string, incr int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HIncrBy(key, field, incr)
}

func (p *Pool) HIncrByFloat(key, field string, incr float64) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.HIncrByFloat(key, field, incr)
}

func (p *Pool) HKeys(key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.HKeys(key)
}

func (p *Pool) HLen(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HLen(key)
}

func (p *Pool) HMGet(key string, fields ...string) *redis.SliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorSliceCmd(err)
	}
	return conn.HMGet(key, fields...)
}

func (p *Pool) HMSet(key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.HMSet(key, values...)
}

func (p *Pool) HSetNX(key, field string, value interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.HSetNX(key, field, value)
}

func (p *Pool) HVals(key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.HVals(key)
}

func (p *Pool) BLPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BLPop(timeout, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.BLPop(timeout, keys...)
}

func (p *Pool) BRPop(timeout time.Duration, keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BRPop(timeout, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.BRPop(timeout, keys...)
}

func (p *Pool) BRPopLPush(source, destination string, timeout time.Duration) *redis.StringCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.BRPopLPush(source, destination, timeout)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorStringCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.BRPopLPush(source, destination, timeout)
}

func (p *Pool) LIndex(key string, index int64) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.LIndex(key, index)
}

func (p *Pool) LInsert(key, op string, pivot, value interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LInsert(key, op, pivot, value)
}

func (p *Pool) LInsertBefore(key string, pivot, value interface{}) *redis.IntCmd {
	return p.LInsert(key, "BEFORE", pivot, value)
}

func (p *Pool) LInsertAfter(key string, pivot, value interface{}) *redis.IntCmd {
	return p.LInsert(key, "AFTER", pivot, value)
}

func (p *Pool) LLen(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LLen(key)
}

func (p *Pool) LPop(key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.LPop(key)
}

func (p *Pool) LPush(key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LPush(key, values...)
}

func (p *Pool) LPushX(key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LPushX(key, values...)
}

func (p *Pool) LRange(key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.LRange(key, start, stop)
}

func (p *Pool) LRem(key string, count int64, value interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.LRem(key, count, value)
}

func (p *Pool) LSet(key string, index int64, value interface{}) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.LSet(key, index, value)
}

func (p *Pool) LTrim(key string, start, stop int64) *redis.StatusCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStatusCmd(err)
	}
	return conn.LTrim(key, start, stop)
}

func (p *Pool) RPop(key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.RPop(key)
}

func (p *Pool) RPopLPush(source, destination string) *redis.StringCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.RPopLPush(source, destination)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorStringCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.RPopLPush(source, destination)
}

func (p *Pool) RPush(key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.RPush(key, values...)
}

func (p *Pool) RPushX(key string, values ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.RPushX(key, values...)
}

func (p *Pool) SAdd(key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SAdd(key, members...)
}

func (p *Pool) SCard(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SCard(key)
}

func (p *Pool) SDiff(keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SDiff(keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SDiff(keys...)
}

func (p *Pool) SDiffStore(destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SDiffStore(destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)

	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SDiffStore(destination, keys...)
}

func (p *Pool) SInter(keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SInter(keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SInter(keys...)
}

func (p *Pool) SInterStore(destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SInterStore(destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SInterStore(destination, keys...)
}

func (p *Pool) SIsMember(key string, member interface{}) *redis.BoolCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorBoolCmd(err)
	}
	return conn.SIsMember(key, member)
}

func (p *Pool) SMembers(key string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SMembers(key)
}

func (p *Pool) SMembersMap(key string) *redis.StringStructMapCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringStructMapCmd(err)
	}
	return conn.SMembersMap(key)
}

func (p *Pool) SMove(source, destination string, member interface{}) *redis.BoolCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SMove(source, destination, member)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(source, destination) {
		return newErrorBoolCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(source)
	return conn.SMove(source, destination, member)
}

func (p *Pool) SPop(key string) *redis.StringCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.SPop(key)
}

func (p *Pool) SPopN(key string, count int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SPopN(key, count)
}

func (p *Pool) SRandMember(key string) *redis.StringCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringCmd(err)
	}
	return conn.SRandMember(key)
}

func (p *Pool) SRandMemberN(key string, count int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SRandMemberN(key, count)
}

func (p *Pool) SRem(key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.SRem(key, members...)
}

func (p *Pool) SUnion(keys ...string) *redis.StringSliceCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, err := p.connFactory.getSlaveConn()
		if err != nil {
			return newErrorStringSliceCmd(err)
		}
		return conn.SUnion(keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(keys...) {
		return newErrorStringSliceCmd(errCrossMultiShards)
	}
	conn, err := p.connFactory.getSlaveConn(keys[0])
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.SUnion(keys...)
}

func (p *Pool) SUnionStore(destination string, keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.SUnionStore(destination, keys...)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if factory.isCrossMultiShards(append(keys, destination)...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(destination)
	return conn.SUnionStore(destination, keys...)
}

func (p *Pool) ZAdd(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAdd(key, members...)
}

func (p *Pool) ZAddNX(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddNX(key, members...)
}

func (p *Pool) ZAddXX(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddXX(key, members...)
}

func (p *Pool) ZAddCh(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddCh(key, members...)
}

func (p *Pool) ZAddNXCh(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddNXCh(key, members...)
}

func (p *Pool) ZAddXXCh(key string, members ...*redis.Z) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZAddXXCh(key, members...)
}

func (p *Pool) ZIncr(key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZIncr(key, member)
}

func (p *Pool) ZIncrNX(key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZIncrNX(key, member)
}

func (p *Pool) ZIncrXX(key string, member *redis.Z) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZIncrXX(key, member)
}

func (p *Pool) ZCard(key string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZCard(key)
}

func (p *Pool) ZCount(key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZCount(key, min, max)
}

func (p *Pool) ZLexCount(key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZLexCount(key, min, max)
}

func (p *Pool) ZIncrBy(key string, increment float64, member string) *redis.FloatCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZIncrBy(key, increment, member)
}

func (p *Pool) ZPopMax(key string, count ...int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZPopMax(key, count...)
}

func (p *Pool) ZPopMin(key string, count ...int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZPopMin(key, count...)
}

func (p *Pool) ZRange(key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRange(key, start, stop)
}

func (p *Pool) ZRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRangeWithScores(key, start, stop)
}

func (p *Pool) ZRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRangeByScore(key, opt)
}

func (p *Pool) ZRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRangeByLex(key, opt)
}

func (p *Pool) ZRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRangeByScoreWithScores(key, opt)
}

func (p *Pool) ZRank(key, member string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRank(key, member)
}

func (p *Pool) ZRem(key string, members ...interface{}) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRem(key, members...)
}

func (p *Pool) ZRemRangeByRank(key string, start, stop int64) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByRank(key, start, stop)
}

func (p *Pool) ZRemRangeByScore(key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByScore(key, min, max)
}

func (p *Pool) ZRemRangeByLex(key, min, max string) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRemRangeByLex(key, min, max)
}

func (p *Pool) ZRevRange(key string, start, stop int64) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRange(key, start, stop)
}

func (p *Pool) ZRevRangeWithScores(key string, start, stop int64) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRevRangeWithScores(key, start, stop)
}

func (p *Pool) ZRevRangeByScore(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRangeByScore(key, opt)
}

func (p *Pool) ZRevRangeByLex(key string, opt *redis.ZRangeBy) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.ZRevRangeByLex(key, opt)
}

func (p *Pool) ZRevRangeByScoreWithScores(key string, opt *redis.ZRangeBy) *redis.ZSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorZSliceCmd(err)
	}
	return conn.ZRevRangeByScoreWithScores(key, opt)
}

func (p *Pool) ZRevRank(key, member string) *redis.IntCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.ZRevRank(key, member)
}

func (p *Pool) ZScore(key, member string) *redis.FloatCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.ZScore(key, member)
}

func (p *Pool) ZUnionStore(dest string, store *redis.ZStore) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.ZUnionStore(dest, store)
	}
	factory := p.connFactory.(*ShardConnFactory)
	keys := append(store.Keys, dest)
	if factory.isCrossMultiShards(keys...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.ZUnionStore(dest, store)
}

func (p *Pool) ZInterStore(destination string, store *redis.ZStore) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.ZInterStore(destination, store)
	}
	factory := p.connFactory.(*ShardConnFactory)
	keys := append(store.Keys, destination)
	if factory.isCrossMultiShards(keys...) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(keys[0])
	return conn.ZInterStore(destination, store)
}

func (p *Pool) GeoAdd(key string, geoLocation ...*redis.GeoLocation) *redis.IntCmd {
	conn, err := p.connFactory.getMasterConn(key)
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.GeoAdd(key, geoLocation...)
}

func (p *Pool) GeoPos(key string, members ...string) *redis.GeoPosCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoCmd(err)
	}
	return conn.GeoPos(key, members...)
}

func (p *Pool) GeoRadius(key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoLocationCmd(err)
	}
	return conn.GeoRadius(key, longitude, latitude, query)
}

func (p *Pool) GeoRadiusStore(key string, longitude, latitude float64, query *redis.GeoRadiusQuery) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.GeoRadiusStore(key, longitude, latitude, query)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if query.Store != "" && factory.isCrossMultiShards(key, query.Store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	if query.StoreDist != "" && factory.isCrossMultiShards(key, query.StoreDist) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(key)
	return conn.GeoRadiusStore(key, longitude, latitude, query)
}

func (p *Pool) GeoRadiusByMember(key, member string, query *redis.GeoRadiusQuery) *redis.GeoLocationCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorGeoLocationCmd(err)
	}
	return conn.GeoRadiusByMember(key, member, query)
}

func (p *Pool) GeoRadiusByMemberStore(key, member string, query *redis.GeoRadiusQuery) *redis.IntCmd {
	if _, ok := p.connFactory.(*HAConnFactory); ok {
		conn, _ := p.connFactory.getMasterConn()
		return conn.GeoRadiusByMemberStore(key, member, query)
	}
	factory := p.connFactory.(*ShardConnFactory)
	if query.Store != "" && factory.isCrossMultiShards(key, query.Store) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	if query.StoreDist != "" && factory.isCrossMultiShards(key, query.StoreDist) {
		return newErrorIntCmd(errCrossMultiShards)
	}
	conn, _ := p.connFactory.getMasterConn(key)
	return conn.GeoRadiusByMemberStore(key, member, query)
}

func (p *Pool) GeoDist(key string, member1, member2, unit string) *redis.FloatCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorFloatCmd(err)
	}
	return conn.GeoDist(key, member1, member2, unit)
}

func (p *Pool) GeoHash(key string, members ...string) *redis.StringSliceCmd {
	conn, err := p.connFactory.getSlaveConn(key)
	if err != nil {
		return newErrorStringSliceCmd(err)
	}
	return conn.GeoHash(key, members...)
}

func (p *Pool) PFAdd(key string, els ...interface{}) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.PFAdd(key, els...)
}

func (p *Pool) PFCount(keys ...string) *redis.IntCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorIntCmd(errShardPoolUnSupported)
	}
	conn, err := p.connFactory.getSlaveConn()
	if err != nil {
		return newErrorIntCmd(err)
	}
	return conn.PFCount(keys...)
}

func (p *Pool) PFMerge(dest string, keys ...string) *redis.StatusCmd {
	if _, ok := p.connFactory.(*ShardConnFactory); ok {
		return newErrorStatusCmd(errShardPoolUnSupported)
	}
	conn, _ := p.connFactory.getMasterConn()
	return conn.PFMerge(dest, keys...)
}

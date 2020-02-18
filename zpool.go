package pool

import "github.com/go-redis/redis"

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
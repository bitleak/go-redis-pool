package pool

import (
	"time"

	"github.com/go-redis/redis/v7"
	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
)

var _ = Describe("Pool", func() {
	var haPool *Pool
	var shardPool *Pool
	var err error
	var pools []*Pool

	BeforeEach(func() {
		haConfig := &HAConfig{
			Master: "127.0.0.1:8379",
			Slaves: []string{
				"127.0.0.1:8380",
				"127.0.0.1:8381",
			},
		}
		haConfig1 := &HAConfig{
			Master: "127.0.0.1:8382",
			Slaves: []string{
				"127.0.0.1:8383",
			},
		}

		haPool, err = NewHA(haConfig)
		Expect(err).NotTo(HaveOccurred())
		master, _ := haPool.WithMaster()
		Expect(master.FlushDB().Err()).NotTo(HaveOccurred())

		shardPool, err = NewShard(&ShardConfig{
			Shards: []*HAConfig{
				haConfig,
				haConfig1,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		shards := shardPool.connFactory.(*ShardConnFactory).shards
		for _, shard := range shards {
			master, _ = shard.getMasterConn()
			Expect(master.FlushDB().Err()).NotTo(HaveOccurred())
		}
		pools = []*Pool{haPool, shardPool}
	})

	AfterEach(func() {
		haPool.Close()
		shardPool.Close()
	})

	Describe("Commands", func() {

		It("ping", func() {
			for _, pool := range pools {
				_, err := pool.Ping().Result()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("get/set", func() {
			for _, pool := range pools {
				result := pool.Set("foo", "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				// wait for master progressing the set result
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get("foo").Val()).To(Equal("bar"))
			}
		})

		It("echo", func() {
			Expect(haPool.Echo("hello").Err()).NotTo(HaveOccurred())
			Expect(shardPool.Echo("hello").Err()).To(Equal(errShardPoolUnSupported))
		})

		It("delete", func() {
			keys := []string{"a0", "b0", "c0", "d0"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				deleteKeys := append(keys, "e")
				n, err := pool.Del(deleteKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
			}
		})

		It("unlink", func() {
			keys := []string{"a1", "b1", "c1", "d1"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				unlinkKeys := append(keys, "e1")
				n, err := pool.Unlink(unlinkKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
			}
		})

		It("touch", func() {
			keys := []string{"a2", "b2", "c2", "d2"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				touchKeys := append(keys, "e2")
				n, err := pool.Touch(touchKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
				pool.Del(keys...)
			}
		})

		It("mget", func() {
			keys := []string{"a3", "b3", "c3", "d3"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(key, key, 0).Err()).NotTo(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				mgetKeys := append(keys, "e3")
				vals, err := pool.MGet(mgetKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(vals)).To(Equal(len(keys) + 1))
				for i := 0; i < len(keys); i++ {
					Expect(vals[i].(string)).To(Equal(keys[i]))
				}
				Expect(vals[len(keys)]).To(BeNil())
				pool.Del(keys...)
			}
		})

		It("exists", func() {
			keys := []string{"a4", "b4", "c4", "d4"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				existsKeys := append(keys, "e4")
				n, err := pool.Exists(existsKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
				pool.Del(keys...)
			}
		})

		It("mset", func() {
			kvs := []string{"key1", "value1", "key2", "value2", "key3", "value3"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				Expect(pool.MSet(kvs).Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				vals, err := pool.MGet(keys...)
				Expect(err).NotTo(HaveOccurred())
				for i := 0; i < len(vals); i += 1 {
					Expect(vals[i].(string)).To(Equal(kvs[2*i+1]))
				}
				pool.Del(keys...)
			}
		})

		It("msetnx", func() {
			kvs := []string{"key1_nx", "value1", "key2_nx", "value2"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				Expect(pool.MSetNX(kvs).Val()).To(Equal(true))
				Expect(pool.MSetNX(kvs).Val()).To(Equal(false))
				if pool == shardPool {
					Expect(pool.MSetNX(append(kvs, "key3_nx", "value3")).Err()).To(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				vals, err := pool.MGet(keys...)
				Expect(err).NotTo(HaveOccurred())
				for i := 0; i < len(vals); i += 1 {
					Expect(vals[i].(string)).To(Equal(kvs[2*i+1]))
				}
				pool.Del(keys...)
			}
		})

		It("expire", func() {
			key := "expire_foo"
			for _, pool := range pools {
				result := pool.Set(key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				Expect(pool.Expire(key, 10*time.Second).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.TTL(key).Val()).NotTo(Equal(-1))
				pool.Del(key)
			}
		})

		It("expire_at", func() {
			key := "expireat_foo"
			for _, pool := range pools {
				result := pool.Set(key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				Expect(pool.ExpireAt(key, time.Now().Add(10*time.Second)).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.TTL(key).Val()).NotTo(Equal(-1))
				pool.Del(key)
			}
		})

		It("rename", func() {
			key := "rename_key"
			newKey := "rename_key_new"
			for _, pool := range pools {
				result := pool.Set(key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				result = pool.Rename(key, newKey)
				Expect(result.Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get(newKey).Val()).To(Equal("bar"))
				Expect(pool.Get(key).Val()).To(Equal(""))
				pool.Del(newKey)
			}
		})

		It("renamenx", func() {
			key := "renamenx_key"
			newKey := "renamenx_key_new4"
			for _, pool := range pools {
				if pool == shardPool {
					Expect(pool.Set(key, "bar", 0).Val()).To(Equal("OK"))
					Expect(pool.RenameNX(key, newKey).Val()).To(Equal(true))
					time.Sleep(10 * time.Millisecond)
					Expect(pool.Get(newKey).Val()).To(Equal("bar"))
					Expect(pool.Get(key).Val()).To(Equal(""))
					pool.Del(newKey)
				}
			}
		})

		It("type", func() {
			key := "type_key"
			for _, pool := range pools {
				Expect(pool.Set(key, "bar", 0).Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Type(key).Val()).To(Equal("string"))
				pool.Del(key)
			}
		})

		It("append", func() {
			key := "append_key"
			for _, pool := range pools {
				Expect(pool.Append(key, "hello").Val()).To(Equal(int64(5)))
				Expect(pool.Append(key, "world").Val()).To(Equal(int64(10)))
				pool.Del(key)
			}
		})

		It("get range", func() {
			key := "getrange_key"
			for _, pool := range pools {
				Expect(pool.Set(key, "hello,world", 0).Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.GetRange(key, 2, 5).Val()).To(Equal("llo,"))
				pool.Del(key)
			}
		})

		It("getset", func() {
			key := "getset_key"
			for _, pool := range pools {
				Expect(pool.Set(key, "hello", 0).Val()).To(Equal("OK"))
				Expect(pool.GetSet(key, "world").Val()).To(Equal("hello"))
				pool.Del(key)
			}
		})

		It("get/set bit", func() {
			key := "setbit_key"
			offsets := []int64{1, 3, 5, 7, 15, 31, 63}
			for _, pool := range pools {
				for _, offset := range offsets {
					Expect(pool.SetBit(key, offset, 1).Val()).To(Equal(int64(0)))
				}
				time.Sleep(10 * time.Millisecond)
				for _, offset := range offsets {
					Expect(pool.GetBit(key, offset).Val()).To(Equal(int64(1)))
				}
				Expect(pool.BitPos(key, 1, 0, 64).Val()).To(Equal(int64(1)))
				Expect(pool.BitPos(key, 0, 0, 64).Val()).To(Equal(int64(0)))
				Expect(pool.BitCount(key, &redis.BitCount{
					Start: 0,
					End:   64,
				}).Val()).To(Equal(int64(len(offsets))))
				pool.Del(key)
			}
		})

		It("bit op", func() {
			key0 := "op_key0"
			key1 := "op_key1"
			key2 := "op_key_cross"
			destKey := "opDestKey"
			for _, pool := range pools {
				Expect(pool.SetBit(key0, 0, 1).Err()).NotTo(HaveOccurred())
				Expect(pool.SetBit(key1, 0, 1).Err()).NotTo(HaveOccurred())
				if pool == shardPool {
					Expect(pool.BitOpAnd(destKey, key0, key2).Err()).To(HaveOccurred())
				}
				Expect(pool.BitOpAnd(destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(destKey, 0).Val()).To(Equal(int64(1)))
				Expect(pool.BitOpOr(destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(destKey, 0).Val()).To(Equal(int64(1)))
				Expect(pool.BitOpXor(destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(destKey, 0).Val()).To(Equal(int64(0)))
				Expect(pool.BitOpNot(destKey, key0).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(destKey, 0).Val()).To(Equal(int64(0)))
				pool.Del(key0, key1, destKey)
			}
		})

		It("incr/decr", func() {
			key := "incr_key"
			for _, pool := range pools {
				Expect(pool.Set(key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.Incr(key).Val()).To(Equal(int64(101)))
				Expect(pool.Decr(key).Val()).To(Equal(int64(100)))
				Expect(pool.IncrBy(key, 100).Val()).To(Equal(int64(200)))
				Expect(pool.DecrBy(key, 100).Val()).To(Equal(int64(100)))
				pool.Del(key)
			}
		})

		It("incrbyfloat", func() {
			key := "incrbyfloat_key"
			for _, pool := range pools {
				Expect(pool.Set(key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.IncrByFloat(key, 1.5).Val()).To(Equal(101.5))
				pool.Del(key)
			}
		})

		It("setnx", func() {
			key := "setnx_key"
			for _, pool := range pools {
				Expect(pool.SetNX(key, "bar", 0).Val()).To(Equal(true))
				Expect(pool.SetNX(key, "bar", 0).Val()).To(Equal(false))
				pool.Del(key)
			}
		})

		It("setxx", func() {
			key := "setxx_key"
			for _, pool := range pools {
				Expect(pool.SetXX(key, "bar", 0).Val()).To(Equal(false))
				Expect(pool.Set(key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.SetNX(key, "bar", 0).Val()).To(Equal(false))
				pool.Del(key)
			}
		})

		It("setrange", func() {
			key := "setrange_key"
			for _, pool := range pools {
				Expect(pool.Set(key, "hello,world", 0).Err()).NotTo(HaveOccurred())
				Expect(pool.SetRange(key, 6, "myworld").Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get(key).Val()).To(Equal("hello,myworld"))
				pool.Del(key)
			}
		})

		It("strlen", func() {
			key := "strlen_key"
			for _, pool := range pools {
				Expect(pool.Set(key, "hello", 0).Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				Expect(pool.StrLen(key).Val()).To(Equal(int64(5)))
				pool.Del(key)
			}
		})

		It("hset/hget", func() {
			key := "hset_key"
			field := "filed"
			for _, pool := range pools {
				Expect(pool.HSet(key, field, "bar").Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HGet(key, field).Val()).To(Equal("bar"))
				pool.Del(key)
			}
		})

		It("hexists", func() {
			key := "hexists_key"
			field := "filed"
			for _, pool := range pools {
				Expect(pool.HSet(key, field, "bar").Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HExists(key, field).Val()).To(Equal(true))
				Expect(pool.HDel(key, field).Val()).To(Equal(int64(1)))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HExists(key, field).Val()).To(Equal(false))
				pool.Del(key)
			}
		})

		It("hgetall", func() {
			key := "hgetall_key"
			fvs := []string{"f1", "v1", "f2", "v2", "f3", "v3"}
			for _, pool := range pools {
				Expect(pool.HMSet(key, fvs).Val()).To(Equal(int64(len(fvs) / 2)))
				time.Sleep(10 * time.Millisecond)
				kvs := pool.HGetAll(key).Val()
				for i := 0; i < len(fvs); i += 2 {
					Expect(kvs[fvs[i]]).To(Equal(fvs[i+1]))
				}
				Expect(len(pool.HKeys(key).Val())).To(Equal(len(fvs) / 2))
				Expect(len(pool.HVals(key).Val())).To(Equal(len(fvs) / 2))
				pool.Del(key)
			}
		})

		It("hmset/hmget", func() {
			key := "hmset_key"
			fvs := []string{"f1", "v1", "f2", "v2", "f3", "v3"}
			fields := make([]string, len(fvs)/2)
			for i := 0; i < len(fvs); i += 2 {
				fields[i/2] = fvs[i]
			}
			for _, pool := range pools {
				Expect(pool.HMSet(key, fvs).Val()).To(Equal(int64(len(fvs) / 2)))
				time.Sleep(10 * time.Millisecond)
				vals := pool.HMGet(key, fields...).Val()
				Expect(len(vals)).To(Equal(len(fvs) / 2))
				Expect(pool.HLen(key).Val()).To(Equal(int64(len(fvs) / 2)))
				pool.Del(key)
			}
		})

		It("hincrby", func() {
			key := "hincrby_key"
			intField := "int_field"
			floatField := "float_field"
			for _, pool := range pools {
				Expect(pool.HIncrBy(key, intField, 100).Val()).To(Equal(int64(100)))
				Expect(pool.HIncrBy(key, intField, 100).Val()).To(Equal(int64(200)))
				Expect(pool.HIncrByFloat(key, floatField, 10.5).Val()).To(Equal(float64(10.5)))
				Expect(pool.HIncrByFloat(key, floatField, 10.5).Val()).To(Equal(float64(21)))
				Expect(pool.HDel(key, intField, floatField).Val()).To(Equal(int64(2)))
				pool.Del(key)
			}
		})

		It("blpop/brpop", func() {
			key := "blpop_key"
			noExistsKey := "non_exists_key"
			for _, pool := range pools {
				go func() {
					time.Sleep(100 * time.Millisecond)
					pool.LPush(key, "e1", "e2")
				}()
				Expect(pool.BLPop(time.Second, key).Val()).To(Equal([]string{key, "e2"}))
				Expect(pool.BLPop(time.Second, key).Val()).To(Equal([]string{key, "e1"}))
				if pool == shardPool {
					Expect(pool.BLPop(time.Second, key, noExistsKey).Err()).To(HaveOccurred())
				}
			}
		})

		It("brpop", func() {
			key := "brpop_key"
			noExistsKey := "non_exists_key"
			for _, pool := range pools {
				go func() {
					time.Sleep(100 * time.Millisecond)
					pool.LPush(key, "e1", "e2")
				}()
				Expect(pool.BRPop(time.Second, key).Val()).To(Equal([]string{key, "e1"}))
				Expect(pool.BRPop(time.Second, key).Val()).To(Equal([]string{key, "e2"}))
				if pool == shardPool {
					Expect(pool.BRPop(time.Second, key, noExistsKey).Err()).To(HaveOccurred())
				}
			}
		})

		It("brpoplpush", func() {
			sourceKey := "brpoplpush_source"
			destKey := "brpoplpush_destination"
			crossShardKey := "cross_shard_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				go func() {
					time.Sleep(100 * time.Millisecond)
					pool.LPush(sourceKey, elems)
				}()
				for _, elem := range elems {
					Expect(pool.BRPopLPush(sourceKey, destKey, time.Second).Val()).To(Equal(elem))
				}
				if pool == shardPool {
					Expect(pool.BRPop(time.Second, sourceKey, crossShardKey).Err()).To(HaveOccurred())
				}
				pool.Del(sourceKey, destKey)
			}
		})

		It("lindex", func() {
			key := "lindex_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				pool.RPush(key, elems)
				time.Sleep(10 * time.Millisecond)
				for i, elem := range elems {
					Expect(pool.LIndex(key, int64(i)).Val()).To(Equal(elem))
				}
				pool.Del(key)
			}
		})

		It("linsert", func() {
			key := "linsert_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				pool.RPush(key, elems)
				Expect(pool.LInsertBefore(key, "e1", "hello").Val()).
					To(Equal(int64(len(elems) + 1)))
				Expect(pool.LInsertBefore(key, "e0", "hello").Val()).
					To(Equal(int64(-1)))
				Expect(pool.LInsertAfter(key, "hello", "world").Val()).
					To(Equal(int64(len(elems) + 2)))
				Expect(pool.LLen(key).Val()).To(Equal(int64(len(elems) + 2)))
				pool.Del(key)
			}
		})

		It("lpush/rpop", func() {
			key := "lpush_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.LPush(key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.RPop(key).Val()).To(Equal(elem))
				}
			}
		})

		It("rpush/lpop", func() {
			key := "rpush_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.LPop(key).Val()).To(Equal(elem))
				}
			}
		})

		It("lpushx", func() {
			key := "lpushx_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.LPushX(key, elems).Val()).To(Equal(int64(0)))
				pool.LPush(key, "e0")
				Expect(pool.LPushX(key, elems).Val()).To(Equal(int64(len(elems) + 1)))
				Expect(pool.RPop(key).Val()).To(Equal("e0"))
				for _, elem := range elems {
					Expect(pool.RPop(key).Val()).To(Equal(elem))
				}
			}
		})

		It("rpushx", func() {
			key := "rpushx_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPushX(key, elems).Val()).To(Equal(int64(0)))
				pool.RPush(key, "e0")
				Expect(pool.RPushX(key, elems).Val()).To(Equal(int64(len(elems) + 1)))
				Expect(pool.LPop(key).Val()).To(Equal("e0"))
				for _, elem := range elems {
					Expect(pool.LPop(key).Val()).To(Equal(elem))
				}
			}
		})

		It("lrange", func() {
			key := "lrange_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(key, elems).Val()).To(Equal(int64(len(elems))))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.LRange(key, 0, -1).Val()).To(Equal(elems))
				pool.Del(key)
			}
		})

		It("lrem", func() {
			key := "lrem_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.LRem(key, 0, elem).Val()).To(Equal(int64(1)))
				}
			}
		})

		It("lset", func() {
			key := "lset_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(key, elems).Val()).To(Equal(int64(len(elems))))
				Expect(pool.LSet(key, 0, "hello").Val()).To(Equal("OK"))
				pool.Del(key)
			}
		})

		It("ltrim", func() {
			key := "ltrim_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(key, elems).Val()).To(Equal(int64(len(elems))))
				Expect(pool.LTrim(key, 1, -1).Val()).To(Equal("OK"))
				Expect(pool.LRange(key, 0, -1).Val()).To(Equal(elems[1:]))
				pool.Del(key)
			}
		})

		It("rpoplpush", func() {
			sourceKey := "rpoplpush_source"
			destKey := "rpoplpush_destination"
			for _, pool := range pools {
				ret, err := pool.RPopLPush(sourceKey, destKey).Result()
				Expect(err).To(Equal(redis.Nil))
				Expect(ret).To(Equal(""))
				pool.LPush(sourceKey, "hello")
				ret, err = pool.RPopLPush(sourceKey, destKey).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal("hello"))
			}
		})

		It("sadd", func() {
			key := "sadd_key"
			members := []string{"sadd_member1", "sadd_member2", "sadd_member3"}
			for _, pool := range pools {
				ret, err := pool.SAdd(key, members).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				Expect(pool.SMembers(key).Val()).To(ContainElements("sadd_member1", "sadd_member2", "sadd_member3"))
			}
		})

		It("scard", func() {
			key := "scard_key"
			members := []string{"scard_member1", "scard_member2", "scard_member3"}
			for _, pool := range pools {
				ret, err := pool.SAdd(key, members).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				ret, err = pool.SCard(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
			}
		})

		It("sdiff", func() {
			key1 := "sdiff_key1"
			members1 := []string{"sdiff_member1", "sdiff_member2"}
			key2 := "sdiff_key2"
			members2 := []string{"sdiff_member2", "sdiff_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Val()).To(Equal(int64(2)))
				Expect(pool.SAdd(key2, members2).Val()).To(Equal(int64(2)))
				ret, err := pool.SDiff(key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(ContainElements("sdiff_member1"))
			}
		})

		It("sdiffstore", func() {
			key1 := "sdiffstore_key1"
			members1 := []string{"sdiffstore_member1", "sdiffstore_member2"}
			key2 := "sdiffstore_key2"
			members2 := []string{"sdiffstore_member2", "sdiffstore_member3"}
			destination := "sdiffstore_destination"
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SDiffStore(destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				Expect(pool.SMembers(destination).Val()).To(ContainElements("sdiffstore_member1"))
			}
		})

		It("sinter", func() {
			key1 := "sinter_key1"
			members1 := []string{"sinter_member1", "sinter_member2"}
			key2 := "sinter_key2"
			members2 := []string{"sinter_member2", "sinter_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SInter(key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(ContainElements("sinter_member2"))
			}
		})

		It("sinterstore", func() {
			key1 := "sinterstore_key1"
			members1 := []string{"sinterstore_member1", "sinterstore_member2"}
			key2 := "sinterstore_key2"
			members2 := []string{"sinterstore_member2", "sinterstore_member3"}
			destination := "sinterstore_destination"
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SInterStore(destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				Expect(pool.SMembers(destination).Val()).To(ContainElements("sinterstore_member2"))
			}
		})

		It("sismember", func() {
			key := "sismember_key"
			members := []string{"sismember_member1", "sismember_member2", "sismember_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SIsMember(key, "sismember_member1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(true))
				ret, err = pool.SIsMember(key, "sismember_member4").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(false))
			}
		})

		It("smembers", func() {
			key := "smembers_key"
			members := []string{"smembers_member1", "smembers_member2", "smembers_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMembers(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(ContainElements("smembers_member1", "smembers_member2", "smembers_member3"))
			}
		})

		It("smembersmap", func() {
			key := "smembersmap_key"
			members := []string{"smembersmap_member1", "smembersmap_member2", "smembersmap_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMembersMap(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(HaveKey("smembersmap_member1"))
				Expect(ret).To(HaveKey("smembersmap_member2"))
				Expect(ret).To(HaveKey("smembersmap_member3"))
			}
		})

		It("smove", func() {
			source := "smove_key1"
			members1 := []string{"smove_member1", "smove_member2"}
			destination := "smove_key2"
			members2 := []string{"smove_member3", "smove_member4"}
			for _, pool := range pools {
				Expect(pool.SAdd(source, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(destination, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMove(source, destination, "smove_member1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(true))
				Expect(pool.SMembers(destination).Val()).To(ContainElements("smove_member1"))
			}
		})

		It("spop", func() {
			key := "spop_key"
			members := []string{"spop_member1", "spop_member2", "spop_member3"}
			for _, pool := range pools {
				memberMap := make(map[string]bool)
				for _, member := range members {
					memberMap[member] = true
				}
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SPop(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(memberMap[ret]).To(Equal(true))
				delete(memberMap, ret)
				for member := range memberMap {
					Expect(pool.SMembers(key).Val()).To(ContainElements(member))
				}
			}
		})

		It("spopn", func() {
			key := "spopn_key"
			members := []string{"spopn_member1", "spopn_member2", "spopn_member3"}
			for _, pool := range pools {
				memberMap := make(map[string]bool)
				for _, member := range members {
					memberMap[member] = true
				}
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				rets, err := pool.SPopN(key, 2).Result()
				Expect(err).NotTo(HaveOccurred())
				for _, ret := range rets {
					Expect(memberMap[ret]).To(Equal(true))
					delete(memberMap, ret)
				}
				for member := range memberMap {
					Expect(pool.SMembers(key).Val()).To(ContainElements(member))
				}
			}
		})

		It("srandmember", func() {
			key := "srandmember_key"
			members := []string{"srandmember_member1", "srandmember_member2", "srandmember_member3"}
			for _, pool := range pools {
				memberMap := make(map[string]bool)
				for _, member := range members {
					memberMap[member] = true
				}
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SRandMember(key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(memberMap[ret]).To(Equal(true))
				Expect(pool.SMembers(key).Val()).To(ContainElements("srandmember_member1", "srandmember_member2", "srandmember_member3"))
			}
		})

		It("srandmembern", func() {
			key := "srandmembern_key"
			members := []string{"srandmembern_member1", "srandmembern_member2", "srandmembern_member3"}
			for _, pool := range pools {
				memberMap := make(map[string]bool)
				for _, member := range members {
					memberMap[member] = true
				}
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				rets, err := pool.SRandMemberN(key, 2).Result()
				Expect(err).NotTo(HaveOccurred())
				for _, ret := range rets {
					Expect(memberMap[ret]).To(Equal(true))
				}
				Expect(pool.SMembers(key).Val()).To(ContainElements("srandmembern_member1", "srandmembern_member2", "srandmembern_member3"))
			}
		})

		It("srem", func() {
			key := "srem_key"
			members := []string{"srem_member1", "srem_member2", "srem_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SRem(key, "srem_member1", "srem_member2").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(2)))
				Expect(pool.SMembers(key).Val()).NotTo(ContainElements("srem_member1", "srem_member2"))
			}
		})

		It("sunion", func() {
			key1 := "sunion_key1"
			members1 := []string{"sunion_member1", "sunion_member2"}
			key2 := "sunion_key2"
			members2 := []string{"sunion_member2", "sunion_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SUnion(key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(ContainElements("sunion_member1", "sunion_member2", "sunion_member3"))
			}
		})

		It("sunionstore", func() {
			key1 := "sunionstore_source1"
			members1 := []string{"sunionstore_member1", "sunionstore_member2"}
			key2 := "sunionstore_source2"
			members2 := []string{"sunionstore_member2", "sunionstore_member3"}
			destination := "sunionstore_destination"
			for _, pool := range pools {
				Expect(pool.SAdd(key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SUnionStore(destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				Expect(pool.SMembers(destination).Val()).To(ContainElements("sunionstore_member1", "sunionstore_member2", "sunionstore_member3"))
				pool.Del(destination)
			}
		})
	})
})

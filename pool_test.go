package pool

import (
	"context"
	"math"
	"sort"
	"time"

	. "github.com/onsi/ginkgo"
	. "github.com/onsi/gomega"
	"github.com/redis/go-redis/v9"
)

var _ = Describe("Pool", func() {
	var haPool *Pool
	var shardPool *Pool
	var err error
	var pools []*Pool

	ctx := context.Background()

	BeforeEach(func() {
		haConfig := &HAConfig{
			Master: "127.0.0.1:8379",
			Slaves: []string{
				"127.0.0.1:8380:100",
				"127.0.0.1:8381:200",
			},
			PollType: PollByWeight,
		}
		haConfig1 := &HAConfig{
			Master: "127.0.0.1:8382",
			Slaves: []string{
				"127.0.0.1:8383",
			},
			PollType: PollByWeight,
		}

		haPool, err = NewHA(haConfig)
		Expect(err).NotTo(HaveOccurred())
		master, _ := haPool.WithMaster()
		Expect(master.FlushDB(ctx).Err()).NotTo(HaveOccurred())

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
			Expect(master.FlushDB(ctx).Err()).NotTo(HaveOccurred())
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
				_, err := pool.Ping(ctx).Result()
				Expect(err).NotTo(HaveOccurred())
			}
		})

		It("get/set", func() {
			for _, pool := range pools {
				result := pool.Set(ctx, "foo", "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				// wait for master progressing the set result
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get(ctx, "foo").Val()).To(Equal("bar"))
			}
		})

		It("echo", func() {
			Expect(haPool.Echo(ctx, "hello").Err()).NotTo(HaveOccurred())
			Expect(shardPool.Echo(ctx, "hello").Err()).To(Equal(errShardPoolUnSupported))
		})

		It("delete", func() {
			keys := []string{"a0", "b0", "c0", "d0"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				deleteKeys := append(keys, "e")
				n, err := pool.Del(ctx, deleteKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
			}
		})

		It("unlink", func() {
			keys := []string{"a1", "b1", "c1", "d1"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				unlinkKeys := append(keys, "e1")
				n, err := pool.Unlink(ctx, unlinkKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
			}
		})

		It("touch", func() {
			keys := []string{"a2", "b2", "c2", "d2"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				touchKeys := append(keys, "e2")
				n, err := pool.Touch(ctx, touchKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("mget", func() {
			keys := []string{"a3", "b3", "c3", "d3"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, key, 0).Err()).NotTo(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				mgetKeys := append(keys, "e3")
				vals, err := pool.MGet(ctx, mgetKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(len(vals)).To(Equal(len(keys) + 1))
				for i := 0; i < len(keys); i++ {
					Expect(vals[i].(string)).To(Equal(keys[i]))
				}
				Expect(vals[len(keys)]).To(BeNil())
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("exists", func() {
			keys := []string{"a4", "b4", "c4", "d4"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, "value", 0).Err()).NotTo(HaveOccurred())
				}
				existsKeys := append(keys, "e4")
				n, err := pool.Exists(ctx, existsKeys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(n)).To(Equal(len(keys)))
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("mset", func() {
			kvs := []string{"key1", "value1", "key2", "value2", "key3", "value3"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				Expect(pool.MSet(ctx, kvs).Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				vals, err := pool.MGet(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				for i := 0; i < len(vals); i += 1 {
					Expect(vals[i].(string)).To(Equal(kvs[2*i+1]))
				}
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("msetnx", func() {
			kvs := []string{"key1_nx", "value1", "key2_nx", "value2"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				Expect(pool.MSetNX(ctx, kvs).Val()).To(Equal(true))
				Expect(pool.MSetNX(ctx, kvs).Val()).To(Equal(false))
				if pool == shardPool {
					Expect(pool.MSetNX(ctx, append(kvs, "key3_nx", "value3")).Err()).To(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				vals, err := pool.MGet(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				for i := 0; i < len(vals); i += 1 {
					Expect(vals[i].(string)).To(Equal(kvs[2*i+1]))
				}
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("expire", func() {
			key := "expire_foo"
			for _, pool := range pools {
				result := pool.Set(ctx, key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				Expect(pool.Expire(ctx, key, 10*time.Second).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.TTL(ctx, key).Val()).NotTo(Equal(-1))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("expire_nx", func() {
			key := "expirenx_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
				Expect(pool.Expire(ctx, key, 10*time.Second).Val()).To(Equal(true))
				Expect(pool.ExpireNX(ctx, key, 20*time.Second).Val()).To(Equal(false))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("expire_xx", func() {
			key := "expirexx_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
				Expect(pool.ExpireXX(ctx, key, 10*time.Second).Val()).To(Equal(false))
				Expect(pool.Expire(ctx, key, 10*time.Second).Val()).To(Equal(true))
				Expect(pool.ExpireXX(ctx, key, 20*time.Second).Val()).To(Equal(true))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("expire_gt", func() {
			key := "expiregt_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
				Expect(pool.Expire(ctx, key, 10*time.Second).Val()).To(Equal(true))
				Expect(pool.ExpireGT(ctx, key, time.Second).Val()).To(Equal(false))
				Expect(pool.ExpireGT(ctx, key, 20*time.Second).Val()).To(Equal(true))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("expire_lt", func() {
			key := "expirelt_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
				Expect(pool.Expire(ctx, key, 10*time.Second).Val()).To(Equal(true))
				Expect(pool.ExpireLT(ctx, key, 20*time.Second).Val()).To(Equal(false))
				Expect(pool.ExpireLT(ctx, key, time.Second).Val()).To(Equal(true))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("expire_at", func() {
			key := "expireat_foo"
			for _, pool := range pools {
				result := pool.Set(ctx, key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				Expect(pool.ExpireAt(ctx, key, time.Now().Add(10*time.Second)).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.TTL(ctx, key).Val()).NotTo(Equal(-1))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("rename", func() {
			key := "rename_key"
			newKey := "rename_key_new"
			for _, pool := range pools {
				result := pool.Set(ctx, key, "bar", 0)
				Expect(result.Val()).To(Equal("OK"))
				result = pool.Rename(ctx, key, newKey)
				Expect(result.Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get(ctx, newKey).Val()).To(Equal("bar"))
				Expect(pool.Get(ctx, key).Val()).To(Equal(""))
				_, _ = pool.Del(ctx, newKey)
			}
		})

		It("renamenx", func() {
			key := "renamenx_key"
			newKey := "renamenx_key_new4"
			for _, pool := range pools {
				if pool == shardPool {
					Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
					Expect(pool.RenameNX(ctx, key, newKey).Val()).To(Equal(true))
					time.Sleep(10 * time.Millisecond)
					Expect(pool.Get(ctx, newKey).Val()).To(Equal("bar"))
					Expect(pool.Get(ctx, key).Val()).To(Equal(""))
					_, _ = pool.Del(ctx, newKey)
				}
			}
		})

		It("type", func() {
			key := "type_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "bar", 0).Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Type(ctx, key).Val()).To(Equal("string"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("append", func() {
			key := "append_key"
			for _, pool := range pools {
				Expect(pool.Append(ctx, key, "hello").Val()).To(Equal(int64(5)))
				Expect(pool.Append(ctx, key, "world").Val()).To(Equal(int64(10)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("get range", func() {
			key := "getrange_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "hello,world", 0).Val()).To(Equal("OK"))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.GetRange(ctx, key, 2, 5).Val()).To(Equal("llo,"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("getset", func() {
			key := "getset_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "hello", 0).Val()).To(Equal("OK"))
				Expect(pool.GetSet(ctx, key, "world").Val()).To(Equal("hello"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("get/set bit", func() {
			key := "setbit_key"
			offsets := []int64{1, 3, 5, 7, 15, 31, 63}
			for _, pool := range pools {
				for _, offset := range offsets {
					Expect(pool.SetBit(ctx, key, offset, 1).Val()).To(Equal(int64(0)))
				}
				time.Sleep(10 * time.Millisecond)
				for _, offset := range offsets {
					Expect(pool.GetBit(ctx, key, offset).Val()).To(Equal(int64(1)))
				}
				Expect(pool.BitPos(ctx, key, 1, 0, 64).Val()).To(Equal(int64(1)))
				Expect(pool.BitPos(ctx, key, 0, 0, 64).Val()).To(Equal(int64(0)))
				Expect(pool.BitCount(ctx, key, &redis.BitCount{
					Start: 0,
					End:   64,
				}).Val()).To(Equal(int64(len(offsets))))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("bit op", func() {
			key0 := "op_key0"
			key1 := "op_key1"
			key2 := "op_key_cross"
			destKey := "opDestKey"
			for _, pool := range pools {
				Expect(pool.SetBit(ctx, key0, 0, 1).Err()).NotTo(HaveOccurred())
				Expect(pool.SetBit(ctx, key1, 0, 1).Err()).NotTo(HaveOccurred())
				if pool == shardPool {
					Expect(pool.BitOpAnd(ctx, destKey, key0, key2).Err()).To(HaveOccurred())
				}
				Expect(pool.BitOpAnd(ctx, destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(ctx, destKey, 0).Val()).To(Equal(int64(1)))
				Expect(pool.BitOpOr(ctx, destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(ctx, destKey, 0).Val()).To(Equal(int64(1)))
				Expect(pool.BitOpXor(ctx, destKey, key0, key1).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(ctx, destKey, 0).Val()).To(Equal(int64(0)))
				Expect(pool.BitOpNot(ctx, destKey, key0).Err()).NotTo(HaveOccurred())
				Expect(pool.GetBit(ctx, destKey, 0).Val()).To(Equal(int64(0)))
				_, _ = pool.Del(ctx, key0, key1, destKey)
			}
		})

		It("incr/decr", func() {
			key := "incr_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.Incr(ctx, key).Val()).To(Equal(int64(101)))
				Expect(pool.Decr(ctx, key).Val()).To(Equal(int64(100)))
				Expect(pool.IncrBy(ctx, key, 100).Val()).To(Equal(int64(200)))
				Expect(pool.DecrBy(ctx, key, 100).Val()).To(Equal(int64(100)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("incrbyfloat", func() {
			key := "incrbyfloat_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.IncrByFloat(ctx, key, 1.5).Val()).To(Equal(101.5))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("setnx", func() {
			key := "setnx_key"
			for _, pool := range pools {
				Expect(pool.SetNX(ctx, key, "bar", 0).Val()).To(Equal(true))
				Expect(pool.SetNX(ctx, key, "bar", 0).Val()).To(Equal(false))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("setxx", func() {
			key := "setxx_key"
			for _, pool := range pools {
				Expect(pool.SetXX(ctx, key, "bar", 0).Val()).To(Equal(false))
				Expect(pool.Set(ctx, key, 100, 0).Err()).NotTo(HaveOccurred())
				Expect(pool.SetNX(ctx, key, "bar", 0).Val()).To(Equal(false))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("setargs", func() {
			key := "setargs_key"
			for _, pool := range pools {
				Expect(pool.SetArgs(ctx, key, "bar", redis.SetArgs{Mode: "XX"}).Val()).To(Equal(""))
				Expect(pool.SetArgs(ctx, key, "bar", redis.SetArgs{}).Val()).To(Equal("OK"))
				Expect(pool.SetArgs(ctx, key, "bar", redis.SetArgs{Mode: "NX"}).Val()).To(Equal(""))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("setrange", func() {
			key := "setrange_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "hello,world", 0).Err()).NotTo(HaveOccurred())
				Expect(pool.SetRange(ctx, key, 6, "myworld").Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				Expect(pool.Get(ctx, key).Val()).To(Equal("hello,myworld"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("strlen", func() {
			key := "strlen_key"
			for _, pool := range pools {
				Expect(pool.Set(ctx, key, "hello", 0).Err()).NotTo(HaveOccurred())
				time.Sleep(10 * time.Millisecond)
				Expect(pool.StrLen(ctx, key).Val()).To(Equal(int64(5)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("hset/hget", func() {
			key := "hset_key"
			field := "filed"
			for _, pool := range pools {
				Expect(pool.HSet(ctx, key, field, "bar").Val()).To(Equal(int64(1)))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HGet(ctx, key, field).Val()).To(Equal("bar"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("hexists", func() {
			key := "hexists_key"
			field := "filed"
			for _, pool := range pools {
				Expect(pool.HSet(ctx, key, field, "bar").Val()).To(Equal(int64(1)))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HExists(ctx, key, field).Val()).To(Equal(true))
				Expect(pool.HDel(ctx, key, field).Val()).To(Equal(int64(1)))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.HExists(ctx, key, field).Val()).To(Equal(false))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("hgetall", func() {
			key := "hgetall_key"
			fvs := []string{"f1", "v1", "f2", "v2", "f3", "v3"}
			for _, pool := range pools {
				Expect(pool.HMSet(ctx, key, fvs).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				kvs := pool.HGetAll(ctx, key).Val()
				for i := 0; i < len(fvs); i += 2 {
					Expect(kvs[fvs[i]]).To(Equal(fvs[i+1]))
				}
				Expect(len(pool.HKeys(ctx, key).Val())).To(Equal(len(fvs) / 2))
				Expect(len(pool.HVals(ctx, key).Val())).To(Equal(len(fvs) / 2))
				_, _ = pool.Del(ctx, key)
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
				Expect(pool.HMSet(ctx, key, fvs).Val()).To(Equal(true))
				time.Sleep(10 * time.Millisecond)
				vals := pool.HMGet(ctx, key, fields...).Val()
				Expect(len(vals)).To(Equal(len(fvs) / 2))
				Expect(pool.HLen(ctx, key).Val()).To(Equal(int64(len(fvs) / 2)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("hincrby", func() {
			key := "hincrby_key"
			intField := "int_field"
			floatField := "float_field"
			for _, pool := range pools {
				Expect(pool.HIncrBy(ctx, key, intField, 100).Val()).To(Equal(int64(100)))
				Expect(pool.HIncrBy(ctx, key, intField, 100).Val()).To(Equal(int64(200)))
				Expect(pool.HIncrByFloat(ctx, key, floatField, 10.5).Val()).To(Equal(float64(10.5)))
				Expect(pool.HIncrByFloat(ctx, key, floatField, 10.5).Val()).To(Equal(float64(21)))
				Expect(pool.HDel(ctx, key, intField, floatField).Val()).To(Equal(int64(2)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("blpop/brpop", func() {
			key := "blpop_key"
			noExistsKey := "non_exists_key"
			for _, pool := range pools {
				go func() {
					time.Sleep(100 * time.Millisecond)
					pool.LPush(ctx, key, "e1", "e2")
				}()
				Expect(pool.BLPop(ctx, time.Second, key).Val()).To(Equal([]string{key, "e2"}))
				Expect(pool.BLPop(ctx, time.Second, key).Val()).To(Equal([]string{key, "e1"}))
				if pool == shardPool {
					Expect(pool.BLPop(ctx, time.Second, key, noExistsKey).Err()).To(HaveOccurred())
				}
			}
		})

		It("brpop", func() {
			key := "brpop_key"
			noExistsKey := "non_exists_key"
			for _, pool := range pools {
				go func() {
					time.Sleep(100 * time.Millisecond)
					pool.LPush(ctx, key, "e1", "e2")
				}()
				Expect(pool.BRPop(ctx, time.Second, key).Val()).To(Equal([]string{key, "e1"}))
				Expect(pool.BRPop(ctx, time.Second, key).Val()).To(Equal([]string{key, "e2"}))
				if pool == shardPool {
					Expect(pool.BRPop(ctx, time.Second, key, noExistsKey).Err()).To(HaveOccurred())
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
					pool.LPush(ctx, sourceKey, elems)
				}()
				for _, elem := range elems {
					Expect(pool.BRPopLPush(ctx, sourceKey, destKey, time.Second).Val()).To(Equal(elem))
				}
				if pool == shardPool {
					Expect(pool.BRPop(ctx, time.Second, sourceKey, crossShardKey).Err()).To(HaveOccurred())
				}
				_, _ = pool.Del(ctx, sourceKey, destKey)
			}
		})

		It("lindex", func() {
			key := "lindex_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				pool.RPush(ctx, key, elems)
				time.Sleep(10 * time.Millisecond)
				for i, elem := range elems {
					Expect(pool.LIndex(ctx, key, int64(i)).Val()).To(Equal(elem))
				}
				_, _ = pool.Del(ctx, key)
			}
		})

		It("linsert", func() {
			key := "linsert_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				pool.RPush(ctx, key, elems)
				Expect(pool.LInsertBefore(ctx, key, "e1", "hello").Val()).
					To(Equal(int64(len(elems) + 1)))
				Expect(pool.LInsertBefore(ctx, key, "e0", "hello").Val()).
					To(Equal(int64(-1)))
				Expect(pool.LInsertAfter(ctx, key, "hello", "world").Val()).
					To(Equal(int64(len(elems) + 2)))
				Expect(pool.LLen(ctx, key).Val()).To(Equal(int64(len(elems) + 2)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("lpush/rpop", func() {
			key := "lpush_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.LPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.RPop(ctx, key).Val()).To(Equal(elem))
				}
			}
		})

		It("rpush/lpop", func() {
			key := "rpush_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.LPop(ctx, key).Val()).To(Equal(elem))
				}
			}
		})

		It("lpushx", func() {
			key := "lpushx_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.LPushX(ctx, key, elems).Val()).To(Equal(int64(0)))
				pool.LPush(ctx, key, "e0")
				Expect(pool.LPushX(ctx, key, elems).Val()).To(Equal(int64(len(elems) + 1)))
				Expect(pool.RPop(ctx, key).Val()).To(Equal("e0"))
				for _, elem := range elems {
					Expect(pool.RPop(ctx, key).Val()).To(Equal(elem))
				}
			}
		})

		It("rpushx", func() {
			key := "rpushx_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPushX(ctx, key, elems).Val()).To(Equal(int64(0)))
				pool.RPush(ctx, key, "e0")
				Expect(pool.RPushX(ctx, key, elems).Val()).To(Equal(int64(len(elems) + 1)))
				Expect(pool.LPop(ctx, key).Val()).To(Equal("e0"))
				for _, elem := range elems {
					Expect(pool.LPop(ctx, key).Val()).To(Equal(elem))
				}
			}
		})

		It("lrange", func() {
			key := "lrange_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				time.Sleep(10 * time.Millisecond)
				Expect(pool.LRange(ctx, key, 0, -1).Val()).To(Equal(elems))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("lrem", func() {
			key := "lrem_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				for _, elem := range elems {
					Expect(pool.LRem(ctx, key, 0, elem).Val()).To(Equal(int64(1)))
				}
			}
		})

		It("lset", func() {
			key := "lset_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				Expect(pool.LSet(ctx, key, 0, "hello").Val()).To(Equal("OK"))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("ltrim", func() {
			key := "ltrim_key"
			elems := []string{"e1", "e2", "e3"}
			for _, pool := range pools {
				Expect(pool.RPush(ctx, key, elems).Val()).To(Equal(int64(len(elems))))
				Expect(pool.LTrim(ctx, key, 1, -1).Val()).To(Equal("OK"))
				Expect(pool.LRange(ctx, key, 0, -1).Val()).To(Equal(elems[1:]))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("rpoplpush", func() {
			sourceKey := "rpoplpush_source"
			destKey := "rpoplpush_destination"
			for _, pool := range pools {
				ret, err := pool.RPopLPush(ctx, sourceKey, destKey).Result()
				Expect(err).To(Equal(redis.Nil))
				Expect(ret).To(Equal(""))
				pool.LPush(ctx, sourceKey, "hello")
				ret, err = pool.RPopLPush(ctx, sourceKey, destKey).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal("hello"))
			}
		})

		It("sadd", func() {
			key := "sadd_key"
			members := []string{"sadd_member1", "sadd_member2", "sadd_member3"}
			for _, pool := range pools {
				ret, err := pool.SAdd(ctx, key, members).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				Expect(pool.SMembers(ctx, key).Val()).To(ContainElements("sadd_member1", "sadd_member2", "sadd_member3"))
			}
		})

		It("scard", func() {
			key := "scard_key"
			members := []string{"scard_member1", "scard_member2", "scard_member3"}
			for _, pool := range pools {
				ret, err := pool.SAdd(ctx, key, members).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				ret, err = pool.SCard(ctx, key).Result()
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
				Expect(pool.SAdd(ctx, key1, members1).Val()).To(Equal(int64(2)))
				Expect(pool.SAdd(ctx, key2, members2).Val()).To(Equal(int64(2)))
				ret, err := pool.SDiff(ctx, key1, key2).Result()
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
				Expect(pool.SAdd(ctx, key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SDiffStore(ctx, destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				Expect(pool.SMembers(ctx, destination).Val()).To(ContainElements("sdiffstore_member1"))
			}
		})

		It("sinter", func() {
			key1 := "sinter_key1"
			members1 := []string{"sinter_member1", "sinter_member2"}
			key2 := "sinter_key2"
			members2 := []string{"sinter_member2", "sinter_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SInter(ctx, key1, key2).Result()
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
				Expect(pool.SAdd(ctx, key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SInterStore(ctx, destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				Expect(pool.SMembers(ctx, destination).Val()).To(ContainElements("sinterstore_member2"))
			}
		})

		It("sismember", func() {
			key := "sismember_key"
			members := []string{"sismember_member1", "sismember_member2", "sismember_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SIsMember(ctx, key, "sismember_member1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(true))
				ret, err = pool.SIsMember(ctx, key, "sismember_member4").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(false))
			}
		})

		It("smembers", func() {
			key := "smembers_key"
			members := []string{"smembers_member1", "smembers_member2", "smembers_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMembers(ctx, key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(ContainElements("smembers_member1", "smembers_member2", "smembers_member3"))
			}
		})

		It("smembersmap", func() {
			key := "smembersmap_key"
			members := []string{"smembersmap_member1", "smembersmap_member2", "smembersmap_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMembersMap(ctx, key).Result()
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
				Expect(pool.SAdd(ctx, source, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, destination, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SMove(ctx, source, destination, "smove_member1").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(true))
				Expect(pool.SMembers(ctx, destination).Val()).To(ContainElements("smove_member1"))
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
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SPop(ctx, key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(memberMap[ret]).To(Equal(true))
				delete(memberMap, ret)
				for member := range memberMap {
					Expect(pool.SMembers(ctx, key).Val()).To(ContainElements(member))
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
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				rets, err := pool.SPopN(ctx, key, 2).Result()
				Expect(err).NotTo(HaveOccurred())
				for _, ret := range rets {
					Expect(memberMap[ret]).To(Equal(true))
					delete(memberMap, ret)
				}
				for member := range memberMap {
					Expect(pool.SMembers(ctx, key).Val()).To(ContainElements(member))
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
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SRandMember(ctx, key).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(memberMap[ret]).To(Equal(true))
				Expect(pool.SMembers(ctx, key).Val()).To(ContainElements("srandmember_member1", "srandmember_member2", "srandmember_member3"))
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
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				rets, err := pool.SRandMemberN(ctx, key, 2).Result()
				Expect(err).NotTo(HaveOccurred())
				for _, ret := range rets {
					Expect(memberMap[ret]).To(Equal(true))
				}
				Expect(pool.SMembers(ctx, key).Val()).To(ContainElements("srandmembern_member1", "srandmembern_member2", "srandmembern_member3"))
			}
		})

		It("srem", func() {
			key := "srem_key"
			members := []string{"srem_member1", "srem_member2", "srem_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key, members).Err()).NotTo(HaveOccurred())
				ret, err := pool.SRem(ctx, key, "srem_member1", "srem_member2").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(2)))
				Expect(pool.SMembers(ctx, key).Val()).NotTo(ContainElements("srem_member1", "srem_member2"))
			}
		})

		It("sunion", func() {
			key1 := "sunion_key1"
			members1 := []string{"sunion_member1", "sunion_member2"}
			key2 := "sunion_key2"
			members2 := []string{"sunion_member2", "sunion_member3"}
			for _, pool := range pools {
				Expect(pool.SAdd(ctx, key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SUnion(ctx, key1, key2).Result()
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
				Expect(pool.SAdd(ctx, key1, members1).Err()).NotTo(HaveOccurred())
				Expect(pool.SAdd(ctx, key2, members2).Err()).NotTo(HaveOccurred())
				ret, err := pool.SUnionStore(ctx, destination, key1, key2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(3)))
				Expect(pool.SMembers(ctx, destination).Val()).To(ContainElements("sunionstore_member1", "sunionstore_member2", "sunionstore_member3"))
				_, _ = pool.Del(ctx, destination)
			}
		})

		It("geoadd", func() {
			key := "geoadd_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				ret, err := pool.GeoAdd(ctx, key, geo1, geo2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(2)))
				_, _ = pool.Del(ctx, key)
			}
		})

		It("geopos", func() {
			key := "geopos_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoPos(ctx, key, geo2.Name, geo1.Name).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(math.Abs(ret[0].Longitude - geo2.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(ret[0].Latitude - geo2.Latitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(ret[1].Longitude - geo1.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(ret[1].Latitude - geo1.Latitude)).To(BeNumerically("<=", 0.00001))
			}
		})

		It("georadius", func() {
			key := "georadius_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoRadius(ctx, key, geo1.Longitude, geo1.Latitude, &redis.GeoRadiusQuery{
					Radius:    10,
					Unit:      "km",
					WithCoord: true,
					WithDist:  true,
					Count:     10,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ret)).To(Equal(1))
				Expect(ret[0].Name).To(Equal(geo1.Name))
				Expect(math.Abs(ret[0].Longitude - geo1.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(ret[0].Latitude - geo1.Latitude)).To(BeNumerically("<=", 0.00001))
			}
		})

		It("georadiusstore", func() {
			key := "georadiusstore_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			storeKey := "georadiusstore_store_dest"
			storeDistKey := "georadiusstore_storedist_dest"
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoRadiusStore(ctx, key, geo1.Longitude, geo1.Latitude, &redis.GeoRadiusQuery{
					Radius: 10,
					Unit:   "km",
					Count:  10,
					Store:  storeKey,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				pos, err := pool.GeoPos(ctx, storeKey, geo1.Name).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(pos)).To(Equal(1))
				Expect(math.Abs(pos[0].Longitude - geo1.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(pos[0].Latitude - geo1.Latitude)).To(BeNumerically("<=", 0.00001))

				ret, err = pool.GeoRadiusStore(ctx, key, geo1.Longitude, geo1.Latitude, &redis.GeoRadiusQuery{
					Radius:    10,
					Unit:      "km",
					Count:     10,
					StoreDist: storeDistKey,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				dist, err := pool.ZRangeWithScores(ctx, storeDistKey, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(dist)).To(Equal(1))
				Expect(dist[0].Member).To(Equal(geo1.Name))
				Expect(dist[0].Score).To(BeNumerically("<=", 0.01))
			}
		})

		It("georadiusbymember", func() {
			key := "georadiusbymember_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoRadiusByMember(ctx, key, geo1.Name, &redis.GeoRadiusQuery{
					Radius:    10,
					Unit:      "km",
					WithCoord: true,
					WithDist:  true,
					Count:     10,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ret)).To(Equal(1))
				Expect(ret[0].Name).To(Equal(geo1.Name))
				Expect(math.Abs(ret[0].Longitude - geo1.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(ret[0].Latitude - geo1.Latitude)).To(BeNumerically("<=", 0.00001))
			}
		})

		It("georadiusbymemberstore", func() {
			key := "georadiusbymemberstore_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			storeKey := "georadiusbymemberstore_store_key"
			storeDistKey := "georadiusbymemberstore_storedist_key"
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoRadiusByMemberStore(ctx, key, geo1.Name, &redis.GeoRadiusQuery{
					Radius: 10,
					Unit:   "km",
					Count:  10,
					Store:  storeKey,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				pos, err := pool.GeoPos(ctx, storeKey, geo1.Name).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(pos)).To(Equal(1))
				Expect(math.Abs(pos[0].Longitude - geo1.Longitude)).To(BeNumerically("<=", 0.00001))
				Expect(math.Abs(pos[0].Latitude - geo1.Latitude)).To(BeNumerically("<=", 0.00001))

				ret, err = pool.GeoRadiusByMemberStore(ctx, key, geo1.Name, &redis.GeoRadiusQuery{
					Radius:    10,
					Unit:      "km",
					Count:     10,
					StoreDist: storeDistKey,
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(Equal(int64(1)))
				dist, err := pool.ZRangeWithScores(ctx, storeDistKey, 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(dist)).To(Equal(1))
				Expect(dist[0].Member).To(Equal(geo1.Name))
				Expect(dist[0].Score).To(BeNumerically("<=", 0.01))
			}
		})

		It("geodist", func() {
			key := "geodist_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoDist(ctx, key, geo1.Name, geo2.Name, "km").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ret).To(BeNumerically(">=", 1000))
			}
		})

		It("geohash", func() {
			key := "geohash_key"
			geo1 := &redis.GeoLocation{
				Name:      "Beijing",
				Longitude: 116.405285,
				Latitude:  39.904989,
			}
			geo2 := &redis.GeoLocation{
				Name:      "Shanghai",
				Longitude: 121.472644,
				Latitude:  31.231706,
			}
			for _, pool := range pools {
				Expect(pool.GeoAdd(ctx, key, geo1, geo2).Err()).NotTo(HaveOccurred())
				ret, err := pool.GeoHash(ctx, key, geo1.Name, geo2.Name).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(len(ret)).To(Equal(2))
				Expect([]byte(ret[0])[0]).To(Equal([]byte(ret[1])[0]))
			}
		})
	})

	Describe("ZSet Commands", func() {
		It("ZAdd/ZScore", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(1)))
			}
		})

		It("ZAddXX", func() {
			for _, pool := range pools {
				_, err := pool.ZAddXX(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZScore(ctx, "key", "one").Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZAddNX", func() {
			for _, pool := range pools {
				_, err := pool.ZAddNX(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAddNX(ctx, "key", redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(1)))
			}
		})

		It("ZAddGT", func() {
			for _, pool := range pools {
				_, err := pool.ZAddGT(ctx, "key", redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAddGT(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(2)))

				_, err = pool.ZAddGT(ctx, "key", redis.Z{
					Score:  3,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err = pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZAddLT", func() {
			for _, pool := range pools {
				_, err := pool.ZAddLT(ctx, "key", redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAddLT(ctx, "key", redis.Z{
					Score:  3,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(2)))

				_, err = pool.ZAddLT(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err = pool.ZScore(ctx, "key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(1)))
			}
		})

		It("ZAddCh", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				ch, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  2,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ch).To(Equal(int64(1)))
			}
		})

		It("ZIncr", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncr(ctx, "key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZIncrNX", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncrNX(ctx, "key", &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(2)))

				_, err = pool.ZIncrNX(ctx, "key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZIncrXX", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZIncrXX(ctx, "key", &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).To(Equal(redis.Nil))

				score, err := pool.ZIncrXX(ctx, "key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZIncrBy", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncrBy(ctx, "key", 2, "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZCard", func() {
			for _, pool := range pools {
				card, err := pool.ZCard(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(card).To(Equal(int64(0)))

				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				card, err = pool.ZCard(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(card).To(Equal(int64(1)))
			}
		})

		It("ZCount", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZCount(ctx, "key", "-inf", "+inf").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))

				count, err = pool.ZCount(ctx, "key", "(1", "3").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))
			}
		})

		It("ZLexCount", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  0,
					Member: "a",
				}, redis.Z{
					Score:  0,
					Member: "b",
				}, redis.Z{
					Score:  0,
					Member: "b",
				}, redis.Z{
					Score:  0,
					Member: "c",
				}, redis.Z{
					Score:  0,
					Member: "d",
				}, redis.Z{
					Score:  0,
					Member: "e",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZLexCount(ctx, "key", "-", "+").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(5)))

				count, err = pool.ZLexCount(ctx, "key", "[b", "[d").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))
			}
		})

		It("ZPopMax", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				z, err := pool.ZPopMax(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{Score: 3, Member: "three"}}))

				z, err = pool.ZPopMax(ctx, "key", 1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))
			}
		})

		It("ZPopMin", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				z, err := pool.ZPopMin(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{Score: 1, Member: "one"}}))

				z, err = pool.ZPopMin(ctx, "key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{Score: 2, Member: "two"}}))
			}
		})

		It("ZRange", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRange(ctx, "key", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"one", "two", "three"}))

				l, err = pool.ZRange(ctx, "key", 2, 3).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"three"}))

				l, err = pool.ZRange(ctx, "key", -2, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"two", "three"}))
			}
		})

		It("ZRangeWithScores", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "uno",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeWithScores(ctx, "key", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l[0]).To(Equal(redis.Z{Score: 1, Member: "one"}))
				Expect(l[1]).To(Equal(redis.Z{Score: 1, Member: "uno"}))
				Expect(l[2]).To(Equal(redis.Z{Score: 2, Member: "two"}))
				Expect(l[3]).To(Equal(redis.Z{Score: 3, Member: "three"}))
			}
		})

		It("ZRangeByScore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeByScore(ctx, "key", &redis.ZRangeBy{
					Min: "-inf",
					Max: "+inf",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"one", "two", "three"}))

				l, err = pool.ZRangeByScore(ctx, "key", &redis.ZRangeBy{
					Min: "(1",
					Max: "2",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"two"}))
			}
		})

		It("ZRangeByLex", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  0,
					Member: "a",
				}, redis.Z{
					Score:  0,
					Member: "b",
				}, redis.Z{
					Score:  0,
					Member: "c",
				}, redis.Z{
					Score:  0,
					Member: "d",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeByLex(ctx, "key", &redis.ZRangeBy{
					Min: "-",
					Max: "[c",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"a", "b", "c"}))

				l, err = pool.ZRangeByLex(ctx, "key", &redis.ZRangeBy{
					Min: "[aaa",
					Max: "(d",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"b", "c"}))
			}
		})

		It("ZRank", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				rank, err := pool.ZRank(ctx, "key", "two").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(rank).To(Equal(int64(1)))
			}
		})

		It("ZRem", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err := pool.ZRem(ctx, "key", "two").Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZRank(ctx, "key", "two").Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZRemRangeByRank", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByRank(ctx, "key", 1, 2).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))
			}
		})

		It("ZRemRangeByScore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByScore(ctx, "key", "1", "1.9").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(1)))
			}
		})

		It("ZRemRangeByLex", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "key", redis.Z{
					Score:  1,
					Member: "a",
				}, redis.Z{
					Score:  2,
					Member: "b",
				}, redis.Z{
					Score:  3,
					Member: "c",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByLex(ctx, "key", "(a", "(c").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(1)))
			}
		})

		It("ZInterStore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "set1", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAdd(ctx, "set2", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZInterStore(ctx, "out", &redis.ZStore{
					Keys:    []string{"set1", "set2"},
					Weights: []float64{2, 3},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))

				out, err := pool.ZRangeWithScores(ctx, "out", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(out).To(Equal([]redis.Z{
					{Score: 5, Member: "one"},
					{Score: 10, Member: "two"},
				}))
			}
		})

		It("ZInterStore-Cross-Shard", func() {
			pool := shardPool
			set1 := "aset"
			set2 := "iset"
			_, err = pool.ZAdd(ctx, set1, redis.Z{
				Score:  1,
				Member: "one",
			}, redis.Z{
				Score:  2,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = pool.ZAdd(ctx, set2, redis.Z{
				Score:  1,
				Member: "one",
			}, redis.Z{
				Score:  2,
				Member: "two",
			}, redis.Z{
				Score:  3,
				Member: "three",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err := pool.ZInterStore(ctx, "out", &redis.ZStore{
				Keys:    []string{set1, set2},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).To(Equal(errCrossMultiShards))
		})

		It("ZUnionStore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd(ctx, "set1", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAdd(ctx, "set2", redis.Z{
					Score:  1,
					Member: "one",
				}, redis.Z{
					Score:  2,
					Member: "two",
				}, redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZUnionStore(ctx, "out", &redis.ZStore{
					Keys:    []string{"set1", "set2"},
					Weights: []float64{2, 3},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))

				out, err := pool.ZRangeWithScores(ctx, "out", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(out).To(Equal([]redis.Z{
					{Score: 5, Member: "one"},
					{Score: 9, Member: "three"},
					{Score: 10, Member: "two"},
				}))
			}
		})

		It("ZUnionStore-Cross-Shard", func() {
			pool := shardPool
			set1 := "aset"
			set2 := "iset"
			_, err = pool.ZAdd(ctx, set1, redis.Z{
				Score:  1,
				Member: "one",
			}, redis.Z{
				Score:  2,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = pool.ZAdd(ctx, set2, redis.Z{
				Score:  1,
				Member: "one",
			}, redis.Z{
				Score:  2,
				Member: "two",
			}, redis.Z{
				Score:  3,
				Member: "three",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err := pool.ZUnionStore(ctx, "out", &redis.ZStore{
				Keys:    []string{set1, set2},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).To(Equal(errCrossMultiShards))
		})

		It("flushdb", func() {
			keys := []string{"a3", "b3", "c3", "d3"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, key, 0).Err()).NotTo(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				nBefore, err := pool.Exists(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(nBefore)).To(Equal(len(keys)))
				Expect(pool.FlushDB(ctx).Err()).NotTo(HaveOccurred())
				nAfter, err := pool.Exists(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(nAfter)).To(Equal(0))
			}
		})

		It("flushdbasync", func() {
			keys := []string{"a3", "b3", "c3", "d3"}
			for _, pool := range pools {
				for _, key := range keys {
					Expect(pool.Set(ctx, key, key, 0).Err()).NotTo(HaveOccurred())
				}
				time.Sleep(10 * time.Millisecond)
				nBefore, err := pool.Exists(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(nBefore)).To(Equal(len(keys)))
				Expect(pool.FlushDBAsync(ctx).Err()).NotTo(HaveOccurred())
				time.Sleep(100 * time.Millisecond)
				nAfter, err := pool.Exists(ctx, keys...)
				Expect(err).NotTo(HaveOccurred())
				Expect(int(nAfter)).To(Equal(0))
			}
		})

		It("GetMasterShards", func() {
			var (
				shards []*redis.Client
				err    error
			)

			shards, err = haPool.GetMasterShards()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(shards)).To(Equal(1))

			shards, err = shardPool.GetMasterShards()
			Expect(err).NotTo(HaveOccurred())
			Expect(len(shards)).To(Equal(2))
		})
	})
})

var _ = Describe("Pool_GD", func() {
	var shardPool *Pool
	var err error
	var pools []*Pool

	ctx := context.Background()

	BeforeEach(func() {
		haConfig := &HAConfig{
			Master: "127.0.0.1:8379",
		}
		haConfig1 := &HAConfig{
			Master: "127.0.0.1:8384",
		}

		shardPool, err = NewShard(&ShardConfig{
			Shards: []*HAConfig{
				haConfig,
				haConfig1,
			},
		})
		Expect(err).NotTo(HaveOccurred())
		shards := shardPool.connFactory.(*ShardConnFactory).shards
		for _, shard := range shards {
			master, _ := shard.getMasterConn()
			if master.Options().Addr == "127.0.0.1:8384" {
				master.Shutdown(ctx)
			} else {
				Expect(master.FlushDB(ctx).Err()).NotTo(HaveOccurred())
			}
		}
		pools = []*Pool{shardPool}
	})

	AfterEach(func() {
		shardPool.Close()
	})

	Describe("Commands", func() {
		It("ping", func() {
			for _, pool := range pools {
				_, err := pool.Ping(ctx).Result()
				Expect(err).To(HaveOccurred())
			}
		})

		It("gd", func() {
			kvs := []string{"a3", "a3", "b3", "b3", "c3", "c3", "d3", "d3"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				statuses := pool.MSetWithGD(ctx, kvs)
				time.Sleep(10 * time.Millisecond)
				sort.Slice(statuses, func(i, j int) bool {
					return statuses[i].Err() == nil
				})
				Expect(statuses[0].Err()).NotTo(HaveOccurred())
				Expect(statuses[1].Err()).To(HaveOccurred())
				mgetKeys := append(keys, "e3")
				vals, keyErrors := pool.MGetWithGD(ctx, mgetKeys...)
				time.Sleep(10 * time.Millisecond)
				Expect(vals).To(Equal([]interface{}{nil, "b3", nil, "d3", nil}))
				Expect(keyErrors).Should(HaveKey("a3"))
				Expect(keyErrors).Should(HaveKey("c3"))
				Expect(keyErrors).Should(HaveKey("e3"))
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("mexpire", func() {
			kvs := []string{"a3", "a3", "b3", "b3", "c3", "c3", "d3", "d3"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				statuses := pool.MSetWithGD(ctx, kvs)
				time.Sleep(10 * time.Millisecond)
				sort.Slice(statuses, func(i, j int) bool {
					return statuses[i].Err() == nil
				})
				Expect(statuses[0].Err()).NotTo(HaveOccurred())
				Expect(statuses[1].Err()).To(HaveOccurred())
				keyErrors := pool.MExpire(ctx, 5*time.Minute, keys...)
				time.Sleep(10 * time.Millisecond)
				Expect(keyErrors).Should(HaveKey("a3"))
				Expect(keyErrors).Should(HaveKey("c3"))
				Expect(pool.TTL(ctx, "b3").Val()).NotTo(Equal(-1))
				Expect(pool.TTL(ctx, "d3").Val()).NotTo(Equal(-1))
				_, _ = pool.Del(ctx, keys...)
			}
		})

		It("mexpireat", func() {
			kvs := []string{"a3", "a3", "b3", "b3", "c3", "c3", "d3", "d3"}
			keys := make([]string, 0)
			for i := 0; i < len(kvs); i += 2 {
				keys = append(keys, kvs[i])
			}
			for _, pool := range pools {
				statuses := pool.MSetWithGD(ctx, kvs)
				time.Sleep(10 * time.Millisecond)
				sort.Slice(statuses, func(i, j int) bool {
					return statuses[i].Err() == nil
				})
				Expect(statuses[0].Err()).NotTo(HaveOccurred())
				Expect(statuses[1].Err()).To(HaveOccurred())
				keyErrors := pool.MExpireAt(ctx, time.Now().Add(5*time.Minute), keys...)
				time.Sleep(10 * time.Millisecond)
				Expect(keyErrors).Should(HaveKey("a3"))
				Expect(keyErrors).Should(HaveKey("c3"))
				Expect(pool.TTL(ctx, "b3").Val()).NotTo(Equal(-1))
				Expect(pool.TTL(ctx, "d3").Val()).NotTo(Equal(-1))
				_, _ = pool.Del(ctx, keys...)
			}
		})
	})
})

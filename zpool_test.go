package pool

import (
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

	Describe("ZSet Commands", func() {

		It("ZAdd/ZScore", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore("key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(1)))
			}
		})

		It("ZAddXX", func() {
			for _, pool := range pools {
				_, err := pool.ZAddXX("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZScore("key", "one").Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZAddNX", func() {
			for _, pool := range pools {
				_, err := pool.ZAddNX("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAddNX("key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZScore("key", "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(1)))
			}
		})

		It("ZAddCh", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				ch, err := pool.ZAdd("key", &redis.Z{
					Score:  2,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(ch).To(Equal(int64(1)))
			}
		})

		It("ZIncr", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncr("key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZIncrNX", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncrNX("key", &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(2)))

				_, err = pool.ZIncrNX("key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZIncrXX", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZIncrXX("key", &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).To(Equal(redis.Nil))

				score, err := pool.ZIncrXX("key", &redis.Z{
					Score:  2,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZIncrBy", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				score, err := pool.ZIncrBy("key", 2, "one").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(score).To(Equal(float64(3)))
			}
		})

		It("ZCard", func() {
			for _, pool := range pools {
				card, err := pool.ZCard("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(card).To(Equal(int64(0)))

				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				card, err = pool.ZCard("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(card).To(Equal(int64(1)))
			}
		})

		It("ZCount", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZCount("key", "-inf", "+inf").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))

				count, err = pool.ZCount("key", "(1", "3").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))
			}
		})

		It("ZLexCount", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  0,
					Member: "a",
				}, &redis.Z{
					Score:  0,
					Member: "b",
				}, &redis.Z{
					Score:  0,
					Member: "b",
				}, &redis.Z{
					Score:  0,
					Member: "c",
				}, &redis.Z{
					Score:  0,
					Member: "d",
				}, &redis.Z{
					Score:  0,
					Member: "e",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZLexCount("key", "-", "+").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(5)))

				count, err = pool.ZLexCount("key", "[b", "[d").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))
			}
		})

		It("ZPopMax", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				z, err := pool.ZPopMax("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{3, "three"}}))

				z, err = pool.ZPopMax("key", 1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{2, "two"}}))
			}
		})

		It("ZPopMin", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				z, err := pool.ZPopMin("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{1, "one"}}))

				z, err = pool.ZPopMin("key").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(z).To(Equal([]redis.Z{{2, "two"}}))
			}
		})

		It("ZRange", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRange("key", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"one", "two", "three"}))

				l, err = pool.ZRange("key", 2, 3).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"three"}))

				l, err = pool.ZRange("key", -2, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"two", "three"}))
			}
		})

		It("ZRangeWithScores", func() {
			for _, pool := range pools {
				_, err := pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "uno",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeWithScores("key", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l[0]).To(Equal(redis.Z{1, "one"}))
				Expect(l[1]).To(Equal(redis.Z{1, "uno"}))
				Expect(l[2]).To(Equal(redis.Z{2, "two"}))
				Expect(l[3]).To(Equal(redis.Z{3, "three"}))
			}
		})

		It("ZRangeByScore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeByScore("key", &redis.ZRangeBy{
					Min: "-inf",
					Max: "+inf",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"one", "two", "three"}))

				l, err = pool.ZRangeByScore("key", &redis.ZRangeBy{
					Min: "(1",
					Max: "2",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"two"}))
			}
		})

		It("ZRangeByLex", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  0,
					Member: "a",
				}, &redis.Z{
					Score:  0,
					Member: "b",
				}, &redis.Z{
					Score:  0,
					Member: "c",
				}, &redis.Z{
					Score:  0,
					Member: "d",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				l, err := pool.ZRangeByLex("key", &redis.ZRangeBy{
					Min: "-",
					Max: "[c",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"a", "b", "c"}))

				l, err = pool.ZRangeByLex("key", &redis.ZRangeBy{
					Min: "[aaa",
					Max: "(d",
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(l).To(Equal([]string{"b", "c"}))
			}
		})

		It("ZRank", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				rank, err := pool.ZRank("key", "two").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(rank).To(Equal(int64(1)))
			}
		})

		It("ZRem", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err := pool.ZRem("key", "two").Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZRank("key", "two").Result()
				Expect(err).To(Equal(redis.Nil))
			}
		})

		It("ZRemRangeByRank", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByRank("key", 1, 2, ).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))
			}
		})

		It("ZRemRangeByScore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByScore("key", "1", "1.9").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(1)))
			}
		})

		It("ZRemRangeByLex", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("key", &redis.Z{
					Score:  1,
					Member: "a",
				}, &redis.Z{
					Score:  2,
					Member: "b",
				}, &redis.Z{
					Score:  3,
					Member: "c",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZRemRangeByLex("key", "(a", "(c").Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(1)))
			}
		})

		It("ZInterStore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("set1", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAdd("set2", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZInterStore("out", &redis.ZStore{
					Keys:    []string{"set1", "set2"},
					Weights: []float64{2, 3},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(2)))

				out, err := pool.ZRangeWithScores("out", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(out).To(Equal([]redis.Z{{5, "one"}, {10, "two"}}))
			}
		})

		It("ZInterStore-Cross-Shard", func() {
			pool := shardPool
			set1 := "aset"
			set2 := "iset"
			_, err = pool.ZAdd(set1, &redis.Z{
				Score:  1,
				Member: "one",
			}, &redis.Z{
				Score:  2,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = pool.ZAdd(set2, &redis.Z{
				Score:  1,
				Member: "one",
			}, &redis.Z{
				Score:  2,
				Member: "two",
			}, &redis.Z{
				Score:  3,
				Member: "three",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err := pool.ZInterStore("out", &redis.ZStore{
				Keys:    []string{set1, set2},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).To(Equal(errCrossMultiShards))
		})

		It("ZUnionStore", func() {
			for _, pool := range pools {
				_, err = pool.ZAdd("set1", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				_, err = pool.ZAdd("set2", &redis.Z{
					Score:  1,
					Member: "one",
				}, &redis.Z{
					Score:  2,
					Member: "two",
				}, &redis.Z{
					Score:  3,
					Member: "three",
				}).Result()
				Expect(err).NotTo(HaveOccurred())

				count, err := pool.ZUnionStore("out", &redis.ZStore{
					Keys:    []string{"set1", "set2"},
					Weights: []float64{2, 3},
				}).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(count).To(Equal(int64(3)))

				out, err := pool.ZRangeWithScores("out", 0, -1).Result()
				Expect(err).NotTo(HaveOccurred())
				Expect(out).To(Equal([]redis.Z{{5, "one"}, {9, "three"}, {10, "two"}}))
			}
		})

		It("ZUnionStore-Cross-Shard", func() {
			pool := shardPool
			set1 := "aset"
			set2 := "iset"
			_, err = pool.ZAdd(set1, &redis.Z{
				Score:  1,
				Member: "one",
			}, &redis.Z{
				Score:  2,
				Member: "two",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err = pool.ZAdd(set2, &redis.Z{
				Score:  1,
				Member: "one",
			}, &redis.Z{
				Score:  2,
				Member: "two",
			}, &redis.Z{
				Score:  3,
				Member: "three",
			}).Result()
			Expect(err).NotTo(HaveOccurred())

			_, err := pool.ZUnionStore("out", &redis.ZStore{
				Keys:    []string{set1, set2},
				Weights: []float64{2, 3},
			}).Result()
			Expect(err).To(Equal(errCrossMultiShards))
		})
	})
})

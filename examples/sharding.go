package main

import (
	"context"
	"fmt"

	pool "github.com/bitleak/go-redis-pool"
	"github.com/bitleak/go-redis-pool/hashkit"
)

func main() {
	ctx := context.Background()
	p, err := pool.NewShard(&pool.ShardConfig{
		Shards: []*pool.HAConfig{
			// shard 1
			{
				Master: "127.0.0.1:6379",
				Slaves: []string{
					"127.0.0.1:6380",
					"127.0.0.1:6381",
				},
				Password:         "", // set master password
				ReadonlyPassword: "", // use password if no set
			},
			// shard 2
			{
				Master: "127.0.0.1:6382",
				Slaves: []string{
					"127.0.0.1:6383",
				},
				Password:         "", // set master password
				ReadonlyPassword: "", // use password if no set
			},
		},
		// better distribution
		DistributeType: pool.DistributeByModular,
		HashFn:         hashkit.Xxh3,
	})
	if err != nil {
		fmt.Println(err)
	}
	p.Set(ctx, "foo", "bar", 0)
	fmt.Println(p.Get(ctx, "pool"))
}

package main

import (
	"fmt"

	pool "github.com/bitleak/go-redis-pool"
)

func main() {
	pool, err := pool.NewShard(&pool.ShardConfig{
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
	})
	if err != nil {
		// log the error
	}
	pool.Set("foo", "bar", 0)
	fmt.Println(pool.Get("pool"))
}

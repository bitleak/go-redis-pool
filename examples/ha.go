package main

import (
	"fmt"
	"time"

	pool "github.com/bitleak/go-redis-pool"
)

func main() {
	pool, err := pool.NewHA(&pool.HAConfig{
		Master: "127.0.0.1:6379",
		Slaves: []string{
			"127.0.0.1:6379",
			"127.0.0.1:6380",
			"127.0.0.1:6381",
		},

		// optional
		AutoEjectHost:      true,
		ServerFailureLimit: 3,
		ServerRetryTimeout: 5 * time.Second,
		MinServerNum:       2,
	})
	if err != nil {
		// log the error
	}
	pool.Set("foo", "bar", 0)
	fmt.Println(pool.Get("pool"))
}

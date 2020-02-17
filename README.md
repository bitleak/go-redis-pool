# go-redis-pool

go-redis-pool was designed to implement the read/write split in Redis master-slave mode, and easy way to sharding the data.

## Installation

## Quick Start

### Setup The Master-Slave Pool

```go
pool, err := pool.NewHA(&pool.HAConfig{
        Master: "127.0.0.1:6379",
        Slaves: []string{
            "127.0.0.1:6380",
            "127.0.0.1:6381",
        },
        Password: "", // set master password
        ReadonlyPassword: "", // use password if no set
})

pool.Set("foo", "bar", 0)
```

The read-only commands would go throught slaves, and write commands would into the master instance.

#### Setup The Sharding Pool

```go

pool, err := pool.NewShard(&pool.ShardConfig{
    Shards: []*HAConfig {

        // shard 1
        &pool.HAConfig{
            Master: "127.0.0.1:6379",
            Slaves: []string{
                "127.0.0.1:6380",
                "127.0.0.1:6381",
            },
            Password: "", // set master password
            ReadonlyPassword: "", // use password if no set
        },

        // shard 2
        &pool.HAConfig{
            Master: "127.0.0.1:6382",
            Slaves: []string{
                "127.0.0.1:6383",
            },
            Password: "", // set master password
            ReadonlyPassword: "", // use password if no set
        },
    }
})

pool.Set("foo", "bar", 0)
```

## How To

## Test

```shell
$ make test
```

## See Also

[https://github.com/go-redis/redis](https://github.com/go-redis/redis)

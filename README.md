# go-redis-pool

go-redis-pool was designed to implement the read/write split in Redis master-slave mode, and easy way to sharding the data.

## Installation

go-redis-pool requires a Go version with [Modules](https://github.com/golang/go/wiki/Modules) support and uses import versioning. So please make sure to initialize a Go module before installing go-redis-pool:

```shell
go mod init github.com/my/repo
go get github.com/meitu/go-redis-pool
```

## Quick Start

API documentation and examples are available via [godoc](https://godoc.org/github.com/meitu/go-redis-pool)

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

The read-only commands would go throught slaves and write commands would into the master instance. We use the Round-Robin as default when determing which slave to serve the readonly request, and currently supports:

* RoundRobin (default)
* Random
* Weight

For example, we change the distribution type to `Weight`:

```go
pool, err := pool.NewHA(&pool.HAConfig{
        Master: "127.0.0.1:6379",
        Slaves: []string{
            "127.0.0.1:6380",  // default weight is 100 if missing
            "127.0.0.1:6381:200", // weight is 200
            "127.0.0.1:6382:300", // weigght is 300
        },
        PollType: pool.PollByWeight,
})
```

The first slave would serve 1/6 reqeusts, and second slave would serve 2/6, last one would serve 3/6. 

##### Auto Eject The Failure Host 

```
pool, err := pool.NewHA(&pool.HAConfig{
        Master: "127.0.0.1:6379",
        Slaves: []string{
            "127.0.0.1:6380",  // default weight is 100 if missing
            "127.0.0.1:6381:200", // weight is 200
            "127.0.0.1:6382:300", // weigght is 300
        },

        AutoEjectHost: true,
        ServerFailureLimit: 3,
        ServerRetryTimeout: 5 * time.Second,
        MinServerNum: 2,
})
```

The pool would evict the host if reached `ServerFailureLimit` times of failure and retry the host after `ServerRetryTimeout`. The
`MinServerNum` was used to avoid evicting too many and would overrun other alive servers. 

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
    },
})

pool.Set("foo", "bar", 0)

```

Shard pool use the `CRC32` as default hash function when sharding the key, you can overwrite the `HashFn` in config if wants to use other sharding hash function. The distribution type supports `ketama` and `modular`, default is modular.

## Test

```shell
$ make test
```

## See Also

[https://github.com/go-redis/redis](https://github.com/go-redis/redis)

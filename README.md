# go-redis-pool
[![Build Status](https://travis-ci.org/bitleak/go-redis-pool.svg?branch=master)](https://travis-ci.org/bitleak/go-redis-pool) [![Go Report Card](https://goreportcard.com/badge/github.com/bitleak/go-redis-pool)](https://goreportcard.com/report/github.com/bitleak/go-redis-pool) [![Coverage Status](https://coveralls.io/repos/github/bitleak/go-redis-pool/badge.svg?branch=master)](https://coveralls.io/github/bitleak/go-redis-pool?branch=master) [![GitHub release](https://img.shields.io/github/tag/bitleak/go-redis-pool.svg?label=release)](https://github.com/bitleak/go-redis-pool/releases) [![GitHub release date](https://img.shields.io/github/release-date/bitleak/go-redis-pool.svg)](https://github.com/bitleak/go-redis-pool/releases) [![LICENSE](https://img.shields.io/github/license/bitleak/go-redis-pool.svg)](https://github.com/bitleak/go-redis-pool/blob/master/LICENSE) [![GoDoc](https://img.shields.io/badge/Godoc-reference-blue.svg)](https://godoc.org/github.com/bitleak/go-redis-pool)


go-redis-pool was designed to implement the read/write split in Redis master-slave mode, and easy way to sharding the data.

## Installation

go-redis-pool requires a Go version with [Modules](https://github.com/golang/go/wiki/Modules) support and uses import versioning. So please make sure to initialize a Go module before installing go-redis-pool:

```shell
go mod init github.com/my/repo
go get github.com/bitleak/go-redis-pool
```

## Quick Start

API documentation and examples are available via [godoc](https://godoc.org/github.com/bitleak/go-redis-pool)

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

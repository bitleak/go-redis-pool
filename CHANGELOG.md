## v3.3.2 (2025-14-04)

- Add GetMasterShards method

## v3.3.1 (2024-22-07)

- Add ExpireNX, ExpireXX, ExpireGT, ExpireLT methods

## v3.3.0 (2024-01-17)

- Rollback to stable version go-redis v9.0.3
- Add FlushDB/FlushDBAsync methods

## v3.2.2 (2023-12-06)

- Add SetArgs method

## v3.2.1 (2023-12-04)

- Add a couple of variants of ZADD command (ZADD key GT, ZADD key LT)

## v3.2.0 (2023-09-21)

- Update go-redis library to v9.2.0

## v3.1.1 (2023-06-24)

- Fixed connection selection error for HAConnFactory in MGet/MGetWithGD
- Fixed a bug with the handling of the weight value for the slave

## v3.1.0 (2023-06-13)

- Edit optional Hooks to HAConfig

## v3.0.0 (2023-04-17)

- Update go-redis library to v9 (v9.0.3)
- Bug fixes

## v2.2.0 (2023-04-05)

- Adding new commands MExpire/MExpireAt with support for executing multiple keys
- Fixed the Ping command, now the check occurs on all shards and returns the first error for the sharded configuration
- Minor performance improvements

## v2.1.0 (2022-07-12)

- Added getting statistics of all connection pools

## v2.0.1 (2022-06-16)

- Fixed error return value for MGetWithGD

## v2.0.0 (2022-05-19)

- Update go-redis library to v8

- All commands require `context.Context` as a first argument, e.g. `Ping(ctx)`. If you are not
  using `context.Context` yet, the simplest option is to define global package variable
  `var ctx = context.TODO()` and use it when `ctx` is required

- Full support for `context.Context` canceling

## v1.1.0 (2022-05-17)

- Bug fixes
- Adding new commands with graceful degradation support (MGetWithGD, MSetWithGD)

## v1.0.0 (2020-02-26)

- Initial public version

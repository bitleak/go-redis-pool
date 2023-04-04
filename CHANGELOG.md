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
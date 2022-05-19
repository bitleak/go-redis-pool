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
# OpenSrv - MySQL

**Bindings for emulating a MySQL/MariaDB server.**

When developing new databases or caching layers, it can be immensely useful to test your system
using existing applications. However, this often requires significant work modifying
applications to use your database over the existing ones. This crate solves that problem by
acting as a MySQL server, and delegating operations such as querying and query execution to
user-defined logic.

## Usage

To start, implement `AsyncMysqlShim` for your backend, and create a `AsyncMysqlIntermediary` over an
instance of your backend and a connection stream. The appropriate methods will be called on
your backend whenever a client issues a `QUERY`, `PREPARE`, or `EXECUTE` command, and you will
have a chance to respond appropriately. For example, to write a shim that always responds to
all commands with a "no results" reply:

```rust
use std::io;
use tokio::io::AsyncWrite;

use opensrv_mysql::*;
use tokio::net::TcpListener;

struct Backend;

#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;

    async fn on_prepare<'a>(
        &'a mut self,
        _: &'a str,
        info: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        info.reply(42, &[], &[]).await
    }

    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: opensrv_mysql::ParamParser<'a>,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        results.completed(OkResponse::default()).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("execute sql {:?}", sql);
        results.start(&[]).await?.finish().await
    }
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let listener = TcpListener::bind("0.0.0.0:3306").await?;

    loop {
        let (stream, _) = listener.accept().await?;
        let (r, w) = stream.into_split();
        tokio::spawn(async move { AsyncMysqlIntermediary::run_on_buffered(Backend, r, w).await });
    }
}
```

This example can be exected with:

```
cargo run --example=serve_one
```

More examples can be found [here](examples).

For production use, prefer `AsyncMysqlIntermediary::run_on_buffered(...)` or
`run_with_options_buffered(...)` instead of wiring a bare stream directly. The buffered entrypoints
add connection-level read/write buffering around the protocol layer and are more stable with
real clients such as JDBC drivers.

## Authentication and Compatibility

This crate supports plugin-auth handshakes and can be used with both MySQL 5.7-style
`mysql_native_password` and MySQL 8.0-style `caching_sha2_password` clients.

The default shim behavior remains `mysql_native_password`, but a backend can opt into
`caching_sha2_password` per connection or per user by returning
`CACHING_SHA2_PASSWORD` from `default_auth_plugin()` or `auth_plugin_for_username()`.

Helpers are provided to validate client auth responses against a known password:

```rust
use opensrv_mysql::verify_auth_plugin_data;

async fn authenticate(
    &self,
    auth_plugin: &str,
    _username: &[u8],
    salt: &[u8],
    auth_data: &[u8],
) -> bool {
    verify_auth_plugin_data(auth_plugin, b"secret", salt, auth_data)
}
```

Compatibility notes:

- MySQL 5.7 clients typically work with `mysql_native_password`.
- MySQL 8.0 clients can work with either `mysql_native_password` or
  `caching_sha2_password`, depending on what the server advertises.
- If you want clients to identify the server as MySQL 8.0, override `version()` in your shim.

## Getting help

Submit [issues](https://github.com/datafuselabs/opensrv/issues/new/choose) for bug report or asking questions in [discussion](https://github.com/datafuselabs/opensrv/discussions/new?category=q-a).

## Credits

This project is a branch of [jonhoo/msql-srv](https://github.com/jonhoo/msql-srv) and focuses on providing asynchronous support.

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.

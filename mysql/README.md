# OpenSrv - MySQL

基于 [databendlabs/opensrv](https://github.com/databendlabs/opensrv) 修改而来，包含针对真实客户端接入的稳定性修复与兼容性增强。

这个 crate 用于模拟 MySQL/MariaDB 服务端协议。你只需要实现 `AsyncMysqlShim`，就可以把自己的后端能力暴露给 MySQL 客户端，例如 JDBC、Navicat、DBeaver、MySQL CLI 等。

## 主要特性

- 支持文本查询与预处理语句
- 支持 `mysql_native_password`
- 支持 `caching_sha2_password`
- 支持 TLS
- 提供更适合生产使用的 buffered 运行入口

## 基本用法

先为你的后端实现 `AsyncMysqlShim`，然后通过 `AsyncMysqlIntermediary` 处理连接。

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
        tokio::spawn(async move {
            AsyncMysqlIntermediary::run_on_buffered(Backend, r, w).await
        });
    }
}
```

运行示例：

```bash
cargo run --example serve_one
```

更多示例见 [examples](./examples)。

## 生产环境建议

建议优先使用：

- `AsyncMysqlIntermediary::run_on_buffered(...)`
- `AsyncMysqlIntermediary::run_with_options_buffered(...)`

这两个入口会在协议层外增加连接级读写缓冲，在 JDBC 等真实客户端场景下更稳定。

## 认证与兼容性

当前支持：

- MySQL 5.7 常见认证方式 `mysql_native_password`
- MySQL 8.0 常见插件认证 `caching_sha2_password`

默认仍使用 `mysql_native_password`。如果你需要按连接或按用户切换到 `caching_sha2_password`，可以在 shim 中返回 `CACHING_SHA2_PASSWORD`。

可以使用以下 helper 校验客户端发来的认证数据：

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

兼容性说明：

- MySQL 5.7 客户端通常可直接使用 `mysql_native_password`
- MySQL 8.0 客户端可根据服务端声明使用 `mysql_native_password` 或 `caching_sha2_password`
- 如果希望客户端将服务端识别为 MySQL 8.0，可以在 shim 中覆盖 `version()`

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.

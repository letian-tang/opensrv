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

    // 仅用于这个无认证示例。生产环境必须验证 username、salt 和 auth_data。
    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }

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

`IntermediaryOptions` 还可以分别配置读、认证和写超时、最大 packet、单连接 prepared statement 数量，以及单连接累计 long-data 上限。写超时默认 60 秒；生产环境应根据查询结果大小和客户端网络情况显式配置其余资源上限。

如果后端支持连接池复用语义，应覆盖 `on_reset_connection()`：只有在事务、会话变量和其他连接局部状态确实恢复后才返回 `true`。默认返回 `false`，此时 `COM_RESET_CONNECTION` 会收到“不支持”错误，不会被虚假确认。

### 会话状态与列元数据

opensrv 不解析 SQL 来判断事务是否开始或结束，后端应报告实际状态：

- OK 响应使用 `completed_with_status(OkResponse { status_flags, ..Default::default() })`；多结果使用 `complete_one_with_status`。这两个接口按原值发送状态，允许合法的零值（关闭 autocommit 且不在事务中）。
- 结果集使用 `QueryResultWriter::set_status_flags` 或 `RowWriter::set_status_flags`；未设置时继承会话状态。数据库初始化可使用 `InitWriter::ok_with_status`。
- 协议层保留事务、autocommit、NO_BACKSLASH_ESCAPES 和只读事务标志，供后续查询、PING、STMT_RESET、切库和元数据 EOF 使用；查询局部标志不会泄漏到下一条命令。后端在错误时发生回滚，也可先设置状态再调用 `error`，使后续响应保持正确；ERR 本身没有状态字段。
- `COM_RESET_CONNECTION` 成功后恢复 `IntermediaryOptions::initial_status_flags`，后端的重置实现也必须恢复对应状态。

为兼容现有调用，`completed` / `complete_one` 中的空 `status_flags` 仍表示继承；需要明确清零时必须使用上述显式接口。`OkResponse` 和 `Column` 原有结构体初始化方式不变。

列定义可以通过 `start_with_metadata(&columns, &metadata)` 指定 `ColumnMetadata { collation: Some(46), decimals: 0 }`；预处理通过 `reply_with_metadata(id, &params, &columns, &param_metadata, &column_metadata)` 提供同样的信息。每个元数据切片必须与对应列数量一致：

- `collation: None` 保留原有类型/flags 推断。显式 collation 支持已知 UTF-8 或 binary (63)，不提供字符集转码。
- `BINARY_FLAG` 不等于二进制字节。例如 utf8mb4_bin 文本用 collation 46，TEXT（BLOB 协议类型）可以用 45；真正的 BLOB/VARBINARY 用 63，并保留相应的 `BINARY_FLAG`。
- `decimals` 表达 DECIMAL scale 或时间小数精度；0 为默认值，31 可表达浮点未指定精度。后端负责提供与类型、值一致的精度。

握手声明 `CLIENT_LONG_FLAG`，与 ColumnDefinition41 的两字节 flags 一致，避免驱动误读其后的 decimals。

Connector/J 8.4.0 的 `isValid()`/PING 路径不解析 OK 中的状态，可能清空驱动本地事务状态。不要将“事务内调用 isValid 后使用 useLocalTransactionState”视为已保证的兼容场景；原始包测试独立验证 opensrv 返回当前状态，JDBC COMMIT 回归不包含该驱动路径。参见官方 [NativeSession.ping](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/core-impl/java/com/mysql/cj/NativeSession.java) 与 [NativeProtocol.sendCommand](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/protocol-impl/java/com/mysql/cj/protocol/a/NativeProtocol.java)。

## 认证与兼容性

`AsyncMysqlShim` 默认拒绝认证，避免实现者遗漏认证方法后意外开放数据库。只允许可信网络中的无认证服务，也必须像上面的示例一样显式返回 `true`。默认 handshake challenge 每个连接随机生成。

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
- 未协商 `CLIENT_PLUGIN_AUTH` 的 4.1 客户端按 native password 处理，要求 `CLIENT_SECURE_CONNECTION`；需要其他认证插件时返回 `ER_NOT_SUPPORTED_AUTH_MODE` 并关闭，不发送客户端无法理解的 AuthSwitch。
- 如果希望客户端将服务端识别为 MySQL 8.0，可以在 shim 中覆盖 `version()`

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.
## MySQL 8 client compatibility and regression tests

HandshakeResponse41 fields selected by capability flags must be present and
NUL-terminated. Empty database/plugin strings remain valid encodings; truncated
responses receive ER_MALFORMED_PACKET and the connection closes before backend
authentication or database initialization.

Only known utf8mb3/utf8mb4 client handshake collations are accepted (including
33, 45, 46 and 255). Unknown IDs return ER_UNKNOWN_COLLATION; other character
sets return ER_UNKNOWN_CHARACTER_SET. This is a deliberate compatibility change:
Latin1, GBK and binary client handshakes no longer succeed. The server's
initial_collation must also select a supported UTF-8 collation.
TLS connections apply client validation to the complete encrypted handshake.

This guarantees a UTF-8 wire encoding boundary, not MySQL collation/sorting or
utf8mb3 character-range semantics. The backend remains responsible for SQL such
as SET NAMES and must not claim to switch to an encoding it cannot implement.
Full caching_sha2_password authentication, compression, cursors and query
attributes are outside this compatibility increment.

### Connector/J regression

Requires JDK 17+, Maven and the Rust toolchain. From the repository root:

```sh
bash mysql/tests/connector-j/run.sh
```

The independent Maven module pins Connector/J **8.4.0 and 9.7.0**, JUnit 5.10.2
and HikariCP 5.1.0. The command runs both driver suites in separate JVMs, verifies
the actually loaded driver version, and retains separate build/test reports.
Each run starts a loopback-only Rust fixture on an ephemeral port, waits for
readiness and terminates the owned process after the tests.
The fixture implements deterministic test SQL, not a production SQL backend.
Java is not required for ordinary cargo test.

To run just one pinned driver (remaining arguments are passed to Maven):

```sh
bash mysql/tests/connector-j/run.sh --driver 9.7.0
bash mysql/tests/connector-j/run.sh --driver 8.4.0
```

Tests cover pool reuse, Chinese/emoji, actual server-prepared statements,
signed 64-bit boundaries and NULL rebinding, a 16 MiB+ logical row, streaming
read disconnection followed by a fresh connection, and 500 simultaneous Java
workers/connections each checking 10 uniquely identified results. They also check
that COMMIT actually reaches the backend with useLocalTransactionState enabled,
and that text/binary types and DECIMAL scale agree in query, prepare and execute
metadata. This is a
correctness regression, not a throughput benchmark or JDBC cancellation test.
Reports are written under
`mysql/tests/connector-j/target/connector-j-{8.4.0,9.7.0}/surefire-reports`.

### 时间类型与持续审查边界

DATE 文本按列类型输出 `YYYY-MM-DD`；DATETIME/TIMESTAMP 保留微秒。`ToMysqlValue`
新增有默认实现的 `to_mysql_text_with_column`，已有自定义编码器不需要修改；`&T`
及 `Option<T>` 会转发列上下文。`chrono` 超出 0–9999 的年份及闰秒无法用 MySQL
时间字段准确表达，编码前返回错误，不再强转或输出非法微秒。

`mysql_common::Value::Time` 支持正负 TIME，包含超过 24 小时和微秒；`Duration`
仍只能表达非负时间。输出范围为 `-838:59:59` 至 `838:59:59`，两端不允许非零
微秒；`Duration` 继续截断不足一微秒的部分。二进制参数只接收合法结构长度，零日期保留为原始值，SQL
模式和实际日期有效性由后端决定。MySQL 8 的 13 字节 DATETIME/TIMESTAMP 参数
（末尾两字节为有符号分钟偏移）完整保留在 `ValueInner::Datetime`；转换为不带
时区的 `NaiveDateTime` 会明确失败，不丢弃偏移，也不替后端选择会话时区。

Connector/J 8.4.0 在负 TIME 的二进制解码中只对 days 取负、未对 hours 取负，
例如将标准编码的 `-25:02:03` 读成 `-23:02:03`。扩大测试后还确认其文本解码在
小时为零时丢失负号（如 `-00:00:00.000001`），负的一位小时也缺少零填充。
服务端仍按 MySQL 官方格式发送；这些不是 opensrv 已保证兼容的场景。
`ConnectorJ840Test` 中两个 `documentsConnectorJ840Negative*TimeLimitation`
测试**仅用于确认旧驱动限制，不计作负 TIME 兼容通过**。参见官方
[MysqlBinaryValueDecoder.decodeTime](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/protocol-impl/java/com/mysql/cj/protocol/a/MysqlBinaryValueDecoder.java)。
文本路径见 [MysqlTextValueDecoder.getTime](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/protocol-impl/java/com/mysql/cj/protocol/a/MysqlTextValueDecoder.java)
及 [InternalTime.toString](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/core-api/java/com/mysql/cj/protocol/InternalTime.java)。

官方在 [Connector/J 9.7.0](https://dev.mysql.com/doc/relnotes/connector-j/en/news-9-7-0.html)
修复了此缺陷（Bug #119863 / #38951042），该驱动仍支持 MySQL 8.0 服务端。
`ConnectorJ970Test.negativeBinaryTimeResultsAreCorrect` 要求真实 server-prepared
查询返回正确值，不允许回退为旧驱动的错误输出。两套驱动共用相同的 Rust 服务端编码，
覆盖负一微秒、不足一小时、23/24/25 小时、范围端点、负零归一化及 NULL，
并重复执行、检查后续查询。这里验证 `getString()` 的时长表示，不声称 Java
`LocalTime` / `java.sql.Time` 能表达负数或超过 24 小时的时长。

分层证据与剩余审查项见 [协议覆盖清单](tests/PROTOCOL_COVERAGE.md)。

`no_more_results()` 只能结束已经通过 `finish_one` / `complete_one` 生成的结果，
不能替代响应：直接在新建 writer 上调用会返回错误并触发未完成响应保护。
需要返回空 OK 时使用 `completed(OkResponse::default())`；这避免客户端等不到
任何响应却仍被服务端视作成功完成。

If the machine's default Maven mirror is unavailable, use the supplied isolated
Central settings without changing the user's Maven configuration:

```sh
bash mysql/tests/connector-j/run.sh -s mysql/tests/connector-j/settings-central.xml
```

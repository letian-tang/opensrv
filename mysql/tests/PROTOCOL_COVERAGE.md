# MySQL 协议持续审查清单

范围：opensrv 的 classic protocol，不实现 SQL 执行器、不修改 nimbus-db。
这是覆盖证据清单，不是“完全兼容 MySQL 8”的声明。官方基线固定为 MySQL 8.0.46；
真实 Java 驱动固定为 Connector/J 8.4.0 和 9.7.0，独立 JVM、独立测试报告，并检查实际加载版本。

## 已有可复现证据

| 层次 | 验证内容 | 测试入口 |
| --- | --- | --- |
| 握手 / TLS | capability 必需字段、空字段、截断、UTF-8 collation、无插件协商、非法 auth 长度标记；ERR 序号、关闭和回调隔离 | `src/tests/commands.rs`、`tests/it/handshake.rs` |
| 网络分帧 | U24_MAX 拆包及空终止帧、sequence 回环、packet 限制、共享读缓冲、逐字节碎片/Pending、逐位置截断/连接重置、部分写入/WriteZero/BrokenPipe、write/flush timeout、缓冲复用 | `src/packet_reader.rs`、`src/packet_writer.rs`、`src/tests/packet.rs` |
| 命令 / 预处理 | 未知 statement、long-data 上限及清理、无响应命令、错误后 PING/EXECUTE 恢复、reset | `tests/it/async.rs` |
| 参数 | 已接受类型 × signedness/NULL/long-data/类型重绑定矩阵、后续参数对齐、NULL bitmap 跨字节、截断、尾随字节、全部 256 种 temporal 长度；保留 13 字节 datetime 偏移 | `src/params.rs` |
| 结果 / 状态 | EOF/OK capability 矩阵、显式零状态、text/PS multi-results 权限及中途超限 ERR、无响应结束拒绝、事务后续命令与 reset、列 metadata 一致性 | `src/tests/writers.rs`、`tests/it/session.rs` |
| 数值 / 时间编码 | 目标整数范围、零日期、chrono 年份/闰秒拒绝、负 TIME、微秒及范围端点、DATE 文本列上下文、写入回滚 | `src/tests/value/`、`src/resultset.rs` |
| 真实客户端 | pool 复用、UTF-8、prepared/NULL/整数边界、大行、断读恢复、真实 COMMIT、元数据、时间参数重绑定、500 × 10 确定性查询 | `tests/connector-j/` |

上述每层仍可能有未覆盖的组合；现有测试通过不代表穷尽协议状态空间。

## 本轮扩大审查的覆盖与后续维护方向

- 已增加确定性碎片化和故障注入：小包每个截断点、异步 Pending、大包续帧/空终止边界、部分写入的每个失败位置及 flush 超时。它们验证调用返回后应丢弃失败连接，不声明 PacketReader/PacketWriter 可在取消或错误后继续使用。
- 已用独立固定载荷遍历当前接受的参数类型，交叉 NULL / long-data / unsigned / 显式重绑定和复用，并检查后续参数对齐、尾随字节拒绝。新增参数类型时必须扩展这个矩阵。
- 已覆盖 text/PS × EOF/OK × 多结果权限，验证第二个结果行超限后只发送 ERR、不夹带部分行或额外 EOF；空 `no_more_results` 不能绕过未完成响应检查，即使后端忽略其错误。
- 列上下文、时间范围、signedness 和 metadata 已有本轮针对性验证；自定义编码器的业务值语义仍由使用方保证。后续可增加长时间 fuzz 和真实 MySQL 服务端差分测试，这不是当前测试已经提供的证据。

## 明确边界及未消除的差异

- 未扩展完整 caching_sha2_password/RSA/full-auth、压缩、字符集转码、query attributes。
- 游标是可选后端接口：原始包测试 `tests/it/cursor.rs` 覆盖 EOF/OK、FETCH 0、整批末尾、
  未打开/未知游标、畸形命令、状态隔离、重复执行、关闭/重置/断连。真实执行及资源配额
  由使用方验证；NimbusDB 的独立 JDBC 模块使用真实 DuckDB，而非 opensrv 的固定 SQL fixture。
- 13 字节输入保留 offset，但不做 session timezone 转换；后端选择实际时间语义。
- Connector/J 8.4.0 PING 不解析 OK 状态；事务内 PING 后依赖 useLocalTransactionState 不保证正确。
- Connector/J 8.4.0 binary TIME 负号处理有错误。Java 中对应 characterization test
  确认实际错误输出，不是正确性验收；Rust 的负 TIME 测试要求标准字节，并与独立 mysql_common 序列化及解码比较。
- 扩大边界测试后还确认 8.4.0 文本 TIME 在小时为零时丢失负号，负一位小时缺少零填充；
  原先仅用大于 24 小时的值验证文本路径，不能代表全部负 TIME。现另有显式命名的旧驱动文本限制测试。
- Connector/J 9.7.0 的负 binary TIME 回归独立要求正确值，覆盖负一微秒、小于一小时、
  23/24/25 小时、范围端点、NULL、负零及重复执行；不允许使用旧驱动错误值作为预期。
- 未连接真实 MySQL 8 服务端运行差分测试；目前“对齐”证据来自官方 C++ 实现、固定字节断言、独立 Rust 解码和真实 Java 驱动。

## 官方依据

游标配套实施的跨仓库验证由 NimbusDB `docs/JDBC_CURSOR_VALIDATION.md` 记录。
opensrv 本轮新增的原始包测试不依赖 NimbusDB，也不把后端物化结果误称为协议层缓存。

- [sql_cursor.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/sql_cursor.cc)：
  游标物化、FETCH 整批/末尾行为，以及在真实会话状态上叠加 CURSOR_EXISTS/LAST_ROW_SENT。

- [protocol_classic.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/protocol_classic.cc)：分帧、结果列、EOF/OK、DATE/DATETIME/TIME 的 wire 格式。
- [net_serv.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql-common/net_serv.cc)：大包续帧、空终止帧、sequence 回环和部分 I/O。
- [sql_prepare.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/sql_prepare.cc)：`set_parameter_value` 的 13 字节 DATETIME、long-data 类型边界。
- [my_time.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/mysys/my_time.cc)：`check_time_range_quick` 的 TIME 上下限及端点微秒限制。
- [sql_authentication.cc](https://github.com/mysql/mysql-server/blob/mysql-8.0.46/sql/auth/sql_authentication.cc)：无 PLUGIN_AUTH 时的认证处理。
- [Connector/J binary decoder](https://github.com/mysql/mysql-connector-j/blob/8.4.0/src/main/protocol-impl/java/com/mysql/cj/protocol/a/MysqlBinaryValueDecoder.java)：负 TIME 客户端偏差。
- [Connector/J 9.7.0 release notes](https://dev.mysql.com/doc/relnotes/connector-j/en/news-9-7-0.html)：Bug #119863 / #38951042 的修复版本，仍支持 MySQL 8.0 服务端。

## 验证命令

```sh
cargo test -p opensrv-mysql --all-features --all-targets
cargo test -p opensrv-mysql --no-default-features --all-targets
cargo clippy -p opensrv-mysql --all-features --all-targets -- -D warnings
cargo clippy -p opensrv-mysql --no-default-features --all-targets -- -D warnings
cargo fmt --all -- --check
git diff --check
bash mysql/tests/connector-j/run.sh -s mysql/tests/connector-j/settings-central.xml
```

## 本轮验证记录（2026-09-27）

- Rust 全特性：201 个单测、66 个集成测试、1 个 example 测试通过。
- Rust 无默认特性：201 个单测、60 个集成测试、1 个 example 测试通过。
- 两套特性的 Clippy（`-D warnings`）、格式检查、`git diff --check` 通过。
- Java 17.0.12 / Maven 3.8.1 / JUnit 5.10.2 / HikariCP 5.1.0：通过默认双版本脚本完成验证。
  - Connector/J 8.4.0：9 项通过，无失败或跳过；其中 7 项为功能回归，2 项仅刻画旧驱动的负 TIME 文本/二进制解码限制，不计兼容通过。
  - Connector/J 9.7.0：9 项功能回归通过，无失败或跳过；文本和 server-prepared 二进制负 TIME 均逐行断言正确值，含负一微秒和上下文复用。
  - 两套驱动各实际运行 500 个 Java 工作线程、最多 500 个连接，每线程 10 轮逐次内容校验，合计 10,000 次并发查询。
  - 运行时读取 JDBC metadata 确认实际驱动版本，报告分别位于 `connector-j/target/connector-j-8.4.0/surefire-reports` 和 `connector-j/target/connector-j-9.7.0/surefire-reports`。
- 新发现的 TIME 端点越界和空结果错误完成标记均先由回归测试复现失败，再验证修复。

## 游标实现复审（2026-09-28）

- 检查命令解析、可选回调、响应完成保护、旧游标清理、结果所有权和分批发送路径。
- 补齐两个游标 writer 的 `set_status_flags`：后端执行启动事务或读取失败回滚时，
  能报告实际状态，不局限于继承上一命令的状态。未添加 SQL 事务推断。
- `cursor_status_updates_include_empty_fetch_and_error_paths` 覆盖 EOF/OK 两种协商、
  打开、FETCH 0、耗尽、执行/FETCH 错误、显式零及会话状态过滤；修复前因缺少公开
  状态接口无法编译，补齐后按实际 wire 字节和持久状态断言通过。
- 当前全特性 205 单测 / 67 集成测试、无默认特性 205 单测 / 61 集成测试通过；
  两套特性 all-targets Clippy（`-D warnings`）、fmt、diff check 通过。

## 0.10.6 发布前验证（2026-09-28）

- 包版本统一为 0.10.6；重新运行两套特性 all-targets 测试，均为 205 单测，
  分别 67 / 61 集成测试，另有 1 项 example 测试通过。
- 两套特性严格 Clippy、fmt 与 diff check 通过。
- 双版本 JDBC 脚本重新通过：8.4.0、9.7.0 各 9 项，无失败或跳过；
  8.4.0 中两项负 TIME 限制刻画仍不算兼容通过。
- 每套 JDBC 各运行 500 工作线程、最多 500 连接、每线程 10 轮查询。
  游标后端的 NimbusDB 联调证据仍以其独立验证记录为准。

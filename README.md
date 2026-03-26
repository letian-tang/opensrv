# OpenSrv

基于 [databendlabs/opensrv](https://github.com/databendlabs/opensrv) 修改而来。

当前仓库只保留了 `opensrv-mysql`，用于模拟 MySQL/MariaDB 服务端协议，方便自定义数据库、缓存层或代理系统复用现有 MySQL 客户端生态。

## 当前内容

- `mysql`：MySQL/MariaDB 协议实现
- `mysql/examples`：示例程序
- `mysql/tests`：集成测试

## 使用说明

MySQL crate 的详细说明见：

- [mysql/README.md](./mysql/README.md)

## License

Licensed under <a href="./LICENSE">Apache License, Version 2.0</a>.

# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](http://keepachangelog.com/en/1.0.0/)
and this project adheres to [Semantic Versioning](http://semver.org/spec/v2.0.0.html).

<!-- insertion marker -->
## Unreleased

## [v0.10.6](https://github.com/letian-tang/opensrv/releases/tag/v0.10.6) - 2026-09-28

- Allow cursor execute/fetch backends to report session status, including empty
  batches, explicit zero and error rollback state, without leaking cursor flags.

- Generate ASCII-safe random authentication challenges, matching MySQL and
  preventing Connector/J's ASCII seed decoding from corrupting native auth.

- Add optional read-only prepared-statement cursors, COM_STMT_FETCH and tracked
  metadata/binary-row writers. Existing backends reject cursors by default;
  cleanup and native result ownership remain backend responsibilities.

## [v0.10.5](https://github.com/letian-tang/opensrv/releases/tag/v0.10.5)

- Run independent Connector/J 8.4.0 and 9.7.0 regression suites with checked runtime
  driver versions and separate reports. Require correct negative binary TIME values
  on 9.7.0, while preserving an explicitly named legacy 8.4.0 defect characterization.
- Expand negative TIME coverage to sub-hour/sub-second values, NULL and range bounds;
  also characterize 8.4.0's text-path sign loss for zero hours and formatting difference.
- Reject reserved length-encoded authentication markers and malformed temporal
  parameter lengths before backend callbacks; retain MySQL 8's 13-byte datetime
  with timezone displacement and reject lengths that overflow the host usize.
- Validate chrono years and leap seconds before encoding, and reject invalid
  datetime fractions during conversion instead of creating a chrono leap second.
- Encode signed mysql_common TIME values in text and binary, and use column-aware
  text encoding so DATE values do not unexpectedly include a DATETIME suffix.
- Reject nonzero microseconds at the TIME range endpoints (±838:59:59), matching
  MySQL 8; retain Duration's existing sub-microsecond truncation.
- Reject no_more_results without any result/OK response, including backends that
  discard the error, so clients cannot hang behind a false completion marker.
- Add deterministic fragmented/truncated/failing I/O, stalled-flush and parameter
  type/NULL/long-data/rebinding matrices.
- Extend real-driver temporal/NULL-rebinding regressions and explicitly characterize
  Connector/J 8.4.0's incorrect negative binary TIME decoding (not a compatibility pass).
- Add explicit zero-status response APIs and preserve durable session flags across
  query, prepared execution, metadata, PING and database initialization; restore
  initial flags after a successful connection reset.
- Add optional column collation/decimals metadata for query and prepared responses,
  preserving existing Column literals and legacy inference. Advertise CLIENT_LONG_FLAG
  so Connector/J reads two-byte column flags and decimals at the correct offsets.
- Avoid AuthSwitch for clients without CLIENT_PLUGIN_AUTH; accept implicit native
  authentication with SECURE_CONNECTION and explicitly reject unsupported modes.
- Extend raw plaintext/TLS and JDBC regressions for authentication capabilities,
  transaction status/actual COMMIT, binary-collation text and DECIMAL scale.
- Reuse small packet-builder allocations after successful sends, retaining at
  most 64 KiB of capacity; release oversized buffers and buffers from failed sends.
- Share protocol I/O configuration across greeting, plain, and TLS paths without
  changing transport buffering, defaults, public interfaces, or flush behavior.
- Reject missing capability-selected database/auth-plugin fields in client
  handshakes with ER_MALFORMED_PACKET before invoking backend callbacks.
- Accept only known UTF-8 client/server handshake collations; non-UTF-8 clients
  now fail explicitly instead of connecting with an unsupported wire encoding.
- Add plaintext and encrypted handshake regressions and an independent
  Connector/J 8.4.0 suite, including 500 concurrent readers and large rows.

## [v0.7.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.7.0) - 2024-02-21

<small>[Compare with v0.6.0](https://github.com/datafuselabs/opensrv/compare/v0.6.0...v0.7.0)</small>

### Bug Fixes

- correctly decode mysql binary timestamp (#60) ([b47d5c7](https://github.com/datafuselabs/opensrv/commit/b47d5c7aaf0758a2bcb487a32105b2f3a987cfd8) by LFC).

## [v0.6.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.6.0) - 2023-12-12

<small>[Compare with v0.5.0](https://github.com/datafuselabs/opensrv/compare/v0.5.0...v0.6.0)</small>

### Features

- update rustls libraries (#57) ([dab388e](https://github.com/datafuselabs/opensrv/commit/dab388e70efe09678fa567464b8ad07c62ae2127) by Ning Sun).

## [v0.5.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.5.0) - 2023-11-15

<small>[Compare with v0.4.1](https://github.com/datafuselabs/opensrv/compare/v0.4.1...v0.5.0)</small>

### Features

- release v0.5.0 (#55) ([d1af47a](https://github.com/datafuselabs/opensrv/commit/d1af47a68592281a473fd7550d2d1684a058bb0a) by Chojan Shang).

## [v0.4.1](https://github.com/datafuselabs/opensrv/releases/tag/v0.4.1) - 2023-09-09

<small>[Compare with v0.4.0](https://github.com/datafuselabs/opensrv/compare/v0.4.0...v0.4.1)</small>

### Bug Fixes

- try to fix memory safety problem ([2f84755](https://github.com/datafuselabs/opensrv/commit/2f84755dfa0ec32de752b7530580f985cb027896) by Chojan Shang).

## [v0.4.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.4.0) - 2023-04-11

<small>[Compare with v0.3.0](https://github.com/datafuselabs/opensrv/compare/v0.3.0...v0.4.0)</small>

### Features

- add marashal/unmarshal support for large size integer types. (#45) ([a97d750](https://github.com/datafuselabs/opensrv/commit/a97d75058baf4fc20031b1c9668ff94e7e4e542e) by RinChanNOW).
- make version() return &str to String ([a6d29b1](https://github.com/datafuselabs/opensrv/commit/a6d29b1cd3c6b43f6c0eef10fd2e9ad30635ec51) by arthur-zhang).
- add option to reject connection when dbname absence in login (#38) ([b44c9d1](https://github.com/datafuselabs/opensrv/commit/b44c9d1360da297b305abf33aecfa94888e1554c) by Ning Sun).
- packet reader reduce bytes resize times ([32af58d](https://github.com/datafuselabs/opensrv/commit/32af58dd9fd9be66c39dd0728142d78e831f6fb6) by baishen).

### Bug Fixes

- corrupt tls handshake caused by buffer over read (#39) ([4f6400c](https://github.com/datafuselabs/opensrv/commit/4f6400cab379bce3b0b35b6753e7cdc6a8d50a8b) by Ning Sun).
- make clippy happy (#41) ([564e62e](https://github.com/datafuselabs/opensrv/commit/564e62e34cd4b06a7c75a47cac271c17637401b0) by Chojan Shang).

## [v0.3.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.3.0) - 2022-11-26

<small>[Compare with v0.2.0](https://github.com/datafuselabs/opensrv/compare/v0.2.0...v0.3.0)</small>

### Features

- add tls support for opensrv-mysql (#34) ([3a984ec](https://github.com/datafuselabs/opensrv/commit/3a984ec1b4046d9b2b8da58abfe5d8921715ddeb) by SSebo).
- bump main deps (#33) ([1b3e11d](https://github.com/datafuselabs/opensrv/commit/1b3e11d73bd5f0fcad1401df1620b3bbb5b7a0f6) by Chojan Shang).
- bump version to 0.2.1 ([0f488d0](https://github.com/datafuselabs/opensrv/commit/0f488d0041f4979f88ace93b3ce41b72713f93f0) by sundyli).
- remove unused clippy ([161b5a9](https://github.com/datafuselabs/opensrv/commit/161b5a97e435aefecd2877c894e20e422aa39de9) by sundyli).
- add orderfloat ([c875ddd](https://github.com/datafuselabs/opensrv/commit/c875ddd29051c3a62462a96caba3eb9792335149) by sundyli).

## [v0.2.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.2.0) - 2022-08-17

<small>[Compare with v0.1.0](https://github.com/datafuselabs/opensrv/compare/v0.1.0...v0.2.0)</small>

### Features

- Implement proposal Simplify ClickHouseSession (#26) ([a757e28](https://github.com/datafuselabs/opensrv/commit/a757e286f49ca3653ff3b972615842fb34f98297) by Xuanwo).

### Code Refactoring

- write mysql resultset in a streaming way (#27) ([1287c32](https://github.com/datafuselabs/opensrv/commit/1287c32cec4242fa2a440e1a9b7ffeab63ea76a8) by dantengsky).

## [v0.1.0](https://github.com/datafuselabs/opensrv/releases/tag/v0.1.0) - 2022-06-14

<small>[Compare with first commit](https://github.com/datafuselabs/opensrv/compare/eff4ec6872504b271b93b1c61a223f9386a29e47...v0.1.0)</small>

### Features

- add a new marshal mod from databend's common-io (#20) ([d29655a](https://github.com/datafuselabs/opensrv/commit/d29655a73ed26d94733861de83fa764e29ad2f78) by Chojan Shang).
- datafuse-extras/msql-srv -> opensrv-mysql (#3) ([86d1be8](https://github.com/datafuselabs/opensrv/commit/86d1be8bf56dcc5be18d49340041b4c57de3a29f) by Chojan Shang).
- common/clickhouse-srv -> opensrv-clickhouse (#1) ([183b728](https://github.com/datafuselabs/opensrv/commit/183b7281ca014033d70616ecab1046df5000fa9c) by Chojan Shang).

### Bug Fixes

- spawn to fix sync tests (#17) ([54638ee](https://github.com/datafuselabs/opensrv/commit/54638ee8b5abb12aa8c0f63469ce78a900223c0a) by Chojan Shang).
- pass federated query ([967477f](https://github.com/datafuselabs/opensrv/commit/967477f1f7005f8911a7d6c38cefbba4edd755ad) by zhang2014).
- add init schema handle on handshake (#11) ([e744427](https://github.com/datafuselabs/opensrv/commit/e744427ebce9271289e47d655f1223790aae7482) by Jun).
- make authenticate async to fix hang issue (#10) ([9690be9](https://github.com/datafuselabs/opensrv/commit/9690be9ff965c0e86e1ff599897c2019b0a379bd) by Chojan Shang).

### Code Refactoring

- make auth_plugin_for_username async (#15) ([4e447f8](https://github.com/datafuselabs/opensrv/commit/4e447f8e64619b78c84c2c10f87574b1ae64a5ca) by Yang Xiufeng).

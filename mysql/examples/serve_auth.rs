// Copyright 2021 Datafuse Labs.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

//! After running this, you should be able to run:
//!
//! ```console
//! $ echo "SELECT * FROM foo" | mysql -h 127.0.0.1 -u default --table
//! $ echo "SELECT * FROM foo" | MYSQL_PWD=secret mysql -h 127.0.0.1 -u mysql8 --table
//! $
//! ```

use std::io;
use std::iter;

use mysql_common as myc;
use opensrv_mysql::*;
use tokio::io::AsyncWrite;
use tokio::net::TcpListener;

struct Backend;

const USER_NATIVE: &[u8] = b"default";
const USER_CACHING_SHA2: &[u8] = b"mysql8";
const PASSWORD: &[u8] = b"secret";

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
        let resp = OkResponse::default();
        results.completed(resp).await
    }

    async fn on_close(&mut self, _: u32) {}

    async fn on_query<'a>(
        &'a mut self,
        sql: &'a str,
        results: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        println!("execute sql {:?}", sql);

        let cols = &[Column {
            table: String::new(),
            column: "abc".to_string(),
            collen: 0,
            coltype: myc::constants::ColumnType::MYSQL_TYPE_LONG,
            colflags: myc::constants::ColumnFlags::UNSIGNED_FLAG,
        }];

        let mut w = results.start(cols).await?;
        w.write_row(iter::once(67108864u32)).await?;
        w.write_row(iter::once(167108864u32)).await?;

        w.finish_with_info("ExtraInfo").await
    }

    /// authenticate method for the specified plugin
    async fn authenticate(
        &self,
        auth_plugin: &str,
        username: &[u8],
        salt: &[u8],
        auth_data: &[u8],
    ) -> bool {
        matches!(username, USER_NATIVE | USER_CACHING_SHA2)
            && verify_auth_plugin_data(auth_plugin, PASSWORD, salt, auth_data)
    }

    fn version(&self) -> String {
        "8.0.36-opensrv".to_string()
    }

    fn connect_id(&self) -> u32 {
        u32::from_le_bytes([0x08, 0x00, 0x00, 0x00])
    }

    fn default_auth_plugin(&self) -> &str {
        MYSQL_NATIVE_PASSWORD
    }

    async fn auth_plugin_for_username(&self, user: &[u8]) -> &'static str {
        if user == USER_CACHING_SHA2 {
            CACHING_SHA2_PASSWORD
        } else {
            MYSQL_NATIVE_PASSWORD
        }
    }

    fn salt(&self) -> [u8; 20] {
        let bs = ";X,po_k}>o6^Wz!/kM}N".as_bytes();
        let mut scramble: [u8; 20] = [0; 20];
        for i in 0..20 {
            scramble[i] = bs[i];
            if scramble[i] == b'\0' || scramble[i] == b'$' {
                scramble[i] += 1;
            }
        }
        scramble
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

#[test]
fn it_works() {
    let c: u8 = b'\0';
    let d: u8 = 0 as u8;
    let e: u8 = 0x00;

    assert_eq!(c, d);
    assert_eq!(e, d);
}

use opensrv_mysql::*;
use std::{
    io,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};
use tokio::io::{AsyncReadExt, AsyncWrite, AsyncWriteExt, DuplexStream};

struct Backend {
    fail_fetch: bool,
    next: u32,
    count: u32,
    opened: bool,
    active: Arc<AtomicUsize>,
}
impl Drop for Backend {
    fn drop(&mut self) {
        if self.opened {
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
}
fn cols() -> [Column; 1] {
    [Column {
        table: String::new(),
        column: "v".into(),
        collen: 0,
        coltype: ColumnType::MYSQL_TYPE_LONG,
        colflags: ColumnFlags::empty(),
    }]
}
#[async_trait::async_trait]
impl<W: AsyncWrite + Send + Unpin> AsyncMysqlShim<W> for Backend {
    type Error = io::Error;
    async fn authenticate(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> bool {
        true
    }
    async fn on_prepare<'a>(
        &'a mut self,
        q: &'a str,
        w: StatementMetaWriter<'a, W>,
    ) -> io::Result<()> {
        self.count = q.parse().unwrap();
        w.reply(7, &[], &cols()).await
    }
    async fn on_execute<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        w: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        w.completed(OkResponse::default()).await
    }
    async fn on_execute_cursor<'a>(
        &'a mut self,
        _: u32,
        _: ParamParser<'a>,
        w: CursorExecuteWriter<'a, W>,
    ) -> io::Result<()> {
        self.next = 0;
        self.opened = true;
        self.active.fetch_add(1, Ordering::SeqCst);
        w.open(&cols(), &[ColumnMetadata::default()]).await
    }
    async fn on_fetch<'a>(
        &'a mut self,
        _: u32,
        n: u32,
        mut w: CursorFetchWriter<'a, W>,
    ) -> io::Result<()> {
        if self.fail_fetch {
            self.fail_fetch = false;
            return w
                .error(ErrorKind::ER_UNKNOWN_ERROR, b"injected fetch failure")
                .await;
        }
        for _ in 0..n {
            if self.next == self.count {
                return w.finish(true).await;
            }
            w.write_row_with(|row| row.write_col(self.next as i32))
                .await?;
            self.next += 1;
        }
        w.finish(false).await
    }
    async fn on_close_cursor(&mut self, _: u32) {
        if self.opened {
            self.opened = false;
            self.active.fetch_sub(1, Ordering::SeqCst);
        }
    }
    async fn on_close(&mut self, _: u32) {}
    async fn on_reset_connection(&mut self) -> io::Result<bool> {
        Ok(true)
    }
    async fn on_query<'a>(
        &'a mut self,
        query: &'a str,
        w: QueryResultWriter<'a, W>,
    ) -> io::Result<()> {
        self.fail_fetch = query == "fail";
        w.completed(OkResponse::default()).await
    }
}
async fn read(c: &mut DuplexStream) -> (u8, Vec<u8>) {
    let mut h = [0; 4];
    c.read_exact(&mut h).await.unwrap();
    let mut body = vec![0; u32::from_le_bytes([h[0], h[1], h[2], 0]) as usize];
    c.read_exact(&mut body).await.unwrap();
    (h[3], body)
}
async fn send(c: &mut DuplexStream, seq: u8, body: &[u8]) {
    c.write_all(&[body.len() as u8, 0, 0, seq]).await.unwrap();
    c.write_all(body).await.unwrap();
}
fn status(body: &[u8], flag: StatusFlags) {
    assert_eq!(body[0], 0xfe);
    assert_eq!(
        u16::from_le_bytes([body[3], body[4]]),
        (StatusFlags::SERVER_STATUS_AUTOCOMMIT | flag).bits()
    );
}
async fn execute(c: &mut DuplexStream) {
    send(c, 0, &[0x17, 7, 0, 0, 0, 1, 1, 0, 0, 0]).await;
    assert_eq!(read(c).await, (1, vec![1]));
    assert_eq!(read(c).await.0, 2);
    let (seq, body) = read(c).await;
    assert_eq!(seq, 3);
    status(&body, StatusFlags::SERVER_STATUS_CURSOR_EXISTS);
}
async fn fetch(c: &mut DuplexStream, n: u32, expected: &[i32], eof: bool) {
    let mut command = vec![0x1c, 7, 0, 0, 0];
    command.extend(n.to_le_bytes());
    send(c, 0, &command).await;
    for (index, value) in expected.iter().enumerate() {
        let (seq, row) = read(c).await;
        assert_eq!(seq, index as u8 + 1);
        assert_eq!(&row[..2], &[0, 0]);
        assert_eq!(i32::from_le_bytes(row[2..].try_into().unwrap()), *value);
    }
    let (seq, body) = read(c).await;
    assert_eq!(seq, expected.len() as u8 + 1);
    status(
        &body,
        if eof {
            StatusFlags::SERVER_STATUS_LAST_ROW_SENT
        } else {
            StatusFlags::SERVER_STATUS_CURSOR_EXISTS
        },
    );
}

#[tokio::test]
async fn cursor_wire_lifecycle_and_boundaries() {
    tokio::time::timeout(std::time::Duration::from_secs(10), async {
        for deprecate in [false, true] {
            for count in [0, 3, 4] {
                let active = Arc::new(AtomicUsize::new(0));
                let backend = Backend {
                    fail_fetch: false,
                    next: 0,
                    count,
                    opened: false,
                    active: Arc::clone(&active),
                };
                let (mut c, server) = tokio::io::duplex(8192);
                let task = tokio::spawn(async move {
                    let (r, w) = tokio::io::split(server);
                    AsyncMysqlIntermediary::run_on_buffered(backend, r, w).await
                });
                read(&mut c).await;
                let mut caps =
                    CapabilityFlags::CLIENT_PROTOCOL_41 | CapabilityFlags::CLIENT_SECURE_CONNECTION;
                caps.set(CapabilityFlags::CLIENT_DEPRECATE_EOF, deprecate);
                let mut hs = vec![0; 32];
                hs[..4].copy_from_slice(&caps.bits().to_le_bytes());
                hs[8] = 45;
                hs.extend(b"u\0\0");
                send(&mut c, 1, &hs).await;
                assert_eq!(read(&mut c).await.1[0], 0);
                send(&mut c, 0, format!("\x16{count}").as_bytes()).await;
                read(&mut c).await;
                read(&mut c).await;
                if !deprecate {
                    read(&mut c).await;
                }
                execute(&mut c).await;
                fetch(&mut c, 0, &[], false).await;
                if count != 0 {
                    fetch(&mut c, 2, &[0, 1], false).await;
                }
                if count == 4 {
                    fetch(&mut c, 2, &[2, 3], false).await;
                    fetch(&mut c, u32::MAX, &[], true).await;
                } else if count == 3 {
                    fetch(&mut c, u32::MAX, &[2], true).await;
                } else {
                    fetch(&mut c, 1, &[], true).await;
                }
                assert_eq!(active.load(Ordering::SeqCst), 0);
                // Backend fetch errors close just the cursor and preserve the connection.
                send(&mut c, 0, b"\x03fail").await;
                read(&mut c).await;
                execute(&mut c).await;
                send(&mut c, 0, &[0x1c, 7, 0, 0, 0, 1, 0, 0, 0]).await;
                let (seq, error) = read(&mut c).await;
                assert_eq!(seq, 1);
                assert_eq!(error[0], 0xff);
                assert_eq!(active.load(Ordering::SeqCst), 0);
                // Exhausted, malformed, unknown statement: ERR followed by usable PING.
                for command in [
                    vec![0x1c, 7, 0, 0, 0, 1, 0, 0, 0],
                    vec![0x1c],
                    vec![0x1c, 99, 0, 0, 0, 1, 0, 0, 0],
                    vec![0x1c, 7, 0, 0, 0, 1, 0, 0, 0, 0],
                    vec![0x17, 7, 0, 0, 0, 2, 1, 0, 0, 0],
                    vec![0x17, 7, 0, 0, 0, 1, 0, 0, 0, 0],
                ] {
                    send(&mut c, 0, &command).await;
                    assert_eq!(read(&mut c).await.1[0], 0xff);
                    send(&mut c, 0, &[0x0e]).await;
                    let (seq, body) = read(&mut c).await;
                    assert_eq!(seq, 1);
                    assert_eq!(&body[3..5], &[2, 0]);
                }
                execute(&mut c).await;
                execute(&mut c).await;
                assert_eq!(active.load(Ordering::SeqCst), 1);
                send(&mut c, 0, &[0x1a, 7, 0, 0, 0]).await;
                read(&mut c).await;
                assert_eq!(active.load(Ordering::SeqCst), 0);
                execute(&mut c).await;
                send(&mut c, 0, &[0x19, 7, 0, 0, 0]).await;
                send(&mut c, 0, &[0x0e]).await;
                read(&mut c).await;
                assert_eq!(active.load(Ordering::SeqCst), 0);
                send(&mut c, 0, b"\x164").await;
                read(&mut c).await;
                read(&mut c).await;
                if !deprecate {
                    read(&mut c).await;
                }
                execute(&mut c).await;
                send(&mut c, 0, &[0x1f]).await;
                read(&mut c).await;
                assert_eq!(active.load(Ordering::SeqCst), 0);
                send(&mut c, 0, b"\x164").await;
                read(&mut c).await;
                read(&mut c).await;
                if !deprecate {
                    read(&mut c).await;
                }
                execute(&mut c).await;
                send(&mut c, 0, &[0x1c, 7, 0, 0, 0, 255, 255, 255, 255]).await;
                drop(c);
                assert!(task.await.unwrap().is_err()); // response transport failed
                assert_eq!(active.load(Ordering::SeqCst), 0);
            }
        }
    })
    .await
    .unwrap();
}

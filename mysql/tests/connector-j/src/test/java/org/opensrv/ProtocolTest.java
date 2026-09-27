package org.opensrv;

import com.zaxxer.hikari.HikariConfig;
import com.zaxxer.hikari.HikariDataSource;
import org.junit.jupiter.api.*;
import java.io.*;
import java.sql.*;
import java.util.*;
import java.util.concurrent.*;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(180)
abstract class ProtocolTest {
    static Process server;
    static String url;
    static String driverVersion;
    static final String[] NEGATIVE_TIMES = {
        "-25:02:03.123456", "00:00:00", "-838:59:58.999999",
        "-00:00:00.000001", "-00:02:03.456789", "-01:02:03",
        "-23:59:59.999999", "-24:00:00", "-838:59:59", "00:00:00", null
    };
    @BeforeAll static void start() throws Exception {
        String fixture = Objects.requireNonNull(System.getProperty("opensrv.fixture"), "Run using run.sh");
        server = new ProcessBuilder(fixture).redirectError(ProcessBuilder.Redirect.INHERIT).start();
        Runtime.getRuntime().addShutdownHook(new Thread(() -> { if (server != null) server.destroyForcibly(); }));
        var reader = new BufferedReader(new InputStreamReader(server.getInputStream()));
        var executor = Executors.newSingleThreadExecutor();
        try {
            String ready = executor.submit(reader::readLine).get(10, TimeUnit.SECONDS);
            assertNotNull(ready); assertTrue(ready.startsWith("READY "), ready);
            url = "jdbc:mysql://127.0.0.1:" + ready.substring(6)
                + "/test?sslMode=DISABLED&characterEncoding=UTF-8&useServerPrepStmts=true"
                + "&emulateUnsupportedPstmts=false&connectTimeout=10000&socketTimeout=30000";
            driverVersion = Objects.requireNonNull(System.getProperty("opensrv.connectorJVersion"));
            assertTrue(Set.of("8.4.0", "9.7.0").contains(driverVersion), "unsupported driver matrix entry");
            try (var c = connect()) {
                String actual = c.getMetaData().getDriverVersion();
                assertTrue(actual.startsWith("mysql-connector-j-" + driverVersion + " "), actual);
                System.out.println("Loaded " + actual + "; 500 threads, 500 max connections, 10 queries per thread");
            }
        } catch (Exception | AssertionError e) { server.destroyForcibly(); throw e; }
        finally { executor.shutdownNow(); }
    }
    @AfterAll static void stop() throws Exception {
        if (server != null) { server.destroy(); if (!server.waitFor(5, TimeUnit.SECONDS)) server.destroyForcibly(); }
    }
    static Connection connect() throws SQLException { return DriverManager.getConnection(url, "test", ""); }
    static void marker(Connection c, String marker) throws SQLException {
        try (var s = c.createStatement(); var r = s.executeQuery("SELECT marker_" + marker)) {
            assertTrue(r.next()); assertEquals(marker, r.getString(1)); assertFalse(r.next());
        }
    }
    static HikariDataSource pool(int size) {
        var config = new HikariConfig();
        config.setJdbcUrl(url); config.setUsername("test"); config.setPassword("");
        config.setMaximumPoolSize(size); config.setMinimumIdle(size); config.setConnectionTimeout(90000);
        return new HikariDataSource(config);
    }
    @Test void unicodeAndPreparedBoundaries() throws Exception {
        try (var c = connect()) {
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT unicode")) {
                assertTrue(r.next()); assertEquals("中文🙂", r.getString(1)); assertFalse(r.next());
            }
            try (var p = c.prepareStatement("SELECT ?, ?, ?")) {
                assertTrue(p.isWrapperFor(com.mysql.cj.jdbc.ServerPreparedStatement.class));
                for (long n : new long[]{Long.MIN_VALUE, -1, 0, Long.MAX_VALUE}) {
                    p.setLong(1, n); p.setString(2, "中文🙂"); p.setNull(3, Types.BIGINT);
                    try (var r = p.executeQuery()) {
                        assertTrue(r.next()); assertEquals(n, r.getLong(1)); assertEquals("中文🙂", r.getString(2));
                        assertNull(r.getObject(3)); assertFalse(r.next());
                    }
                    p.setLong(3, n);
                    try (var r = p.executeQuery()) {
                        assertTrue(r.next()); assertEquals(n, r.getLong(3)); assertFalse(r.next());
                    }
                }
            }
        }
    }
    @Test void poolReusesConnection() throws Exception {
        try (var ds = pool(1)) {
            Connection physical;
            try (var c = ds.getConnection()) { physical = c.unwrap(com.mysql.cj.jdbc.JdbcConnection.class); marker(c, "first"); }
            try (var c = ds.getConnection()) { assertSame(physical, c.unwrap(com.mysql.cj.jdbc.JdbcConnection.class)); marker(c, "second"); }
        }
    }
    @Test void transactionStateDoesNotSuppressCommit() throws Exception {
        try (var c = DriverManager.getConnection(url + "&useLocalTransactionState=true", "test", "")) {
            c.setAutoCommit(false);
            var session = c.unwrap(com.mysql.cj.jdbc.JdbcConnection.class).getSession().getServerSession();
            assertFalse(session.isAutocommit(), "explicit zero status must not inherit AUTOCOMMIT");
            try (var s = c.createStatement()) { s.execute("BEGIN"); }
            assertTrue(session.inTransactionOnServer());
            marker(c, "inside_transaction");
            assertTrue(session.inTransactionOnServer(), "SELECT must preserve transaction state");
            try (var p = c.prepareStatement("SELECT ?, ?, ?")) {
                p.setLong(1, 1); p.setString(2, "tx"); p.setNull(3, Types.BIGINT);
                try (var r = p.executeQuery()) { assertTrue(r.next()); assertEquals(1, r.getLong(1)); assertFalse(r.next()); }
            }
            assertTrue(session.inTransactionOnServer(), "prepared execution must preserve transaction state");
            // PING status is covered at the raw-wire layer. Connector/J 8.4.0's
            // NativeSession.ping() does not decode the OK packet's status flags.
            c.commit();
            assertFalse(session.inTransactionOnServer());
            assertFalse(session.isAutocommit());
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT commits")) {
                assertTrue(r.next()); assertEquals(1, r.getLong(1), "COMMIT must actually reach the backend"); assertFalse(r.next());
            }
        }
    }
    static void checkMetadata(ResultSetMetaData m) throws SQLException {
        assertEquals(Types.VARCHAR, m.getColumnType(1));
        assertEquals(String.class.getName(), m.getColumnClassName(1));
        assertEquals(String.class.getName(), m.getColumnClassName(2));
        assertEquals(byte[].class.getName(), m.getColumnClassName(3));
        assertEquals(Types.DECIMAL, m.getColumnType(4));
        assertEquals(2, m.getScale(4));
    }
    static void checkMetadataResult(ResultSet r) throws SQLException {
        checkMetadata(r.getMetaData());
        assertTrue(r.next());
        assertEquals("中文🙂", r.getObject(1));
        assertEquals("文本🙂", r.getObject(2));
        assertArrayEquals(new byte[]{0, (byte)0xff, (byte)0x80}, (byte[])r.getObject(3));
        assertEquals(new java.math.BigDecimal("12.34"), r.getBigDecimal(4));
        assertFalse(r.next());
    }
    @Test void explicitColumnMetadataInTextAndPreparedResults() throws Exception {
        try (var c = connect()) {
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT metadata")) { checkMetadataResult(r); }
            try (var p = c.prepareStatement("SELECT metadata")) {
                assertTrue(p.isWrapperFor(com.mysql.cj.jdbc.ServerPreparedStatement.class));
                checkMetadata(p.getMetaData());
                try (var r = p.executeQuery()) { checkMetadataResult(r); }
            }
        }
    }
    @Test void largeResultAndReadDisconnect() throws Exception {
        try (var c = connect(); var s = c.createStatement(); var r = s.executeQuery("SELECT large")) {
            assertTrue(r.next()); byte[] data = r.getBytes(1); assertEquals(16777216, data.length);
            for (byte b : data) assertEquals((byte)'x', b);
            assertFalse(r.next());
        }
        Connection c = connect();
        try {
            var s = c.createStatement(); s.setFetchSize(Integer.MIN_VALUE);
            var r = s.executeQuery("SELECT stream");
            assertTrue(r.next()); assertEquals(65536, r.getBytes(1).length);
            c.abort(Runnable::run);
        } finally { c.close(); }
        try (var next = connect()) { marker(next, "after_abort"); }
    }
    static void checkTemporalResult(ResultSet r) throws SQLException {
        assertEquals(Types.DATE, r.getMetaData().getColumnType(1));
        // JDBC getScale() returns zero for temporal types in Connector/J; inspect
        // the actual decoded wire field to check fractional precision instead.
        var metadata = r.getMetaData().unwrap(com.mysql.cj.jdbc.result.ResultSetMetaData.class);
        assertEquals(6, metadata.getFields()[1].getDecimals());
        String[][] expected = {
            {"2026-09-27", "2026-09-27 01:02:03.123456", "25:02:03.123456"},
            {"0000-00-00", "0000-00-00 00:00:00", "00:00:00"},
            {"9999-12-31", "9999-12-31 23:59:59.999999", "838:59:58.999999"}
        };
        for (String[] row : expected) {
            assertTrue(r.next());
            for (int i=0; i<3; i++) assertEquals(row[i], r.getString(i+1));
        }
        assertFalse(r.next());
    }
    @Test void temporalTextBinaryAndPreparedParameters() throws Exception {
        try (var c = connect()) {
            try (var s = c.createStatement(); var r = s.executeQuery("SELECT temporal")) { checkTemporalResult(r); }
            try (var p = c.prepareStatement("SELECT temporal")) {
                assertTrue(p.isWrapperFor(com.mysql.cj.jdbc.ServerPreparedStatement.class));
                try (var r = p.executeQuery()) { checkTemporalResult(r); }
            }
            try (var p = c.prepareStatement("SELECT temporal(?, ?, ?)")) {
                var date = java.time.LocalDate.of(2026, 9, 27);
                var datetime = date.atTime(1, 2, 3, 123456000);
                var time = java.time.LocalTime.of(12, 34, 56, 654321000);
                // Rebinding through NULL and back exercises type-map reuse and null bitmap handling.
                for (boolean nulls : new boolean[]{false, true, false}) {
                    p.setObject(1, nulls ? null : date, Types.DATE);
                    p.setObject(2, nulls ? null : datetime, Types.TIMESTAMP);
                    p.setObject(3, nulls ? null : time, Types.TIME);
                    try (var r = p.executeQuery()) {
                        assertTrue(r.next());
                        if (nulls) { for (int i=1; i<=3; i++) assertNull(r.getObject(i)); }
                        else {
                            assertEquals(date, r.getObject(1, java.time.LocalDate.class));
                            assertEquals(datetime, r.getObject(2, java.time.LocalDateTime.class));
                            assertEquals(time, r.getObject(3, java.time.LocalTime.class));
                        }
                        assertFalse(r.next());
                    }
                }
            }
        }
    }
    static void checkNegativeTimes(ResultSet r, String[] expected) throws SQLException {
        assertEquals(Types.TIME, r.getMetaData().getColumnType(3));
        for (int i = 0; i < expected.length; i++) {
            assertTrue(r.next(), "missing TIME row " + i);
            assertEquals(expected[i], r.getString(3), "TIME row " + i);
            assertEquals(expected[i] == null, r.wasNull(), "NULL flag for TIME row " + i);
        }
        assertFalse(r.next(), "unexpected extra TIME row");
    }
    static void checkNegativeTextTimes(String[] expected) throws Exception {
        try (var c = connect(); var s = c.createStatement(); var r = s.executeQuery("SELECT negative_temporal")) {
            checkNegativeTimes(r, expected);
        }
    }
    static void checkNegativeBinaryTimes(String[] expected) throws Exception {
        try (var c = connect(); var p = c.prepareStatement("SELECT negative_temporal")) {
            assertTrue(p.isWrapperFor(com.mysql.cj.jdbc.ServerPreparedStatement.class), "must exercise binary protocol");
            // Re-execute on the same statement and connection to catch leftover
            // row bytes, not only the first successful decode.
            for (int run = 0; run < 2; run++) {
                try (var r = p.executeQuery()) { checkNegativeTimes(r, expected); }
            }
            marker(c, "after_negative_time");
        }
    }
    @Test void concurrentReads() throws Exception {
        var executor = Executors.newFixedThreadPool(500);
        try (var ds = pool(500)) {
            long warmupDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(90);
            while (ds.getHikariPoolMXBean().getIdleConnections() < 500 && System.nanoTime() < warmupDeadline) {
                Thread.sleep(50);
            }
            assertEquals(500, ds.getHikariPoolMXBean().getIdleConnections(), "pool warmup");
            var barrier = new CyclicBarrier(500);
            List<Future<?>> futures = new ArrayList<>();
            for (int i=0; i<500; i++) {
                final int worker = i;
                futures.add(executor.submit(() -> {
                    try (var c = ds.getConnection()) {
                        barrier.await(120, TimeUnit.SECONDS);
                        for (int round=0; round<10; round++) marker(c, worker + "_" + round);
                    }
                    return null;
                }));
            }
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(150);
            for (var f : futures) f.get(Math.max(1, deadline-System.nanoTime()), TimeUnit.NANOSECONDS);
        } finally { executor.shutdownNow(); assertTrue(executor.awaitTermination(10, TimeUnit.SECONDS)); }
    }
}

package com.dbcomponent.core;

import com.dbcomponent.adapter.IAdapter;
import com.dbcomponent.model.DbQueryResult;
import com.dbcomponent.pool.ConnectionPool;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DbComponent: decoupled by adapter and backed by internal connection pool.
 * Public API only allows predefined query ids (no raw SQL exposed).
 */
public class DbComponent {

    private final IAdapter adapter;
    private final ConnectionPool pool;
    private final Map<String, String> predefinedQueries;
    private final ThreadLocal<Connection> transactionConnection = new ThreadLocal<>();

    public DbComponent(
            IAdapter adapter,
            String host,
            int port,
            String database,
            String user,
            String password,
            int poolMinSize,
            int poolMaxSize,
            long poolScaleUpThresholdMs,
            long poolScaleDownThresholdMs,
            long poolAcquireTimeoutMs,
            String queryCatalogResource) {
        try {
            this.adapter = adapter;
            Class.forName(adapter.driverClassName());
            String jdbcUrl = adapter.buildJdbcUrl(host, port, database);
            this.pool = new ConnectionPool(
                    jdbcUrl,
                    user,
                    password,
                    poolMinSize,
                    poolMaxSize,
                    poolScaleUpThresholdMs,
                    poolScaleDownThresholdMs,
                    poolAcquireTimeoutMs,
                    null);
            this.predefinedQueries = QueryCatalog.load(queryCatalogResource);
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDBC driver not found: " + adapter.driverClassName(), e);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize connection pool", e);
        }
    }

    public DbQueryResult query(String queryId) throws SQLException, InterruptedException {
        String sql = resolveSql(queryId);

        Connection txConn = transactionConnection.get();
        if (txConn != null) {
            return execute(txConn, sql);
        }

        Connection conn = null;
        try {
            conn = pool.acquire();
            return execute(conn, sql);
        } finally {
            if (conn != null) {
                pool.release(conn);
            }
        }
    }

    public DbTransaction transaction() throws SQLException, InterruptedException {
        if (transactionConnection.get() != null) {
            throw new IllegalStateException("There is already an active transaction in this thread");
        }

        Connection conn = pool.acquire();
        conn.setAutoCommit(false);
        transactionConnection.set(conn);
        return new DbTransaction(conn);
    }

    public void shutdown() {
        pool.shutdown();
    }

    public String getAdapterName() {
        return adapter.dialectName();
    }

    public List<String> getQueryIds() {
        return new ArrayList<>(predefinedQueries.keySet());
    }

    public int getPoolTotalConexiones() {
        return pool.getTotalConexiones();
    }

    public int getPoolConexionesDisponibles() {
        return pool.getConexionesDisponibles();
    }

    public int getPoolConexionesEnUso() {
        return pool.getConexionesEnUso();
    }

    public final class DbTransaction implements AutoCloseable {
        private final Connection conn;
        private boolean active = true;

        private DbTransaction(Connection conn) {
            this.conn = conn;
        }

        public DbQueryResult query(String queryId) throws SQLException {
            ensureActive();
            return execute(conn, resolveSql(queryId));
        }

        public void commit() throws SQLException {
            ensureActive();
            conn.commit();
            close();
        }

        public void rollback() throws SQLException {
            ensureActive();
            conn.rollback();
            close();
        }

        @Override
        public void close() throws SQLException {
            if (!active) {
                return;
            }
            active = false;
            try {
                if (!conn.getAutoCommit()) {
                    conn.setAutoCommit(true);
                }
            } finally {
                transactionConnection.remove();
                pool.release(conn);
            }
        }

        private void ensureActive() {
            if (!active) {
                throw new IllegalStateException("Transaction already closed");
            }
        }
    }

    private String resolveSql(String queryId) {
        String sql = predefinedQueries.get(queryId);
        if (sql == null || sql.isBlank()) {
            throw new IllegalArgumentException("Unknown query id: " + queryId);
        }
        return sql;
    }

    private DbQueryResult execute(Connection conn, String sql) throws SQLException {
        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            boolean hasResultSet = stmt.execute();
            if (!hasResultSet) {
                return new DbQueryResult(false, stmt.getUpdateCount(), List.of());
            }
            try (ResultSet rs = stmt.getResultSet()) {
                return new DbQueryResult(true, -1, mapRows(rs));
            }
        }
    }

    private List<Map<String, Object>> mapRows(ResultSet rs) throws SQLException {
        ResultSetMetaData meta = rs.getMetaData();
        int columns = meta.getColumnCount();
        List<Map<String, Object>> rows = new ArrayList<>();

        while (rs.next()) {
            Map<String, Object> row = new LinkedHashMap<>();
            for (int i = 1; i <= columns; i++) {
                row.put(meta.getColumnLabel(i), rs.getObject(i));
            }
            rows.add(row);
        }
        return rows;
    }
}

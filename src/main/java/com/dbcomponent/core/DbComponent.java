package com.dbcomponent.core;

import com.dbcomponent.adapter.IAdapter;
import com.dbcomponent.model.DbQueryResult;
import com.dbcomponent.pool.ConnectionPool;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * DbComponent desacoplado por adapter y respaldado por pool interno.
 * La API publica solo permite query ids predefinidos (sin SQL crudo expuesto).
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

            Connection catalogConnection = null;
            try {
                catalogConnection = pool.acquire();
                ensureDemoDataIfNeeded(catalogConnection);
                this.predefinedQueries = QueryCatalog.loadAdaptive(queryCatalogResource, catalogConnection);
            } finally {
                if (catalogConnection != null) {
                    pool.release(catalogConnection);
                }
            }
        } catch (ClassNotFoundException e) {
            throw new IllegalStateException("JDBC driver not found: " + adapter.driverClassName(), e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Interrupted while initializing query catalog", e);
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot initialize connection pool", e);
        }
    }

    public DbQueryResult query(String queryId) throws SQLException, InterruptedException {
        return query(queryId, new Object[0]);
    }

    public DbQueryResult query(String queryId, Object... params) throws SQLException, InterruptedException {
        String sql = resolveSql(queryId);

        Connection txConn = transactionConnection.get();
        if (txConn != null) {
            return execute(txConn, sql, params);
        }

        Connection conn = null;
        try {
            conn = pool.acquire();
            return execute(conn, sql, params);
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
            return query(queryId, new Object[0]);
        }

        public DbQueryResult query(String queryId, Object... params) throws SQLException {
            ensureActive();
            return execute(conn, resolveSql(queryId), params);
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

    private DbQueryResult execute(Connection conn, String sql, Object... params) throws SQLException {
        int expectedParams = countPositionalParameters(sql);
        int providedParams = params == null ? 0 : params.length;
        if (expectedParams != providedParams) {
            throw new IllegalArgumentException("Query expects " + expectedParams
                    + " params but received " + providedParams);
        }

        try (PreparedStatement stmt = conn.prepareStatement(sql)) {
            if (params != null) {
                for (int i = 0; i < params.length; i++) {
                    stmt.setObject(i + 1, params[i]);
                }
            }
            boolean hasResultSet = stmt.execute();
            if (!hasResultSet) {
                return new DbQueryResult(false, stmt.getUpdateCount(), List.of());
            }
            try (ResultSet rs = stmt.getResultSet()) {
                return new DbQueryResult(true, -1, mapRows(rs));
            }
        }
    }

    private int countPositionalParameters(String sql) {
        int count = 0;
        boolean inSingleQuote = false;
        boolean inDoubleQuote = false;

        for (int i = 0; i < sql.length(); i++) {
            char c = sql.charAt(i);

            if (c == '\'' && !inDoubleQuote) {
                if (inSingleQuote && i + 1 < sql.length() && sql.charAt(i + 1) == '\'') {
                    i++;
                    continue;
                }
                inSingleQuote = !inSingleQuote;
                continue;
            }

            if (c == '"' && !inSingleQuote) {
                inDoubleQuote = !inDoubleQuote;
                continue;
            }

            if (c == '?' && !inSingleQuote && !inDoubleQuote) {
                count++;
            }
        }

        return count;
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

    private void ensureDemoDataIfNeeded(Connection conn) throws SQLException {
        if (!"H2".equalsIgnoreCase(adapter.dialectName())
                && !"SQLite".equalsIgnoreCase(adapter.dialectName())) {
            return;
        }
        if (hasUserTables(conn)) {
            return;
        }

        try (Statement st = conn.createStatement()) {
            st.executeUpdate("CREATE TABLE IF NOT EXISTS productos ("
                    + "id INT PRIMARY KEY, "
                    + "nombre VARCHAR(120), "
                    + "precio DECIMAL(10,2), "
                    + "stock INT)");

            st.executeUpdate("CREATE TABLE IF NOT EXISTS pedidos ("
                    + "id INT PRIMARY KEY, "
                    + "cliente_id INT, "
                    + "estado VARCHAR(50), "
                    + "creado_en TIMESTAMP)");

                st.executeUpdate("DELETE FROM productos");
                st.executeUpdate("DELETE FROM pedidos");

                st.executeUpdate("INSERT INTO productos (id, nombre, precio, stock) VALUES "
                    + "(1, 'Mate', 2500.00, 80), "
                    + "(2, 'Termo', 12000.00, 20), "
                    + "(3, 'Bombilla', 3000.00, 35)");

                st.executeUpdate("INSERT INTO pedidos (id, cliente_id, estado, creado_en) VALUES "
                    + "(1, 10, 'pendiente', CURRENT_TIMESTAMP), "
                    + "(2, 11, 'completado', CURRENT_TIMESTAMP)");
        }
    }

    private boolean hasUserTables(Connection conn) throws SQLException {
        DatabaseMetaData meta = conn.getMetaData();
        String[] types = { "TABLE" };
        try (ResultSet rs = meta.getTables(conn.getCatalog(), null, "%", types)) {
            while (rs.next()) {
                String schema = rs.getString("TABLE_SCHEM");
                if (schema == null) {
                    return true;
                }
                String s = schema.trim().toUpperCase();
                if (!s.startsWith("INFORMATION_SCHEMA") && !s.startsWith("PG_")) {
                    return true;
                }
            }
        }
        return false;
    }
}

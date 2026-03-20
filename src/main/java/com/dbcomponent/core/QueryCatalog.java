package com.dbcomponent.core;

import java.io.InputStream;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

final class QueryCatalog {

    private QueryCatalog() {
    }

    static Map<String, String> load(String resourcePath) {
        Properties props = new Properties();
        try (InputStream is = QueryCatalog.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalArgumentException("Query catalog not found: " + resourcePath);
            }
            props.load(is);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load query catalog: " + resourcePath, e);
        }

        Map<String, String> queries = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            String sql = props.getProperty(key);
            if (sql != null && !sql.isBlank()) {
                queries.put(key.trim(), sql.trim());
            }
        }

        if (queries.isEmpty()) {
            throw new IllegalStateException("Query catalog is empty: " + resourcePath);
        }

        return Map.copyOf(queries);
    }

    static Map<String, String> loadAdaptive(String resourcePath, Connection connection) {
        Map<String, String> adaptive = buildFromDatabaseMetadata(connection);
        Map<String, String> merged = new LinkedHashMap<>(adaptive);

        try {
            Map<String, String> fromFile = load(resourcePath);
            // Prioritize user-defined query ids from file when collisions occur.
            merged.putAll(fromFile);
        } catch (Exception ignored) {
            // Keep adaptive catalog if file does not exist or is invalid.
        }

        if (merged.isEmpty()) {
            throw new IllegalStateException("No queries available from resource or database metadata");
        }

        return Map.copyOf(merged);
    }

    private static Map<String, String> buildFromDatabaseMetadata(Connection connection) {
        List<TableRef> tables = discoverTables(connection);
        Map<String, String> queries = new LinkedHashMap<>();

        for (TableRef table : tables) {
            String suffix = toQuerySuffix(table.tableName());
            String tableRef = toSqlTableReference(table, connection);
            queries.put("auto." + suffix + ".listar", "SELECT * FROM " + tableRef + " LIMIT 10");
            queries.put("auto." + suffix + ".contar", "SELECT COUNT(*) AS total FROM " + tableRef);
        }

        return queries;
    }

    private static List<TableRef> discoverTables(Connection connection) {
        try {
            DatabaseMetaData meta = connection.getMetaData();
            String catalog = connection.getCatalog();
            List<TableRef> tables = new ArrayList<>();

            try (ResultSet rs = meta.getTables(catalog, null, "%", new String[] { "TABLE" })) {
                while (rs.next()) {
                    String schema = rs.getString("TABLE_SCHEM");
                    String table = rs.getString("TABLE_NAME");
                    if (table == null || table.isBlank()) {
                        continue;
                    }
                    if (isSystemSchema(schema)) {
                        continue;
                    }
                    tables.add(new TableRef(schema, table));
                }
            }

            tables.sort(Comparator
                    .comparing((TableRef t) -> t.schema() == null ? "" : t.schema())
                    .thenComparing(TableRef::tableName));
            return tables;
        } catch (SQLException e) {
            throw new IllegalStateException("Cannot inspect database metadata for adaptive queries", e);
        }
    }

    private static boolean isSystemSchema(String schema) {
        if (schema == null || schema.isBlank()) {
            return false;
        }
        String s = schema.trim().toLowerCase();
        return s.startsWith("pg_")
                || "information_schema".equals(s)
                || "sys".equals(s)
                || "system".equals(s);
    }

    private static String toSqlTableReference(TableRef table, Connection connection) {
        String quote = "\"";
        try {
            String q = connection.getMetaData().getIdentifierQuoteString();
            if (q != null && !q.isBlank()) {
                quote = q.trim();
            }
        } catch (SQLException ignored) {
        }

        String quotedTable = quoteIdentifier(table.tableName(), quote);
        if (table.schema() == null || table.schema().isBlank()) {
            return quotedTable;
        }
        return quoteIdentifier(table.schema(), quote) + "." + quotedTable;
    }

    private static String quoteIdentifier(String value, String quote) {
        String escaped = value.replace(quote, quote + quote);
        return quote + escaped + quote;
    }

    private static String toQuerySuffix(String tableName) {
        String normalized = tableName.trim().toLowerCase().replaceAll("[^a-z0-9]+", "_");
        return normalized.replaceAll("^_+|_+$", "");
    }

    private record TableRef(String schema, String tableName) {
    }
}

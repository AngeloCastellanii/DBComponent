package com.dbcomponent.core;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.yaml.snakeyaml.Yaml;
import org.yaml.snakeyaml.error.YAMLException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.NodeList;

import javax.xml.parsers.DocumentBuilderFactory;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
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

    private static final ObjectMapper JSON_MAPPER = new ObjectMapper();
    private static final TypeReference<LinkedHashMap<String, String>> MAP_TYPE = new TypeReference<>() {
    };

    private QueryCatalog() {
    }

    static Map<String, String> load(String resourcePath) {
        String normalizedPath = resourcePath == null ? "" : resourcePath.trim();
        if (normalizedPath.isEmpty()) {
            throw new IllegalArgumentException("Query catalog path cannot be empty");
        }

        String extension = fileExtension(normalizedPath);
        return switch (extension) {
            case "json" -> loadJson(normalizedPath);
            case "yaml", "yml" -> loadYaml(normalizedPath);
            case "xml" -> loadXml(normalizedPath);
            case "sql" -> loadPlainSql(normalizedPath);
            default -> loadProperties(normalizedPath);
        };
    }

    static Map<String, String> loadAdaptive(String resourcePath, Connection connection) {
        Map<String, String> adaptive = buildFromDatabaseMetadata(connection);
        Map<String, String> merged = new LinkedHashMap<>(adaptive);

        try {
            Map<String, String> fromFile = load(resourcePath);
            // Prioriza query ids definidos por el usuario cuando hay colisiones.
            merged.putAll(fromFile);
        } catch (Exception ignored) {
            // Mantiene el catalogo adaptativo si el archivo no existe o es invalido.
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
            queries.put("auto." + suffix + ".listar", "SELECT * FROM " + tableRef + " LIMIT ?");
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

    private static Map<String, String> loadProperties(String resourcePath) {
        Properties props = new Properties();
        try (InputStream is = resourceStream(resourcePath)) {
            props.load(is);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load properties query catalog: " + resourcePath, e);
        }

        Map<String, String> queries = new LinkedHashMap<>();
        for (String key : props.stringPropertyNames()) {
            queries.put(key, props.getProperty(key));
        }
        return validateAndFreeze(resourcePath, queries);
    }

    private static Map<String, String> loadJson(String resourcePath) {
        try (InputStream is = resourceStream(resourcePath)) {
            Map<String, String> queries = JSON_MAPPER.readValue(is, MAP_TYPE);
            return validateAndFreeze(resourcePath, queries);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load json query catalog: " + resourcePath, e);
        }
    }

    private static Map<String, String> loadYaml(String resourcePath) {
        try (InputStream is = resourceStream(resourcePath)) {
            Object loaded = new Yaml().load(is);
            if (!(loaded instanceof Map<?, ?> rawMap)) {
                throw new IllegalStateException("YAML root must be a key/value map");
            }
            Map<String, String> queries = new LinkedHashMap<>();
            for (Map.Entry<?, ?> entry : rawMap.entrySet()) {
                queries.put(String.valueOf(entry.getKey()), entry.getValue() == null ? null : String.valueOf(entry.getValue()));
            }
            return validateAndFreeze(resourcePath, queries);
        } catch (YAMLException e) {
            throw new IllegalStateException("Invalid YAML query catalog: " + resourcePath, e);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load yaml query catalog: " + resourcePath, e);
        }
    }

    private static Map<String, String> loadXml(String resourcePath) {
        try (InputStream is = resourceStream(resourcePath)) {
            DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
            factory.setFeature("http://apache.org/xml/features/disallow-doctype-decl", true);
            Document doc = factory.newDocumentBuilder().parse(is);
            doc.getDocumentElement().normalize();

            Map<String, String> queries = new LinkedHashMap<>();
            NodeList queryNodes = doc.getElementsByTagName("query");
            for (int i = 0; i < queryNodes.getLength(); i++) {
                Element el = (Element) queryNodes.item(i);
                String id = el.getAttribute("id");
                String sql = el.getTextContent();
                queries.put(id, sql);
            }

            NodeList entryNodes = doc.getElementsByTagName("entry");
            for (int i = 0; i < entryNodes.getLength(); i++) {
                Element el = (Element) entryNodes.item(i);
                String key = el.getAttribute("key");
                String sql = el.getTextContent();
                queries.put(key, sql);
            }

            return validateAndFreeze(resourcePath, queries);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load xml query catalog: " + resourcePath, e);
        }
    }

    private static Map<String, String> loadPlainSql(String resourcePath) {
        String content;
        try (InputStream is = resourceStream(resourcePath)) {
            content = new String(is.readAllBytes(), StandardCharsets.UTF_8);
        } catch (Exception e) {
            throw new IllegalStateException("Cannot load sql query catalog: " + resourcePath, e);
        }

        Map<String, String> queries = new LinkedHashMap<>();
        String currentId = null;
        StringBuilder currentSql = new StringBuilder();

        for (String rawLine : content.split("\\R")) {
            String line = rawLine.trim();
            if (line.startsWith("-- id:") || line.startsWith("# id:")) {
                if (currentId != null) {
                    queries.put(currentId, currentSql.toString().trim());
                }
                currentId = line.substring(line.indexOf(':') + 1).trim();
                currentSql.setLength(0);
                continue;
            }
            if (currentId != null) {
                currentSql.append(rawLine).append('\n');
            }
        }
        if (currentId != null) {
            queries.put(currentId, currentSql.toString().trim());
        }

        return validateAndFreeze(resourcePath, queries);
    }

    private static Map<String, String> validateAndFreeze(String resourcePath, Map<String, String> rawQueries) {
        Map<String, String> clean = new LinkedHashMap<>();
        if (rawQueries != null) {
            for (Map.Entry<String, String> entry : rawQueries.entrySet()) {
                String key = entry.getKey() == null ? "" : entry.getKey().trim();
                String sql = entry.getValue() == null ? "" : entry.getValue().trim();
                if (!key.isEmpty() && !sql.isEmpty()) {
                    clean.put(key, sql);
                }
            }
        }

        if (clean.isEmpty()) {
            throw new IllegalStateException("Query catalog is empty: " + resourcePath);
        }
        return Map.copyOf(clean);
    }

    private static InputStream resourceStream(String resourcePath) {
        InputStream is = QueryCatalog.class.getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) {
            throw new IllegalArgumentException("Query catalog not found: " + resourcePath);
        }
        return is;
    }

    private static String fileExtension(String path) {
        int idx = path.lastIndexOf('.');
        if (idx < 0 || idx == path.length() - 1) {
            return "";
        }
        return path.substring(idx + 1).toLowerCase();
    }

    private record TableRef(String schema, String tableName) {
    }
}

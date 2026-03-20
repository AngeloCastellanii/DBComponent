package com.dbcomponent.core;

import java.io.InputStream;
import java.util.LinkedHashMap;
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
}

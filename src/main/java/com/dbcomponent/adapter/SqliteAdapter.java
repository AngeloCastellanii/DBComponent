package com.dbcomponent.adapter;

public class SqliteAdapter extends IAdapter {

    @Override
    public String dialectName() {
        return "SQLite";
    }

    @Override
    public String driverClassName() {
        return "org.sqlite.JDBC";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        String dbFile = (database == null || database.isBlank()) ? "sqlite-demo.db" : database.trim();
        return "jdbc:sqlite:" + dbFile;
    }
}
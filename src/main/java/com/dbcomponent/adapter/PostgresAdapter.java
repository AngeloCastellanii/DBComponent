package com.dbcomponent.adapter;

public class PostgresAdapter implements IAdapter {

    @Override
    public String dialectName() {
        return "PostgreSQL";
    }

    @Override
    public String driverClassName() {
        return "org.postgresql.Driver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:postgresql://" + host + ":" + port + "/" + database;
    }
}

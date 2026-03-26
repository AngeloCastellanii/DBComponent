package com.dbcomponent.adapter;

public abstract class IAdapter {

    public abstract String dialectName();

    public abstract String driverClassName();

    public abstract String buildJdbcUrl(String host, int port, String database);
}

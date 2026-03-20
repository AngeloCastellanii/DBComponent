package com.dbcomponent.adapter;

public interface IAdapter {

    String dialectName();

    String driverClassName();

    String buildJdbcUrl(String host, int port, String database);
}

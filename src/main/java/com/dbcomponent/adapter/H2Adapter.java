package com.dbcomponent.adapter;

public class H2Adapter implements IAdapter {

    @Override
    public String dialectName() {
        return "H2";
    }

    @Override
    public String driverClassName() {
        return "org.h2.Driver";
    }

    @Override
    public String buildJdbcUrl(String host, int port, String database) {
        return "jdbc:h2:tcp://" + host + ":" + port + "/~/" + database;
    }
}

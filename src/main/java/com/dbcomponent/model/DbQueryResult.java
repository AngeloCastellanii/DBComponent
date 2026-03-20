package com.dbcomponent.model;

import java.util.List;
import java.util.Map;

public class DbQueryResult {

    private final boolean hasResultSet;
    private final int updatedRows;
    private final List<Map<String, Object>> rows;

    public DbQueryResult(boolean hasResultSet, int updatedRows, List<Map<String, Object>> rows) {
        this.hasResultSet = hasResultSet;
        this.updatedRows = updatedRows;
        this.rows = rows;
    }

    public boolean hasResultSet() {
        return hasResultSet;
    }

    public int getUpdatedRows() {
        return updatedRows;
    }

    public List<Map<String, Object>> getRows() {
        return rows;
    }
}

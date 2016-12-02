package com.oracle.tutorial.jdbc;

import com.sun.istack.internal.NotNull;

/**
 * Created by Gregory Dony on 02/12/2016.
 */
public enum JDBCDataSource {

    DB2("derby", "org.apache.derby.jdbc.EmbeddedDriver", "properties/javadb-sample-properties.xml"),
    MYSQL("mysql", "com.mysql.jdbc.Driver", "properties/mysql-sample-properties.xml"),
    POSTGRESQL("postgresql", "org.postgresql.Driver", "properties/postgresql-sample-properties.xml"),;

    private transient String dbms;
    private transient String jdbcDriver;
    private transient String propertyFilePath;

    JDBCDataSource(String dbms, String jdbcDriver, String propertyFilePath) {
        this.dbms = dbms;
        this.jdbcDriver = jdbcDriver;
        this.propertyFilePath = propertyFilePath;
    }

    public String getDbms() {
        return dbms;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public String getPropertyFilePath() {
        return propertyFilePath;
    }

    public static
    @NotNull
    JDBCDataSource fromDbms(@NotNull String dbms) {
        for (JDBCDataSource jdbcDataSource : JDBCDataSource.values()) {
            if (dbms.equals(jdbcDataSource.getDbms())) {
                return jdbcDataSource;
            }
        }
        throw new IllegalArgumentException("Invalid dbms value : " + dbms);
    }
}

package com.oracle.tutorial.jdbc;

import com.sun.istack.internal.NotNull;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

/**
 * Created by Gregory Dony on 02/12/2016.
 */
public enum JdbcDataSource {

    DB2("derby", "org.apache.derby.jdbc.EmbeddedDriver", "properties/javadb-sample-properties.xml"),
    MYSQL("mysql", "com.mysql.jdbc.Driver", "properties/mysql-sample-properties.xml"),
    POSTGRESQL("postgresql", "org.postgresql.Driver", "properties/postgresql-sample-properties.xml"),;

    private static final String DBMS_PLACEHOLDER = "%dbms%";
    private static final String SERVER_NAME_PLACEHOLDER = "%serverName%";
    private static final String PORT_NUMBER_PLACEHOLDER = "%portNumber%";
    private static final String DB_NAME_PLACEHOLDER = "%dbName%";

    public static
    @NotNull
    JdbcDataSource fromDbms(@NotNull String dbms) {
        for (JdbcDataSource jdbcDataSource : JdbcDataSource.values()) {
            if (dbms.equals(jdbcDataSource.getDbms())) {
                return jdbcDataSource;
            }
        }
        throw new IllegalArgumentException("Invalid dbms value : " + dbms);
    }

    private transient String dbms;
    private transient String jdbcDriver;
    private transient Properties properties;

    JdbcDataSource(String dbms, String jdbcDriver, String propertyFilePath) {
        this.dbms = dbms;
        this.jdbcDriver = jdbcDriver;
        this.properties = new Properties();
        FileInputStream fis = null;
        try {
            fis = new FileInputStream(propertyFilePath);
            properties.loadFromXML(fis);
        } catch (IOException ioe) {
            throw new RuntimeException("Unable to read properties from file " + propertyFilePath, ioe);
        }
    }

    public String getDbms() {
        return dbms;
    }

    public String getJdbcDriver() {
        return jdbcDriver;
    }

    public String getDbName() {
        return properties.getProperty("database_name");
    }

    public String getServerName() {
        return properties.getProperty("server_name");
    }

    public int getPortNumber() {
        return Integer.parseInt(properties.getProperty("port_number"));
    }

    public String getUserName() {
        return properties.getProperty("user_name");
    }

    public String getPassword() {
        return properties.getProperty("password");
    }
}

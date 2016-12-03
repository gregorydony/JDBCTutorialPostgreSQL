package com.oracle.tutorial.jdbc;


import com.sun.istack.internal.Nullable;

import java.sql.Connection;

/**
 * Created by Gregory Dony on 02/12/2016.
 */
public abstract class AbstractJdbcSample {

    protected final Connection con;
    protected final JdbcDataSource jdbcDataSource;


    public AbstractJdbcSample(Connection connArg,
                              JdbcDataSource jdbcDataSource) {
        this.con = connArg;
        this.jdbcDataSource = jdbcDataSource;
    }


    protected static JdbcDataSource getJdbcDataSource(@Nullable String arg) {
        if (arg == null) {
            throw new IllegalArgumentException("Properties file not specified at command line");
        }
        return JdbcDataSource.fromDbms(arg);
    }
}

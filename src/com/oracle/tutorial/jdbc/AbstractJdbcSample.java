package com.oracle.tutorial.jdbc;

import java.sql.Connection;

/**
 * Created by Elyse on 02/12/2016.
 */
public abstract class AbstractJdbcSample {

    protected final String dbName;
    protected final Connection con;
    protected final String dbms;
    protected final JDBCTutorialUtilities settings;


    public AbstractJdbcSample(Connection connArg,
                              JDBCTutorialUtilities settingsArg) {
        this.con = connArg;
        this.dbName = settingsArg.dbName;
        this.dbms = settingsArg.dbms;
        this.settings = settingsArg;
    }
}

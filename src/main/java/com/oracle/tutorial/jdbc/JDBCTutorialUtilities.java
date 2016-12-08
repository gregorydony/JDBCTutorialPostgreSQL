/*
 * Copyright (c) 1995, 2011, Oracle and/or its affiliates. All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions
 * are met:
 *
 *   - Redistributions of source code must retain the above copyright
 *     notice, this list of conditions and the following disclaimer.
 *
 *   - Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *
 *   - Neither the name of Oracle or the names of its
 *     contributors may be used to endorse or promote products derived
 *     from this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS
 * IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
 * THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
 * PURPOSE ARE DISCLAIMED.  IN NO EVENT SHALL THE COPYRIGHT OWNER OR
 * CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL,
 * EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO,
 * PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR
 * PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF
 * LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING
 * NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
 * SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

package com.oracle.tutorial.jdbc;

import org.w3c.dom.Document;

import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerConfigurationException;
import javax.xml.transform.TransformerException;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import java.io.StringWriter;
import java.sql.*;
import java.util.Properties;

import static java.sql.RowIdLifetime.ROWID_UNSUPPORTED;

public final class JDBCTutorialUtilities {

    public static void initializeTables(Connection con, JdbcDataSource jdbcDataSource) throws SQLException {
        SuppliersTable mySuppliersTable =
                new SuppliersTable(con, jdbcDataSource);
        CoffeesTable myCoffeeTable =
                new CoffeesTable(con, jdbcDataSource.getDbName(), jdbcDataSource);
        RSSFeedsTable myRSSFeedsTable =
                new RSSFeedsTable(con, jdbcDataSource);
        ProductInformationTable myPIT =
                new ProductInformationTable(con, jdbcDataSource);

        System.out.println("\nDropping exisiting PRODUCT_INFORMATION, COFFEES and SUPPLIERS tables");
        myPIT.dropTable();
        myRSSFeedsTable.dropTable();
        myCoffeeTable.dropTable();
        mySuppliersTable.dropTable();

        System.out.println("\nCreating and populating SUPPLIERS table...");

        System.out.println("\nCreating SUPPLIERS table");
        mySuppliersTable.createTable();
        System.out.println("\nPopulating SUPPLIERS table");
        mySuppliersTable.populateTable();

        System.out.println("\nCreating and populating COFFEES table...");

        System.out.println("\nCreating COFFEES table");
        myCoffeeTable.createTable();
        System.out.println("\nPopulating COFFEES table");
        myCoffeeTable.populateTable();

        System.out.println("\nCreating RSS_FEEDS table...");
        myRSSFeedsTable.createTable();
    }

    public static void rowIdLifetime(Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        RowIdLifetime lifetime;
        try {
            lifetime = dbMetaData.getRowIdLifetime();
        } catch (SQLFeatureNotSupportedException e) {
            lifetime = ROWID_UNSUPPORTED;
        }
        switch (lifetime) {
            case ROWID_UNSUPPORTED:
                System.out.println("ROWID type not supported");
                break;
            case ROWID_VALID_FOREVER:
                System.out.println("ROWID has unlimited lifetime");
                break;
            case ROWID_VALID_OTHER:
                System.out.println("ROWID has indeterminate lifetime");
                break;
            case ROWID_VALID_SESSION:
                System.out.println("ROWID type has lifetime that is valid for at least the containing session");
                break;
            case ROWID_VALID_TRANSACTION:
                System.out.println("ROWID type has lifetime that is valid for at least the containing transaction");
        }
    }


    public static void cursorHoldabilitySupport(Connection conn) throws SQLException {
        DatabaseMetaData dbMetaData = conn.getMetaData();
        System.out.println("ResultSet.HOLD_CURSORS_OVER_COMMIT = " +
                ResultSet.HOLD_CURSORS_OVER_COMMIT);
        System.out.println("ResultSet.CLOSE_CURSORS_AT_COMMIT = " +
                ResultSet.CLOSE_CURSORS_AT_COMMIT);
        System.out.println("Default cursor holdability: " +
                dbMetaData.getResultSetHoldability());
        System.out.println("Supports HOLD_CURSORS_OVER_COMMIT? " +
                dbMetaData.supportsResultSetHoldability(ResultSet.HOLD_CURSORS_OVER_COMMIT));
        System.out.println("Supports CLOSE_CURSORS_AT_COMMIT? " +
                dbMetaData.supportsResultSetHoldability(ResultSet.CLOSE_CURSORS_AT_COMMIT));
    }

    public static void getWarningsFromResultSet(ResultSet rs) throws SQLException {
        JDBCTutorialUtilities.printWarnings(rs.getWarnings());
    }

    public static void getWarningsFromStatement(Statement stmt) throws SQLException {
        JDBCTutorialUtilities.printWarnings(stmt.getWarnings());
    }

    public static void printWarnings(SQLWarning warning) throws SQLException {
        if (warning != null) {
            System.out.println("\n---Warning---\n");
            while (warning != null) {
                System.out.println("Message: " + warning.getMessage());
                System.out.println("SQLState: " + warning.getSQLState());
                System.out.print("Vendor error code: ");
                System.out.println(warning.getErrorCode());
                System.out.println("");
                warning = warning.getNextWarning();
            }
        }
    }

    public static boolean ignoreSQLException(String sqlState) {
        if (sqlState == null) {
            System.out.println("The SQL state is not defined!");
            return false;
        }
        // X0Y32: Jar file already exists in schema
        if (sqlState.equalsIgnoreCase("X0Y32"))
            return true;
        // 42Y55: Table already exists in schema
        if (sqlState.equalsIgnoreCase("42Y55"))
            return true;
        return false;
    }

    public static void printBatchUpdateException(BatchUpdateException b) {
        System.err.println("----BatchUpdateException----");
        System.err.println("SQLState:  " + b.getSQLState());
        System.err.println("Message:  " + b.getMessage());
        System.err.println("Vendor:  " + b.getErrorCode());
        System.err.print("Update counts:  ");
        int[] updateCounts = b.getUpdateCounts();
        for (int i = 0; i < updateCounts.length; i++) {
            System.err.print(updateCounts[i] + "   ");
        }
    }

    public static void printSQLException(SQLException ex) {
        for (Throwable e : ex) {
            if (e instanceof SQLException) {
                if (ignoreSQLException(((SQLException) e).getSQLState()) == false) {
                    e.printStackTrace(System.err);
                    System.err.println("SQLState: " + ((SQLException) e).getSQLState());
                    System.err.println("Error Code: " + ((SQLException) e).getErrorCode());
                    System.err.println("Message: " + e.getMessage());
                    Throwable t = ex.getCause();
                    while (t != null) {
                        System.out.println("Cause: " + t);
                        t = t.getCause();
                    }
                }
            }
        }
    }

    public static Connection getConnectionToDatabase(JdbcDataSource jdbcDataSource, boolean createIfAbsent) throws SQLException {
        {
            Connection conn = null;
            Properties connectionProps = new Properties();
            connectionProps.put("user", jdbcDataSource.getUserName());
            connectionProps.put("password", jdbcDataSource.getPassword());

            final String serverName = jdbcDataSource.getServerName();
            final String dbName = jdbcDataSource.getDbName();
            switch (jdbcDataSource) {
                case POSTGRESQL:
                    DriverManager.registerDriver(new org.postgresql.Driver());
                    //jdbc:postgresql://192.168.99.100:5432/postgres
                    conn =
                            DriverManager.getConnection(getConnectionUrl(jdbcDataSource,createIfAbsent),
                                    connectionProps);
                    conn.setCatalog(dbName);
                    break;
                case MYSQL:
                    DriverManager.registerDriver(new com.mysql.jdbc.Driver());
                    conn =
                            DriverManager.getConnection(getConnectionUrl(jdbcDataSource,createIfAbsent),
                                    connectionProps);
                    conn.setCatalog(dbName);
                    break;
                case DERBY:
                    //DriverManager.registerDriver(new org.apache.derby.jdbc.EmbeddedDriver());
                    conn =
                            DriverManager.getConnection(getConnectionUrl(jdbcDataSource,createIfAbsent), connectionProps);
                    break;
            }
            System.out.println("Connected to database");
            return conn;
        }
    }

    public static String getConnectionUrl(JdbcDataSource jdbcDataSource, boolean createIfAbsent) {
        final String dbms = jdbcDataSource.getDbms();
        final String serverName = jdbcDataSource.getServerName();
        final int portNumber = jdbcDataSource.getPortNumber();
        final String dbName = jdbcDataSource.getDbName();
        switch (jdbcDataSource) {
            case DERBY:
                return "jdbc:" + dbms + ":" + dbName + (createIfAbsent ? ";create=true" : "");
            default :
                return "jdbc:" + dbms + "://" + serverName +
                        ":" + portNumber + "/" + dbName;

        }
    }

    public static void createDatabase(Connection connArg, String dbNameArg,
                                      String dbmsArg) {

        if (dbmsArg.equals("mysql")) {
            try {
                Statement s = connArg.createStatement();
                String newDatabaseString =
                        "CREATE DATABASE IF NOT EXISTS " + dbNameArg;
                // String newDatabaseString = "CREATE DATABASE " + dbName;
                s.executeUpdate(newDatabaseString);

                System.out.println("Created database " + dbNameArg);
            } catch (SQLException e) {
                printSQLException(e);
            }
        }
    }

    public static String convertDocumentToString(Document doc) throws TransformerConfigurationException,
            TransformerException {
        Transformer t = TransformerFactory.newInstance().newTransformer();
//    t.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
        StringWriter sw = new StringWriter();
        t.transform(new DOMSource(doc), new StreamResult(sw));
        return sw.toString();


    }

    public static void main(String[] args) {
        JdbcDataSource jdbcDataSource;
        if (args[0] == null) {
            System.err.println("Properties file not specified at command line");
            return;
        } else {
            try {
                System.out.println("Reading properties file " + args[0]);
                jdbcDataSource = JdbcDataSource.fromDbms(args[0]);
            } catch (Exception e) {
                System.err.println("Problem reading properties file " + args[0]);
                e.printStackTrace();
                return;
            }
        }

        try (Connection myConnection = getConnectionToDatabase(jdbcDataSource, true)){
            //      JDBCTutorialUtilities.outputClientInfoProperties(myConnection);
            // myConnection = myJDBCTutorialUtilities.getConnection("root", "root", "jdbc:mysql://localhost:3306/");
            //       myConnection = myJDBCTutorialUtilities.
            //         getConnectionWithDataSource(myJDBCTutorialUtilities.dbName,"derby","", "", "localhost", 3306);

            // Java DB does not have an SQL create database command; it does require createDatabase
            createDatabase(myConnection,
                    jdbcDataSource.getDbName(),
                    jdbcDataSource.getDbms());

            cursorHoldabilitySupport(myConnection);
            rowIdLifetime(myConnection);

        } catch (SQLException e) {
            printSQLException(e);
        } catch (Exception e) {
            e.printStackTrace(System.err);
        }
    }
}

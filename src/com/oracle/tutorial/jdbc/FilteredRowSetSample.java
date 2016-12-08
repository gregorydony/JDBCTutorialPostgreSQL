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

import com.sun.rowset.FilteredRowSetImpl;

import javax.sql.rowset.CachedRowSet;
import javax.sql.rowset.FilteredRowSet;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;

public class FilteredRowSetSample extends AbstractJdbcSample {

    public FilteredRowSetSample(Connection connArg,
                                JdbcDataSource jdbcDataSource) {
        super(connArg, jdbcDataSource);
    }

    private void viewFilteredRowSet(FilteredRowSet frs) throws SQLException {

        if (frs == null) {
            return;
        }

        CachedRowSet crs = (CachedRowSet) frs;

        while (crs.next()) {
            if (crs == null) {
                break;
            }
            System.out.println(
                    crs.getInt("STORE_ID") + ", " +
                            crs.getString("CITY") + ", " +
                            crs.getInt("COFFEE") + ", " +
                            crs.getInt("MERCH") + ", " +
                            crs.getInt("TOTAL"));
        }
    }

    public static void viewTable(Connection con) throws SQLException {
        String query = "select * from COFFEE_HOUSES";
        try ( Statement stmt = con.createStatement()){
            ResultSet rs = stmt.executeQuery(query);
            while (rs.next()) {
                System.out.println(rs.getInt("STORE_ID") + ", " +
                        rs.getString("CITY") + ", " + rs.getInt("COFFEE") +
                        ", " + rs.getInt("MERCH") + ", " +
                        rs.getInt("TOTAL"));
            }
        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        }
    }

    public void testFilteredRowSet() {
        FilteredRowSet frs = null;
        StateFilter myStateFilter = new StateFilter(10000, 10999, 1);
        String[] cityArray = {"SF", "LA"};

        CityFilter myCityFilter = new CityFilter(cityArray, 2);

        try {
            frs = new FilteredRowSetImpl();

            frs.setCommand("SELECT * FROM COFFEE_HOUSES");
            frs.setUsername(jdbcDataSource.getUserName());
            frs.setPassword(jdbcDataSource.getPassword());
            frs.setUrl(JDBCTutorialUtilities.getConnectionUrl(jdbcDataSource,false));
            frs.execute();

            System.out.println("\nBefore filter:");
            FilteredRowSetSample.viewTable(this.con);

            System.out.println("\nSetting state filter:");
            frs.beforeFirst();
            frs.setFilter(myStateFilter);
            this.viewFilteredRowSet(frs);

            System.out.println("\nSetting city filter:");
            frs.beforeFirst();
            frs.setFilter(myCityFilter);
            this.viewFilteredRowSet(frs);

        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        }
    }

    public static void main(String[] args) {
        JdbcDataSource jdbcDataSource = getJdbcDataSource(args[0]);

        try (Connection myConnection = JDBCTutorialUtilities.getConnectionToDatabase(jdbcDataSource, false)){
            FilteredRowSetSample myFilteredRowSetSample =
                    new FilteredRowSetSample(myConnection, jdbcDataSource);
            myFilteredRowSetSample.testFilteredRowSet();
        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        } catch (Exception ex) {
            System.out.println("Unexpected exception");
            ex.printStackTrace();
        }
    }
}

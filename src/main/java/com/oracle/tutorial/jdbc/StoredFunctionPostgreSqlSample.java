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

import java.math.BigDecimal;
import java.sql.*;

public final class StoredFunctionPostgreSqlSample extends AbstractJdbcSample {

    public StoredFunctionPostgreSqlSample(Connection connArg,
                                          JdbcDataSource jdbcDataSource) {
        super(connArg, jdbcDataSource);
    }

    private void createFunction(final String functionDrop, final String functionCreate) throws SQLException {
        try (Statement stmtDrop = con.createStatement()){
            System.out.println("Calling DROP PROCEDURE");
            stmtDrop.execute(functionDrop);
        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        }

        try (Statement stmt = con.createStatement()){
            stmt.executeUpdate(functionCreate);
        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        }

    }

    public void createFunctionRaisePrice() throws SQLException {
        String functionDrop = "DROP FUNCTION IF EXISTS RAISE_PRICE( VARCHAR(32), FLOAT, NUMERIC(10, 2) );";

        String functionCreate = "CREATE OR REPLACE FUNCTION RAISE_PRICE(coffeeName VARCHAR(32), maximumPercentage REAL, newPrice NUMERIC(10, 2))\n" +
                "  RETURNS NUMERIC(10, 2) AS $c$\n" +
                "  DECLARE\n" +
                "  maximumNewPrice NUMERIC(10, 2);\n" +
                "  oldPrice        NUMERIC(10, 2);\n" +
                "  BEGIN\n" +
                "          --main: BEGIN\n" +
                "  SELECT COFFEES.PRICE\n" +
                "  INTO oldPrice\n" +
                "  FROM COFFEES\n" +
                "  WHERE COFFEES.COF_NAME = coffeeName;\n" +
                "  maximumNewPrice := oldPrice * (1 + maximumPercentage);\n" +
                "  IF (newPrice > maximumNewPrice)\n" +
                "  THEN newPrice := maximumNewPrice;\n" +
                "  END IF;\n" +
                "  IF (newPrice <= oldPrice)\n" +
                "  THEN newPrice := oldPrice;\n" +
                "  ELSE\n" +
                "  UPDATE COFFEES\n" +
                "  SET PRICE = newPrice\n" +
                "  WHERE COF_NAME = coffeeName;\n" +
                "  END IF;\n" +
                "  RETURN newPrice;\n" +
                "  END;\n" +
                "  $c$ LANGUAGE plpgsql;";
        createFunction(functionDrop, functionCreate);
    }


    /*
    CREATE OR REPLACE FUNCTION GET_SUPPLIER_OF_COFFEE(coffeeName VARCHAR(32))
    RETURNS VARCHAR(40) AS $b$
    BEGIN
    CREATE TEMP TABLE supplierName
    ON COMMIT DROP AS
    SELECT SUPPLIERS.SUP_NAME
    FROM SUPPLIERS, COFFEES
    WHERE SUPPLIERS.SUP_ID = COFFEES.SUP_ID
    AND coffeeName = COFFEES.COF_NAME;
    SELECT supplierName;
    END;
    $b$ LANGUAGE plpgsql;
    */

    public void createFunctionGetSupplierOfCoffee() throws SQLException {
        String functionDrop = "DROP FUNCTION IF EXISTS GET_SUPPLIER_OF_COFFEE(VARCHAR(32));";

        String functionCreate = "CREATE OR REPLACE FUNCTION GET_SUPPLIER_OF_COFFEE(coffeeName VARCHAR(32))\n" +
                "  RETURNS SETOF VARCHAR(40) AS $b$\n" +
                "  BEGIN\n" +
                "  RETURN QUERY SELECT " +
                "  SUPPLIERS.SUP_NAME AS supplierName\n" +
                "  FROM SUPPLIERS, COFFEES\n" +
                "  WHERE SUPPLIERS.SUP_ID = COFFEES.SUP_ID\n" +
                "  AND coffeeName = COFFEES.COF_NAME;\n" +
                "  END;\n" +
                "  $b$ LANGUAGE plpgsql;";

        createFunction(functionDrop, functionCreate);
    }


    public void createFunctionShowSuppliers() throws SQLException {
        String functionDrop = "DROP FUNCTION IF EXISTS SHOW_SUPPLIERS();";

        String functionCreate = "CREATE OR REPLACE FUNCTION SHOW_SUPPLIERS()\n" +
                "  RETURNS TABLE(supplierName VARCHAR(40), coffeeName VARCHAR(32)) AS $a$\n" +
                "  BEGIN\n" +
                "  RETURN QUERY SELECT\n" +
                "  SUPPLIERS.SUP_NAME AS supplierName,\n" +
                "  COFFEES.COF_NAME   AS coffeeName\n" +
                "  FROM SUPPLIERS, COFFEES\n" +
                "  WHERE SUPPLIERS.SUP_ID = COFFEES.SUP_ID\n" +
                "  ORDER BY SUP_NAME;\n" +
                "  END;\n" +
                "  $a$ LANGUAGE plpgsql;";

        createFunction(functionDrop, functionCreate);
    }

    public void runStoredProcedures(String coffeeNameArg, float maximumPercentageArg, BigDecimal newPriceArg) throws SQLException {
        CallableStatement cs = null;

        try {
            System.out.println("\nCalling the function GET_SUPPLIER_OF_COFFEE");
            final String getSupplierOfCoffeeSql = "select * from GET_SUPPLIER_OF_COFFEE( ? )";
            PreparedStatement getSupplierOfCoffeePstmt = con.prepareStatement(getSupplierOfCoffeeSql);
            getSupplierOfCoffeePstmt.setString(1, coffeeNameArg);
            ResultSet getSupplierOfCoffeeRs = getSupplierOfCoffeePstmt.executeQuery();
            String supplierOfCoffee = null;
            while (getSupplierOfCoffeeRs.next()) {
                supplierOfCoffee = getSupplierOfCoffeeRs.getString(1);
            }
            getSupplierOfCoffeePstmt.close();

            if (supplierOfCoffee != null) {
                System.out.println("\nSupplier of the coffee " + coffeeNameArg + ": " + supplierOfCoffee);
            } else {
                System.out.println("\nUnable to find the coffee " + coffeeNameArg);
            }

            System.out.println("\nCalling the function SHOW_SUPPLIERS");
            final String showSuppliersSql = "select * from SHOW_SUPPLIERS()";
            PreparedStatement showSuppliersPstmt = con.prepareStatement(showSuppliersSql);
            ResultSet showSuppliersRs = showSuppliersPstmt.executeQuery();

            while (showSuppliersRs.next()) {
                String supplier = showSuppliersRs.getString("supplierName");
                String coffee = showSuppliersRs.getString("coffeeName");
                System.out.println(supplier + ": " + coffee);
            }

            showSuppliersRs.close();

            System.out.println("\nContents of COFFEES table before calling RAISE_PRICE:");
            CoffeesTable.viewTable(this.con);

            System.out.println("\nCalling the function RAISE_PRICE");
            final String raicePriceSql = "select * from RAISE_PRICE( ? , ?, ?)";
            PreparedStatement raicePricePstmt = con.prepareStatement(raicePriceSql);
            raicePricePstmt.setString(1, coffeeNameArg);
            raicePricePstmt.setFloat(2, maximumPercentageArg);
            raicePricePstmt.setBigDecimal(3, newPriceArg);

            ResultSet raicePriceRs =  raicePricePstmt.executeQuery();

            while (raicePriceRs.next()) {
                System.out.println("\nValue of newPrice after calling RAISE_PRICE: " + raicePriceRs.getFloat(1));
            }

            System.out.println("\nContents of COFFEES table after calling RAISE_PRICE:");
            CoffeesTable.viewTable(this.con);


        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        } finally {
            if (cs != null) {
                cs.close();
            }
        }
    }

    public static void main(String[] args) {
        JdbcDataSource jdbcDataSource = getJdbcDataSource(args[0]);

        try (Connection myConnection = JDBCTutorialUtilities.getConnectionToDatabase(jdbcDataSource,false)){
            StoredFunctionPostgreSqlSample myStoredProcedureSample =
                    new StoredFunctionPostgreSqlSample(myConnection,
                            jdbcDataSource);

//      JDBCTutorialUtilities.initializeTables(myConnection,
//                                             myJDBCTutorialUtilities.dbName,
//                                             myJDBCTutorialUtilities.dbms);


            System.out.println("\nCreating SHOW_SUPPLIERS stored procedure");
            myStoredProcedureSample.createFunctionShowSuppliers();

            System.out.println("\nCreating GET_SUPPLIER_OF_COFFEE stored procedure");
            myStoredProcedureSample.createFunctionGetSupplierOfCoffee();

            System.out.println("\nCreating RAISE_PRICE stored procedure");
            myStoredProcedureSample.createFunctionRaisePrice();


            System.out.println("\nCalling all stored procedures:");
            myStoredProcedureSample.runStoredProcedures("Colombian", 0.10f, BigDecimal.valueOf(19.99));
        } catch (SQLException e) {
            JDBCTutorialUtilities.printSQLException(e);
        }
    }
}

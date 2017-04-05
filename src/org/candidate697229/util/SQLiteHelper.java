package org.candidate697229.util;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;

import java.sql.*;
import java.util.stream.Collectors;

/**
 * Class that provides a few static methods for working with SQLite databases.
 */
public class SQLiteHelper {
    /**
     * Private constructor to ensure class cannot be accidentally instantiated (it is intended only to use static methods).
     */
    private SQLiteHelper() {
    }

    /**
     * Construct a SQLite database.
     *
     * @param database the database to construct the SQLite database from
     * @param name     the name of the database file
     */
    public static void makeSQLiteDatabase(Database database, String name) {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + name)) {
            if (conn != null) database.getRelations().forEach(relation -> makeTable(conn, relation));
            else throw new InternalError("Invalid database connection.");
        } catch (SQLException e) {
            throw new InternalError("SQL error during database creation.", e);
        }
    }

    /**
     * Run a query against a SQLite database.
     * Notice the result of the query is expected to be a sequence of longs.
     *
     * @param database the file containing the database to run against
     * @param query    the SQL query to execute
     * @return the result of the query
     */
    public static long[] runQuery(String database, String query) {
        long[] result;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            rs.next();
            result = new long[rs.getMetaData().getColumnCount()];
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i)
                result[i - 1] = rs.getLong(i);
        } catch (SQLException e) {
            throw new InternalError("SQL error during query execution.", e);
        }
        return result;
    }

    /**
     * Create a table from a relation.
     * @param conn the database connection to create the table in
     * @param relation the relation to create the table from
     */
    private static void makeTable(Connection conn, Relation relation) {
        try {
            Statement createTable = conn.createStatement();
            createTable.execute("CREATE TABLE " + relation.getName() + " (" +
                    relation.getAttributes().stream()
                            .map(attribute -> attribute + " BIGINT NOT NULL")
                            .collect(Collectors.joining(", "))
                    + ");");

            StringBuilder insertSql = new StringBuilder("INSERT INTO " + relation.getName() + "(");
            insertSql.append(String.join(",", relation.getAttributes()));
            insertSql.append(") VALUES ");
            long[][] tuples = relation.getTuples();
            for (long[] tuple : tuples) {
                insertSql.append("(");
                for (int j = 0; j < tuple.length - 1; ++j)
                    insertSql.append(tuple[j]).append(",");
                insertSql.append(tuple[tuple.length - 1]).append("),");
            }
            insertSql.deleteCharAt(insertSql.lastIndexOf(",")).append(";");
            Statement insertValues = conn.createStatement();
            insertValues.execute(insertSql.toString());
        } catch (SQLException e) {
            throw new InternalError("SQL error during table creation.", e);
        }
    }
}

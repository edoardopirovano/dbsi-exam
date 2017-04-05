package org.candidate697229.util;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.stream.Collectors;

public class SQLiteHelper {
    public static void makeSQLiteDatabase(Database database, String name) {
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + name)) {
            if (conn != null) database.getRelations().forEach(relation -> makeTable(conn, relation));
            else throw new InternalError("Invalid database connection.");
        } catch (SQLException e) {
            throw new InternalError(e);
        }
    }

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
            throw new InternalError(e);
        }
    }
}

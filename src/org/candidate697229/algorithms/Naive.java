package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;

import java.sql.*;
import java.util.LinkedList;
import java.util.List;
import java.util.stream.Collectors;

public class Naive {

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

    public static List<Long> runQuery(String database, String query) {
        List<Long> result = new LinkedList<>();
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            rs.next();
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i)
                result.add(rs.getLong(i));
        } catch (SQLException e) {
            throw new InternalError(e);
        }
        return result;
    }

    public static String buildQueryAll(Database database) {
        StringBuilder query = new StringBuilder("SELECT ");
        database.getAttributeNamePairs().forEach(attributePair -> query
                .append("SUM(")
                .append(attributePair.getFirst())
                .append("*")
                .append(attributePair.getSecond())
                .append("),"));
        buildNaturalJoin(database, query.deleteCharAt(query.lastIndexOf(",")));
        return query.toString();
    }

    public static String buildQueryOne(Database database) {
        StringBuilder query = new StringBuilder("SELECT SUM(");
        query.append(database.getRelations().get(0).getAttributes().get(0))
                .append("*")
                .append(database.getRelations().get(0).getAttributes().get(0))
                .append(")");
        buildNaturalJoin(database, query);
        return query.toString();
    }

    private static void buildNaturalJoin(Database database, StringBuilder query) {
        query.append(" FROM ")
                .append(database.getRelations().stream()
                        .map(Relation::getName)
                        .collect(Collectors.joining(" NATURAL JOIN ")))
                .append(";");
    }
}

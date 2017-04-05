package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;

import java.sql.*;
import java.util.stream.Collectors;

import static org.candidate697229.config.Configuration.USE_TEST_DATABASE;

public class Naive implements AggAlgorithm {
    private final int scaleFactor;

    /**
     *
     * @param scaleFactor the scaleFactor to run on
     */
    public Naive(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public long[] computeAllAggregatesOfNaturalJoin() {
        return runQuery(USE_TEST_DATABASE ? "test-table.db" : "housing/housing-" + scaleFactor + ".db", buildQueryAll());
    }

    @Override
    public long computeOneAggregateOfNaturalJoin() {
        return runQuery(USE_TEST_DATABASE ? "test-table.db" : "housing/housing-" + scaleFactor + ".db", buildQueryOne())[0];
    }

    private long[] runQuery(String database, String query) {
        long[] result;
        try (Connection conn = DriverManager.getConnection("jdbc:sqlite:" + database);
             Statement statement = conn.createStatement();
             ResultSet rs = statement.executeQuery(query)) {
            rs.next();
            result = new long[rs.getMetaData().getColumnCount()];
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); ++i)
                result[i - 1] = rs.getLong(i);
        } catch (SQLException e) {
            throw new InternalError(e);
        }
        return result;
    }

    private String buildQueryAll() {
        Database database = Database.makeFromDirectory(USE_TEST_DATABASE ? "test-table" : "housing/housing-" + scaleFactor, false);
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

    private String buildQueryOne() {
        Database database = Database.makeFromDirectory(USE_TEST_DATABASE ? "test-table" : "housing/housing-" + scaleFactor, false);
        StringBuilder query = new StringBuilder("SELECT SUM(");
        query.append(database.getRelations().get(0).getAttributes().get(0))
                .append("*")
                .append(database.getRelations().get(0).getAttributes().get(0))
                .append(")");
        buildNaturalJoin(database, query);
        return query.toString();
    }

    private void buildNaturalJoin(Database database, StringBuilder query) {
        query.append(" FROM ")
                .append(database.getRelations().stream()
                        .map(Relation::getName)
                        .collect(Collectors.joining(" NATURAL JOIN ")))
                .append(";");
    }
}

package org.candidate697229.algorithms;

import org.candidate697229.database.Database;
import org.candidate697229.database.Relation;
import org.candidate697229.util.SQLiteHelper;

import java.util.stream.Collectors;

import static org.candidate697229.util.Configuration.USE_EXAMPLE_DATABASE;

/**
 * Implementation of aggregation using a naive SQLite database.
 */
public class Naive implements AggAlgorithm {
    private final int scaleFactor;

    /**
     * Create an instance of this algorithm.
     * @param scaleFactor the scaleFactor to run on
     */
    public Naive(int scaleFactor) {
        this.scaleFactor = scaleFactor;
    }

    @Override
    public long[] computeAllAggregatesOfNaturalJoin() {
        return SQLiteHelper.runQuery(USE_EXAMPLE_DATABASE ? "example-database.db" : "housing/housing-" + scaleFactor + ".db", buildQueryAll());
    }

    @Override
    public long computeOneAggregateOfNaturalJoin() {
        return SQLiteHelper.runQuery(USE_EXAMPLE_DATABASE ? "example-database.db" : "housing/housing-" + scaleFactor + ".db", buildQueryOne())[0];
    }

    /**
     * Build a query for computing all aggregates.
     *
     * @return a SQL query for computing the sum of all products of pairs of attributes in the table
     */
    private String buildQueryAll() {
        Database database = Database.makeFromDirectory(USE_EXAMPLE_DATABASE ? "example-database" : "housing/housing-" + scaleFactor, false);
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

    /**
     * Build a query for computing one aggregate.
     * @return a SQL query for computing the sum of a products of pairs of attributes in the table
     */
    private String buildQueryOne() {
        Database database = Database.makeFromDirectory(USE_EXAMPLE_DATABASE ? "example-database" : "housing/housing-" + scaleFactor, false);
        StringBuilder query = new StringBuilder("SELECT SUM(");
        query.append(database.getRelations().get(0).getAttributes().get(0))
                .append("*")
                .append(database.getRelations().get(0).getAttributes().get(0))
                .append(")");
        buildNaturalJoin(database, query);
        return query.toString();
    }

    /**
     * Append the natural join of all tables to the end of a SQL query.
     * @param database the database to get the tables from
     * @param query the query to append to
     */
    private void buildNaturalJoin(Database database, StringBuilder query) {
        query.append(" FROM ")
                .append(database.getRelations().stream()
                        .map(Relation::getName)
                        .collect(Collectors.joining(" NATURAL JOIN ")))
                .append(";");
    }
}

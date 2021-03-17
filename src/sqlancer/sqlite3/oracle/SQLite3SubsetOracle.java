package sqlancer.sqlite3.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import sqlancer.ComparatorHelper;
import sqlancer.IgnoreMeException;
import sqlancer.Randomly;
import sqlancer.common.oracle.SubsetBase;
import sqlancer.common.oracle.TestOracle;
import sqlancer.common.query.SQLQueryAdapter;
import sqlancer.common.query.SQLancerResultSet;
import sqlancer.sqlite3.SQLite3Errors;
import sqlancer.sqlite3.SQLite3GlobalState;
import sqlancer.sqlite3.SQLite3Visitor;
import sqlancer.sqlite3.ast.SQLite3Aggregate;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation.PostfixUnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.gen.SQLite3Common;
import sqlancer.sqlite3.gen.SQLite3ExpressionGenerator;
import sqlancer.sqlite3.schema.SQLite3Schema;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Column;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Table;
import sqlancer.sqlite3.schema.SQLite3Schema.SQLite3Tables;

public class SQLite3SubsetOracle extends SubsetBase<SQLite3GlobalState> implements TestOracle {

    private static final int NO_VALID_RESULT = -1;
    private final SQLite3Schema s;
    private SQLite3ExpressionGenerator gen;

    public SQLite3SubsetOracle(SQLite3GlobalState globalState) {
        super(globalState);
        this.s = globalState.getSchema();
        SQLite3Errors.addExpectedExpressionErrors(errors);
        SQLite3Errors.addMatchQueryErrors(errors);
        SQLite3Errors.addQueryErrors(errors);
        errors.add("misuse of aggregate");
        errors.add("misuse of window function");
        errors.add("second argument to nth_value must be a positive integer");
        errors.add("no such table");
        errors.add("no query solution");
        errors.add("unable to use function MATCH in the requested context");
    }

    @Override
    public void check() throws SQLException {
        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Column> columns = randomTables.getColumns();
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromTables(tableRefs);
        select.setJoinClauses(joinStatements);
        useAggregate = Randomly.getBoolean();

        getOriginalQuery(select, randomWhereCondition);
        getSubsetQuery(select, randomWhereCondition);
        getSupersetQuery(select, randomWhereCondition);

        checkSubsetQuery(subsetQueryString, originalQueryString, useAggregate);
        checkSubsetQuery(originalQueryString, supersetQueryString, useAggregate);
    }

    private void getOriginalQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(Collections.emptyList(),
                    SQLite3Aggregate.SQLite3AggregateFunction.COUNT_ALL)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(randomWhereCondition);
        originalQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(originalQueryString);
        }
    }

    private void getSubsetQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(Collections.emptyList(),
                    SQLite3Aggregate.SQLite3AggregateFunction.COUNT_ALL)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        SQLite3Expression andExpression = new Sqlite3BinaryOperation(randomWhereCondition,
                gen.generateExpression(), BinaryOperator.AND);
        select.setWhereClause(andExpression);
        subsetQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(subsetQueryString);
        }
    }

    private void getSupersetQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(Collections.emptyList(),
                    SQLite3Aggregate.SQLite3AggregateFunction.COUNT_ALL)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        SQLite3Expression orExpression = new Sqlite3BinaryOperation(randomWhereCondition,
                gen.generateExpression(), BinaryOperator.OR);
        select.setWhereClause(orExpression);
        supersetQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(supersetQueryString);
        }
    }

    private void checkSubsetQuery(String subsetQuery, String originalQuery, boolean useAggregate) throws SQLException {
        if (useAggregate) {
            SQLQueryAdapter qSubset = new SQLQueryAdapter(subsetQuery, errors);
            SQLQueryAdapter qOrigin = new SQLQueryAdapter(originalQuery, errors);
            int subsetCount = extractCounts(qSubset), originCount = extractCounts(qOrigin);
            if (subsetCount == NO_VALID_RESULT || originCount == NO_VALID_RESULT) {
                throw new IgnoreMeException();
            }
            if (subsetCount > originCount) {
                state.getState().getLocalState().log("SUBSET BUG!\n" + subsetQuery + ";\n" + originalQuery + ";");
                throw new AssertionError(subsetCount + " " + originCount);
            }
        } else {
            List<String> originalResultSet =
                    ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, errors, state);
            List<String> combinedString = new ArrayList<>();
            List<String> secondResultSet = ComparatorHelper.getTwoCombinedResultSetNoDuplicates(originalQuery,
                    subsetQuery, combinedString, true, state, errors);
            ComparatorHelper.assumeResultSetsAreEqual(originalResultSet, secondResultSet, originalQuery, combinedString,
                    state);
        }
    }

    private int extractCounts(SQLQueryAdapter q) {
        int count = 0;
        try (SQLancerResultSet rs = q.executeAndGet(state)) {
            if (rs == null) {
                return NO_VALID_RESULT;
            } else {
                try {
                    while (rs.next()) {
                        count += rs.getInt(1);
                    }
                } catch (SQLException e) {
                    count = NO_VALID_RESULT;
                }
            }
        } catch (Exception e) {
            if (e instanceof IgnoreMeException) {
                throw (IgnoreMeException) e;
            }
            throw new AssertionError(originalQueryString, e);
        }
        return count;
    }

}

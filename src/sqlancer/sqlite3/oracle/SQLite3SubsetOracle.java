package sqlancer.sqlite3.oracle;

import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;

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
import sqlancer.sqlite3.ast.SQLite3Aggregate.SQLite3AggregateFunction;
import sqlancer.sqlite3.ast.SQLite3Expression;
import sqlancer.sqlite3.ast.SQLite3Expression.BetweenOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.BinaryComparisonOperation.BinaryComparisonOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.Cast;
import sqlancer.sqlite3.ast.SQLite3Expression.CollateOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Function;
import sqlancer.sqlite3.ast.SQLite3Expression.InOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.MatchOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3ColumnName;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Distinct;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Exist;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3OrderingTerm;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixText;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3PostfixUnaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3TableReference;
import sqlancer.sqlite3.ast.SQLite3Expression.SQLite3Text;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation;
import sqlancer.sqlite3.ast.SQLite3Expression.Sqlite3BinaryOperation.BinaryOperator;
import sqlancer.sqlite3.ast.SQLite3Expression.Subquery;
import sqlancer.sqlite3.ast.SQLite3Expression.TypeLiteral;
import sqlancer.sqlite3.ast.SQLite3Expression.Join;
import sqlancer.sqlite3.ast.SQLite3Expression.Join.JoinType;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation;
import sqlancer.sqlite3.ast.SQLite3UnaryOperation.UnaryOperator;
import sqlancer.sqlite3.ast.SQLite3Select;
import sqlancer.sqlite3.ast.SQLite3Select.SelectType;
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
    private SQLite3AggregateFunction aggregateFunction;
    private List<SQLite3Expression> aggregateColumn;
    private enum MutationOperatorType {
        SMALLEREQ_SUBSTITUE, // <= to < or ==
        GREATEREQ_SUBSTITUE, // >= to > or ==
        NOTEQ_SUBSTITUE, // != (<>) to < or >
        ADD_AND,
        IN_SHRINK,
        LIKE_SUBSTITUTE, //
        SMALLEREQ_VALUE_CHANGE,
        SMALLER_VALUE_CHANGE,
        GREATEREQ_VALUE_CHANGE,
        GREATER_VALUE_CHANGE,
        // -------------------------------------------
        JOIN_OUTER_INNER, // outer join -> inner join
        DISTINCT,
        GROUP_BY,
        LIMIT,
        INTERSECT
    }
    private int subsetConfig;


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
        // set subset combination strategy
        Random rand = new Random();
        subsetConfig = rand.nextInt(1 << 15); // dependent to the number of MutationOperatorType

        SQLite3Tables randomTables = s.getRandomTableNonEmptyTables();
        List<SQLite3Column> columns = randomTables.getColumns();
        gen = new SQLite3ExpressionGenerator(state).setColumns(columns);
        SQLite3Expression randomWhereCondition = gen.generateExpression();
        List<SQLite3Table> tables = randomTables.getTables();
        List<Join> joinStatements = gen.getRandomJoinClauses(tables);
        List<SQLite3Expression> tableRefs = SQLite3Common.getTableRefs(tables, s);
        SQLite3Select select = new SQLite3Select();
        select.setFromTables(tableRefs);


        useAggregate = Randomly.getBoolean();
        if (useAggregate) {
            aggregateFunction = Randomly.fromOptions(SQLite3AggregateFunction.COUNT_ALL,
                    SQLite3AggregateFunction.MAX, SQLite3AggregateFunction.MIN);
            aggregateColumn = new ArrayList<>();
            aggregateColumn.add(gen.getRandomColumn());
        }

        getOriginalQuery(select, randomWhereCondition, joinStatements);
        getSubsetQuery(select, randomWhereCondition, joinStatements);
        // getSupersetQuery(select, randomWhereCondition);

        checkSubsetQuery(subsetQueryString, originalQueryString, useAggregate);
        // checkSubsetQuery(originalQueryString, supersetQueryString, useAggregate);
    }


    private void mutateBinaryComparisonOperation(BinaryComparisonOperation expr, boolean negated) {
        if (negated) {
            // build superset
            switch (expr.getOperator()) {
                case SMALLER:
                case SMALLER_EQUALS:
                case GREATER:
                case GREATER_EQUALS:
                case EQUALS:
                case NOT_EQUALS:
                case LIKE:
                default:
                    return ;
            }
        } else {
            // build subset
            switch (expr.getOperator()) {
                case SMALLER:
                case SMALLER_EQUALS:
                case GREATER:
                case GREATER_EQUALS:
                case EQUALS:
                case NOT_EQUALS:
                case LIKE:
                default:
                    return ;
            }
        }

        


    }

    private void mutatePredicate(SQLite3Expression expr, boolean negated) throws SQLException {
        if (expr instanceof SQLite3UnaryOperation) {
            SQLite3UnaryOperation unaryExpr = (SQLite3UnaryOperation)expr;
            if unaryExpr.getOperation() == UnaryOperator.NOT {
                mutatePredicate(unaryExpr.getExpression(), !negated);
            }
        }
        else if (expr instanceof BinaryComparisonOperation) {
            mutateBinaryComparisonOperation((BinaryComparisonOperation)expr, negated);
            mutate
        } else if (expr instanceof InOperation && checkCanMutate(IN_SHRINK)) {
            InOperation inExpr = (InOperation)expr;
            List<SQLite3Expression> rightExprList = inExpr.getRightExpressionList();
            if (inExpr.getRightSelect.isEmpty()) {
                if (negated) {
                    // NOT IN
                    int N = Randomly.getInteger()
                    while (N--) {
                        rightExprList.add(gen.generateExpression);
                    }
                } else {
                    // IN
                    int N = Randomly.getInteger()
                    while (N-- && !rightExprList.isEmpty()) {
                        rightExprList.remove(rightExprList.size() - 1);
                    }
                }
                mutatePredicate(inExpr.getLeft());
                for ()
            }
        }
    }

    private void getOriginalQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(aggregateColumn, aggregateFunction)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }
        select.setWhereClause(randomWhereCondition);
        select.setJoinClauses(joinStatements);
        originalQueryString = SQLite3Visitor.asString(select);
        if (options.logEachSelect()) {
            logger.writeCurrent(originalQueryString);
        }
    }

    private boolean checkCanMutate(MutationOperatorType t) {
        return (1 << (t.ordinal())) & subsetConfig != 0;
    }

    private void getSubsetQuery(SQLite3Select select, SQLite3Expression randomWhereCondition,
                    List<SQLite3Expression> tableRefs, List<Join> joins) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(aggregateColumn, aggregateFunction)));
        } else {
            SQLite3ColumnName aggr = new SQLite3ColumnName(SQLite3Column.createDummy("*"), null);
            select.setFetchColumns(Arrays.asList(aggr));
        }


        // mutate JOIN
        if (checkCanMutate(MutationOperatorType.JOIN_OUTER_INNER)) {
            for (Join joinStmt : joins) {
                if (joinStmt.getType() == JoinType.OUTER) {
                    joinStmt.setType(JoinType.INNER);
                    break;
                }
            }
        }
        select.setJoinClauses(joinStatements);

        // mutate DISTINCT
        if (checkCanMutate(MutationOperatorType.DISTINCT)) {
            select.setSelectType(SelectType.DINSTINCT);
        }
        // mutate GROUP_BY
        if (checkCanMutate(MutationOperatorType.GROUP_BY)) {
            // TODO mutate group_by
        }

        // mutate LIMIT
        if (checkCanMutate(MutationOperatorType.LIMIT)) {
            select.setLimitClause(SQLite3IntConstant(Randomly.getInteger()));
        }

        // mutate predicate
        mutatePredicate(randomWhereCondition, false);

        // mutate ADD_AND
        if (checkCanMutate(ADD_AND)) {
            SQLite3Expression andExpression = new Sqlite3BinaryOperation(randomWhereCondition,
                        gen.generateExpression(), BinaryOperator.AND);
            select.setWhereClause(andExpression);
        } else {
            select.setWhereClause(randomWhereCondition);
        }

        subsetQueryString = SQLite3Visitor.asString(select);

        if (checkCanMutate(MutationOperatorType.INTERSECT)) {
            SQLite3Expression randomWhereCondition2 = gen.generateExpression();
            select.setWhereClause(randomWhereCondition2);
            subsetQueryString += " INTERSECT " + SQLite3Visitor.asString(select);
        }
        if (options.logEachSelect()) {
            logger.writeCurrent(subsetQueryString);
        }
    }

    /*
    private void getSupersetQuery(SQLite3Select select, SQLite3Expression randomWhereCondition) throws SQLException {
        if (Randomly.getBoolean()) {
            select.setOrderByExpressions(gen.generateOrderBys());
        }
        if (useAggregate) {
            select.setFetchColumns(Arrays.asList(new SQLite3Aggregate(aggregateColumn, aggregateFunction)));
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
    */

    private void checkSubsetQuery(String subsetQuery, String originalQuery, boolean useAggregate) throws SQLException {
        if (useAggregate) {
            SQLQueryAdapter qSubset = new SQLQueryAdapter(subsetQuery, errors);
            SQLQueryAdapter qOrigin = new SQLQueryAdapter(originalQuery, errors);
            int subsetCount = extractCounts(qSubset), originCount = extractCounts(qOrigin);
            if (subsetCount == NO_VALID_RESULT || originCount == NO_VALID_RESULT) {
                throw new IgnoreMeException();
            }
            if (aggregateFunction == SQLite3AggregateFunction.MIN) {
                if (subsetCount < originCount && subsetCount != 0) { // TODO: update sqlite version?
                    state.getState().getLocalState().log("--SUBSET BUG!\n" + subsetQuery + ";\n" + originalQuery + ";");
                    throw new AssertionError(subsetCount + " " + originCount);
                }
            } else {
                if (subsetCount > originCount && subsetCount != 0) {
                    state.getState().getLocalState().log("--SUBSET BUG!\n" + subsetQuery + ";\n" + originalQuery + ";");
                    throw new AssertionError(subsetCount + " " + originCount);
                }
            }
        } else {
            List<String> originalResultSet =
                    ComparatorHelper.getResultSetFirstColumnAsString(originalQuery, errors, state);
            List<String> subsetResultSet =
                    ComparatorHelper.getResultSetFirstColumnAsString(subsetQuery, errors, state);
            ComparatorHelper.assumeResultSetsAreSubset(originalResultSet, subsetResultSet, originalQuery, subsetQuery,
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

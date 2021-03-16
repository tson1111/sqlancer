package sqlancer.common.oracle;

import sqlancer.Main.StateLogger;
import sqlancer.MainOptions;
import sqlancer.SQLConnection;
import sqlancer.SQLGlobalState;
import sqlancer.common.query.ExpectedErrors;

public abstract class SubsetBase<S extends SQLGlobalState<?, ?>> implements TestOracle {

    protected final S state;
    protected final ExpectedErrors errors = new ExpectedErrors();
    protected final StateLogger logger;
    protected final MainOptions options;
    protected final SQLConnection con;
    protected String originalQueryString;
    protected String subsetQueryString;
    protected String supersetQueryString;
    protected boolean useAggregate;

    public SubsetBase(S state) {
        this.state = state;
        this.con = state.getConnection();
        this.logger = state.getLogger();
        this.options = state.getOptions();
    }

}

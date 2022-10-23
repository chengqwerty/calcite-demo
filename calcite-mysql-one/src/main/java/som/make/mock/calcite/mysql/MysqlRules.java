package som.make.mock.calcite.mysql;

import org.apache.calcite.plan.RelOptRule;
import som.make.mock.calcite.mysql.rules.MysqlFilterRule;

public class MysqlRules {

    public static final RelOptRule[] RULES = {
            MysqlFilterRule.Config.DEFAULT.toRule(),
    };

}

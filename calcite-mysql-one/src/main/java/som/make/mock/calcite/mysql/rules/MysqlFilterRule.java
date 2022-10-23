package som.make.mock.calcite.mysql.rules;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import som.make.mock.calcite.mysql.MysqlRelNode;
import som.make.mock.calcite.mysql.MysqlTableScan;

public class MysqlFilterRule extends RelRule<MysqlFilterRule.Config> {

    /**
     * Creates a RelRule.
     *
     * @param config
     */
    protected MysqlFilterRule(Config config) {
        super(config);
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
        LogicalFilter filter = call.rel(0);
        if (filter.getTraitSet().contains(Convention.NONE)) {
            final RelNode converted = convert(filter);
            if (converted != null) {
                call.transformTo(converted);
            }
        }
    }

    public RelNode convert(LogicalFilter logicalFilter) {
        final RelTraitSet traitSet = logicalFilter.getTraitSet().replace(MysqlRelNode.CONVENTION);
        return new MysqlFilter(logicalFilter.getCluster(), traitSet,
                convert(logicalFilter.getInput(), MysqlRelNode.CONVENTION),
                logicalFilter.getCondition());
    }

    public interface Config extends RelRule.Config {
        Config DEFAULT = ImmutableMysqlFilterRule.Config.builder()
                .withOperandSupplier(b0 ->
                        b0.operand(LogicalFilter.class).oneInput(b1 ->
                                b1.operand(MysqlTableScan.class).noInputs()))
                .withDescription("MysqlFilterRule")
                .build();

        @Override
        default RelOptRule toRule() {
            return new MysqlFilterRule(this);
        }
    }

}

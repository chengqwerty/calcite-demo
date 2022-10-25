package som.make.mock.calcite.mysql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;

public class MysqlTableScan extends TableScan implements MysqlRel {

    private final MysqlTable mysqlTable;

    protected MysqlTableScan(MysqlTable mysqlTable, RelDataType projectRowType, RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.mysqlTable = mysqlTable;
    }

    @Override
    public void register(RelOptPlanner planner) {
        planner.addRule(MysqlToEnumerableConverterRule.DEFAULT_CONFIG.toRule());
        for (RelOptRule rule : MysqlRules.RULES) {
            planner.addRule(rule);
        }
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.mysqlTable = mysqlTable;
        implementor.table = table;
    }

}

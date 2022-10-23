package som.make.mock.calcite.mysql;

import com.google.common.collect.ImmutableList;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.type.RelDataType;

import java.util.List;

public class MysqlTableScan extends TableScan implements MysqlRelNode {

    private MysqlTable mysqlTable;
    private RelDataType projectRowType;

    protected MysqlTableScan(MysqlTable mysqlTable, RelDataType projectRowType, RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table) {
        super(cluster, traitSet, ImmutableList.of(), table);
        this.mysqlTable = mysqlTable;
        this.projectRowType = projectRowType;
    }



    @Override
    public void register(RelOptPlanner planner) {
        for (RelOptRule rule : MysqlRules.RULES) {
            planner.addRule(rule);
        }
    }

}

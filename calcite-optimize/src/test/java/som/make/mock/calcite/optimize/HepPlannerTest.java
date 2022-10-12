package som.make.mock.calcite.optimize;

import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rules.FilterJoinRule;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class HepPlannerTest {

    @Test
    public void hepTest1() throws SqlParseException {
        final String sql = "select a.Id from data as a  join data b on a.Id = b.Id where a.Id>1";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner = new HepPlanner(
                programBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule())
                        .build());
        RelNode relNode = SqlToRelNode.getSqlNode(sql, hepPlanner);
        // 未优化算子树结构
        System.out.println("未优化算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner planner = relNode.getCluster().getPlanner();
        planner.setRoot(relNode);
        RelNode bestExp = planner.findBestExp();
        // 优化后结果
        System.out.println("优化后算子树结构:");
        System.out.println(RelOptUtil.toString(bestExp));
    }

    @Test
    public void hepTest2() throws ValidationException, SqlParseException, RelConversionException {
        final String sql = "select a.Id from data as a  join data b on a.Id = b.Id where a.Id>1";
        HepProgramBuilder programBuilder = HepProgram.builder();
        HepPlanner hepPlanner = new HepPlanner(
                programBuilder.addRuleInstance(FilterJoinRule.FilterIntoJoinRule.FilterIntoJoinRuleConfig.DEFAULT.toRule())
                        .build());
        RelNode relNode = SqlToRelNode.getRelNode(sql);
        System.out.println("未优化算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
        hepPlanner.setRoot(relNode);
        relNode = hepPlanner.findBestExp();
        System.out.println(RelOptUtil.toString(relNode));
    }


}

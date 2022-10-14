package som.make.mock.calcite.optimize;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelDistributionTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.tools.RelConversionException;
import org.apache.calcite.tools.ValidationException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class VolcanoPlannerTest {

    @Test
    public void volcanoTest() throws ValidationException, SqlParseException, RelConversionException {
//        final String sql = "select a.Id from data as a  join data b on a.Id = b.Id where a.Id>1";
        final String sql = "select a.Id from data as a where a.Id>1";
        RelNode relNode = SqlToRelNode.getRelNode(sql);
//        relNode.getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        System.out.println("未优化算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner relOptPlanner = relNode.getCluster().getPlanner();
        relOptPlanner.setRoot(relNode);
        RelTraitSet desiredTraits = relOptPlanner.getRoot().getCluster().traitSet().replace(EnumerableConvention.INSTANCE);
        RelNode newRoot = relOptPlanner.changeTraits(relNode, desiredTraits);
        relOptPlanner.setRoot(newRoot);
        relNode = relOptPlanner.findBestExp();
        System.out.println("优化后算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
    }

}

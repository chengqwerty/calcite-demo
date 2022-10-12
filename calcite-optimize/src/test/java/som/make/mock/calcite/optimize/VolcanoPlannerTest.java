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
public class VolcanoPlannerTest {

    @Test
    public void volcanoTest() throws ValidationException, SqlParseException, RelConversionException {
//        final String sql = "select a.Id from data as a  join data b on a.Id = b.Id where a.Id>1";
        final String sql = "select a.Id from data as a where a.Id>1";
        RelNode relNode = SqlToRelNode.getRelNode(sql);
        System.out.println("未优化算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
        RelOptPlanner relOptPlanner = relNode.getCluster().getPlanner();
        relOptPlanner.setRoot(relNode);
        relNode = relOptPlanner.findBestExp();
        System.out.println("优化后算子树结构:");
        System.out.println(RelOptUtil.toString(relNode));
    }

}

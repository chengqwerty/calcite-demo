package som.make.mock.calcite.optimize;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import som.make.mock.calcite.csv.CsvSchema;

import java.util.List;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class RelBuilderTest {

    private Frameworks.ConfigBuilder configBuilder;

    @BeforeAll
    public void config() {
        final SchemaPlus rootSchema = Frameworks.createRootSchema(true);
        this.configBuilder = Frameworks.newConfigBuilder()
                .parserConfig(SqlParser.Config.DEFAULT)
                .defaultSchema(rootSchema.add("csv", new CsvSchema("data.csv")))
                .traitDefs((List<RelTraitDef>) null);
    }

    @Test
    public void scanTest() {
        FrameworkConfig frameworkConfig = configBuilder.build();
        RelBuilder builder = RelBuilder.create(frameworkConfig);
        RelNode node = builder.scan("data").build();
        System.out.println(RelOptUtil.toString(node));
    }

    @Test
    public void projectFilterTest() {
        FrameworkConfig frameworkConfig = configBuilder.build();
        RelBuilder builder = RelBuilder.create(frameworkConfig);
        RelNode node = builder.scan("data")
                .project(builder.field("Name"), builder.field("Score"))
                .filter(builder.call(SqlStdOperatorTable.GREATER_THAN, builder.field("Score"), builder.literal(90)))
                .build();
        System.out.println(RelOptUtil.toString(node));
    }

}

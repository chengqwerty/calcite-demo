package som.make.mock.calcite.mysql;

import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterRule;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.function.Predicate;

public class MysqlToEnumerableConverterRule extends ConverterRule {

    static Predicate<RelNode> mysqlPredicate = relNode -> true;

    public static final Config DEFAULT_CONFIG = (Config) Config.INSTANCE
            .withConversion(RelNode.class, mysqlPredicate,
                    MysqlRel.CONVENTION, EnumerableConvention.INSTANCE,
                    "EnumerableCalcRule")
            .withRuleFactory(MysqlToEnumerableConverterRule::new)
            .withDescription("MysqlToEnumerableConverterRule");

    public MysqlToEnumerableConverterRule(Config config) {
        super(config);
    }

    @Override
    public @Nullable RelNode convert(RelNode rel) {
        final RelTraitSet newTraits = rel.getTraitSet().replace(getOutConvention());
        return new MysqlToEnumerableConverter(rel.getCluster(), newTraits, rel);
    }

}

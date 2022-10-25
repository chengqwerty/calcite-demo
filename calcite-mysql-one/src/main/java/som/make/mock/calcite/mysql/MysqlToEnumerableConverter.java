package som.make.mock.calcite.mysql;

import org.apache.calcite.adapter.enumerable.*;
import org.apache.calcite.linq4j.tree.BlockBuilder;
import org.apache.calcite.linq4j.tree.Expression;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.linq4j.tree.MethodCallExpression;
import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.convert.ConverterImpl;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.calcite.util.BuiltInMethod;
import org.apache.calcite.util.Pair;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.AbstractList;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;

public class MysqlToEnumerableConverter extends ConverterImpl implements EnumerableRel {

    /**
     * Creates a ConverterImpl.
     *
     * @param cluster  planner's cluster
     * @param traitSet   the output traits of this converter
     * @param child    child rel (provides input traits)
     */
    protected MysqlToEnumerableConverter(RelOptCluster cluster, RelTraitSet traitSet, RelNode child) {
        super(cluster, ConventionTraitDef.INSTANCE, traitSet, child);
    }

    // list转化为表达式方法
    private <T> MethodCallExpression constantArrayList(List<T> values, Class<?> clazz) {
        return Expressions.call(BuiltInMethod.ARRAYS_AS_LIST.method, Expressions.newArrayInit(clazz, constantList(values)));
    }

    /**
     * 将list转为常量表达式
     */
    private <T> List<? extends Expression> constantList(List<T> values) {
        return values.stream().map(Expressions::constant).collect(Collectors.toList());
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
        final MysqlRel.Implementor mysqlImplementor = new MysqlRel.Implementor();
        mysqlImplementor.visitChild(0, getInput());

        final RelDataType rowType = getRowType();
        final PhysType physType = PhysTypeImpl.of(implementor.getTypeFactory(), rowType, pref.prefer(JavaRowFormat.ARRAY));
        final BlockBuilder list = new BlockBuilder();
        final Expression fields = list.append("FIELDS",
                constantArrayList(Pair.zip(MysqlUtils.mysqlFieldNames(rowType),
                        new AbstractList<Class<?>>() {
                            @Override
                            public Class<?> get(int index) {
                                return physType.fieldClass(index);
                            }

                            @Override
                            public int size() {
                                return rowType.getFieldCount();
                            }
                        }), Pair.class));
        final Expression table = list.append("TABLE", mysqlImplementor.table.getExpression(MysqlQueryable.class));
        final Expression predicates = list.append("PREDICATES", constantArrayList(mysqlImplementor.whereClause,
                String.class));
        final Expression enumerable = list.append("ENUMERABLE", Expressions.call(table,
                MysqlMethod.Mysql_Method_QUERYABLE_QUERY.method,
                fields, predicates));
        list.add(Expressions.return_(null, enumerable));
        return implementor.result(physType, list.toBlock());
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
        return new MysqlToEnumerableConverter(getCluster(), traitSet, sole(inputs));
    }

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return Objects.requireNonNull(super.computeSelfCost(planner, mq)).multiplyBy(0.1);
    }
}

package som.make.mock.calcite.mysql.rules;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.metadata.RelMetadataQuery;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.checkerframework.checker.nullness.qual.Nullable;
import som.make.mock.calcite.mysql.MysqlRel;
import som.make.mock.calcite.mysql.MysqlUtils;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;

public class MysqlFilter extends Filter implements MysqlRel {

    private RelDataType rowTypes = getRowType();
    private List<String> fieldNames = MysqlUtils.mysqlFieldNames(getRowType());

    @Override
    public @Nullable RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq) {
        return  planner.getCostFactory().makeCost(1000000, 1, 10);
    }

    protected MysqlFilter(RelOptCluster cluster, RelTraitSet traits, RelNode child, RexNode condition) {
        super(cluster, traits, child, condition);
    }

    @Override
    public Filter copy(RelTraitSet traitSet, RelNode input, RexNode condition) {
        return new MysqlFilter(getCluster(), traitSet, input, condition);
    }

    @Override
    public void implement(Implementor implementor) {
        implementor.visitChild(0, getInput());
        implementor.add(null, Collections.singletonList(translateMatch(condition)));
    }

    private String translateMatch(RexNode condition) {
        final List<String> exp =
                RelOptUtil.disjunctions(condition).stream().map(this::translateAnd).collect(Collectors.toList());
        return exp.size() == 1 ? exp.get(0) : String.join(" OR ", exp);
    }

    private String translateAnd(RexNode condition) {
        final List<String> exp =
                RelOptUtil.disjunctions(condition).stream().map(this::translateMatch2).collect(Collectors.toList());
        return String.join(" AND ", exp);
    }

    private String translateMatch2(RexNode node) {
        switch (node.getKind()) {
            case EQUALS:
                return translateBinary(SqlKind.EQUALS.sql, SqlKind.EQUALS.sql, (RexCall) node);
            default:
                throw new RuntimeException("无法转换 ：" + node);
        }
    }

    private String translateBinary(String op, String rop, RexCall call) {
        final RexNode left = call.operands.get(0);
        final RexNode right = call.operands.get(1);
        String expression = translateBinary(op, left, right);
        if (expression != null) return expression;
        expression = translateBinary(rop, right, left);
        if (expression != null) return expression;
        throw new RuntimeException(String.format("无法转换 op=%s, call=%s", op, call));
    }

    private String translateBinary(String op, RexNode left, RexNode right) {
        switch (left.getKind()) {
            case INPUT_REF:
                final String name = fieldNames.get(((RexInputRef) left).getIndex());
                return translateOp(op, name, right);
            case CAST:
                return translateBinary(op, ((RexCall) left).operands.get(0), right);
            case OTHER_FUNCTION:
                return translateOp(op, left, right);
            default:
                throw new RuntimeException("无法找到该SqlKind：" + left.getKind());
        }
    }

    private String translateOp(String op, RexNode name, RexNode right) {
        return String.format("%s %s %s", translate(name), op, translateRexNode(right, null));
    }

    private String translateOp(String op, String name, RexNode right) {
        return String.format("%s %s %s", name, op, translateRexNode(right, null));
    }

    private String translateRexNode(RexNode right, String name) {
        if (right instanceof RexLiteral) {
            final Object v = literalValue((RexLiteral) right);
            String vs = v.toString();
            if (v instanceof String && name != null) {
                final SqlTypeName typeName = rowTypes.getField(name, true, false).getType().getSqlTypeName();
                if (typeName != SqlTypeName.CHAR) {
                    vs = "'" + vs + "'";
                }
            }
            return vs;
        } else if (right instanceof RexCall) {
            return translate(right);
        }
        return "";
    }

    private Object literalValue(RexLiteral literal) {
//        final Comparable value = RexLiteral.value(literal);
        // TODO
        return literal.getValueAs(String.class);
    }

    private String translate(RexNode rexNode) {
        RexCall call = (RexCall) rexNode;
        List<String> arg = new ArrayList<>();
        for (int i = 0; i < call.operands.size(); i++) {
            final RexNode operand = call.operands.get(i);
            if (operand instanceof RexInputRef) {
                final String fieldName = fieldNames.get(((RexInputRef) operand).getIndex());
                arg.add(fieldName);
            } else if (operand instanceof RexCall) {
                arg.add(translate(operand));
            } else {
                arg.add(((RexLiteral) operand).getValue3().toString());
            }
        }
        return String.join(",", arg);
    }
}

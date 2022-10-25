package som.make.mock.calcite.mysql;

import org.apache.calcite.plan.*;
import org.apache.calcite.rel.RelNode;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public interface MysqlRel extends RelNode {

    Convention CONVENTION = new Convention.Impl("Mysql", MysqlRel.class);

    void implement(Implementor implementor);

    class Implementor {
        RelOptTable table;
        MysqlTable mysqlTable;

        final Map<String, String> selectFields = new LinkedHashMap<>();
        /**
         * where 字段
         */
        final List<String> whereClause = new ArrayList<>();

        public void visitChild(int ordinal, RelNode input) {
            assert ordinal == 0;
            assert input instanceof MysqlRel;
            ((MysqlRel) input).implement(this);
        }

        public void add(Map<String, String> fields, List<String> predicates) {
            if (fields != null) {
                selectFields.putAll(fields);
            }
            if (predicates != null) {
                whereClause.addAll(predicates);
            }
        }
    }

}

package som.make.mock.calcite.mysql;

import org.apache.calcite.adapter.java.AbstractQueryableTable;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.linq4j.Queryable;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.sql.type.SqlTypeName;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

public class MysqlTable extends AbstractQueryableTable implements TranslatableTable {

    private final List<CdmColumn> columns;
    private final MysqlConfig mysqlConfig;

    public MysqlTable(List<CdmColumn> columns, MysqlConfig mysqlConfig) {
        super(Type.class);
        this.columns = columns;
        this.mysqlConfig = mysqlConfig;
    }

    @Override
    public <T> Queryable<T> asQueryable(QueryProvider queryProvider, SchemaPlus schema, String tableName) {
        return (Queryable<T>) new MysqlQueryable(queryProvider, schema, this, mysqlConfig, tableName);
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
        final RelOptCluster cluster = context.getCluster();
        return new MysqlTableScan(this, null, cluster, cluster.traitSetOf(MysqlRel.CONVENTION), relOptTable);
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        List<String> names = new ArrayList<>();
        List<RelDataType> types = new ArrayList<>();
        columns.forEach(c -> {
            names.add(c.getName());
            types.add(typeFactory.createSqlType(SqlTypeName.get(c.getType().toUpperCase())));
        });
        return typeFactory.createStructType(types, names);
    }

}

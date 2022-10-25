package som.make.mock.calcite.mysql;

import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.QueryProvider;
import org.apache.calcite.schema.QueryableTable;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTableQueryable;

import java.sql.*;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

public class MysqlQueryable extends AbstractTableQueryable<Object> {

    private MysqlConfig mysqlConfig;
    private String tableName;

    public MysqlQueryable(QueryProvider queryProvider, SchemaPlus schema, QueryableTable table, MysqlConfig mysqlConfig, String tableName) {
        super(queryProvider, schema, table, tableName);
        this.mysqlConfig = mysqlConfig;
        this.tableName = tableName;
    }

    @Override
    public Enumerator<Object> enumerator() {
        return query(null, null).enumerator();
    }

    public Enumerable<Object> query(List<Map.Entry<String, Class<?>>> fields, List<String> predicates) {
        final StringBuilder sql = new StringBuilder();
        String fieldSql = fields.stream().map(Map.Entry::getKey).collect(Collectors.joining(","));
        sql.append("SELECT ").append(fieldSql).append(" FROM ").append(tableName);
        if (predicates.size() > 0) sql.append(" WHERE ").append(predicates.get(0));
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object> enumerator() {
                try {
                    Class.forName("com.mysql.cj.jdbc.Driver");
                    final Connection conn = DriverManager.getConnection(mysqlConfig.getUrl(), mysqlConfig.getUsername(), mysqlConfig.getPassword());
                    final Statement stmt = conn.createStatement();
                    final ResultSet rs = stmt.executeQuery(sql.toString());
                    return (Enumerator<Object>) new MysqlEnumerator(rs, fields);
                } catch (ClassNotFoundException | SQLException e) {
                    throw new RuntimeException(e);
                }
            }
        };
    }

}

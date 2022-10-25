package som.make.mock.calcite.mysql;

import org.apache.calcite.linq4j.Enumerator;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class MysqlEnumerator implements Enumerator<Object> {

    private ResultSet rs;
    private List<Map.Entry<String, Class<?>>> fields;

    private Object curr;

    public MysqlEnumerator(ResultSet rs, List<Map.Entry<String, Class<?>>> fields) {
        this.rs = rs;
        this.fields = fields;
    }

    @Override
    public Object current() {
        try {
            if (fields.size() == 1) {
                return rs.getObject(1);
            }
            // 解构
            List<Object> row = new ArrayList<>(fields.size());
            Object[] objects = new Object[fields.size()];
            for (int i = 1; i <= rs.getMetaData().getColumnCount(); i++) {
                row.add(rs.getObject(i));
                objects[i - 1] = rs.getObject(i);
            }
//            return row;
            return objects;
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public boolean moveNext() {
        try {
            return rs.next();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void reset() {
        try {
            rs.relative(0);
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void close() {
        try {
            rs.getStatement().getConnection().close();
        } catch (SQLException e) {
            throw new RuntimeException(e);
        }
    }
}

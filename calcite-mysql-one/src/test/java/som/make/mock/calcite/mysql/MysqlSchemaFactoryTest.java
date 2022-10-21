package som.make.mock.calcite.mysql;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.Sources;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Objects;
import java.util.Properties;

public class MysqlSchemaFactoryTest {

    public ResultSet sql(String sql) throws SQLException {
        String path = Sources.of(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("model.json"))).file().getAbsolutePath();
        Properties info = new Properties();
        info.put("model", path);
        info.put("unquotedCasing", "UNCHANGED");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        Statement st = connection.createStatement();
        return st.executeQuery(sql);
    }

    @Test
    public void test1() throws SQLException {
        String sql = "select key from \"user:token:s0001\"";
        ResultSet resultSet = sql(sql);
    }

}

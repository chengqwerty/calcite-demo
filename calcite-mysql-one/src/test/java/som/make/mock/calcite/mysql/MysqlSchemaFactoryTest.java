package som.make.mock.calcite.mysql;

import org.apache.calcite.util.Sources;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.Objects;
import java.util.Properties;

public class MysqlSchemaFactoryTest {

//    private static final String jsonFile = "model.json";
    private static final String jsonFile = "model-2.json";

    public ResultSet sql(String sql) throws SQLException {
        String path = Sources.of(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource(jsonFile))).file().getAbsolutePath();
        Properties info = new Properties();
        info.put("model", path);
        info.put("unquotedCasing", "UNCHANGED");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        Statement st = connection.createStatement();
        return st.executeQuery(sql);
    }

    @Test
    public void test1() throws SQLException {
        String sql = "select * from sys_user";
        ResultSet resultSet = sql(sql);
    }

}

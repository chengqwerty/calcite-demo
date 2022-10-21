package som.make.mock.calcite.redis;

import org.apache.calcite.util.Sources;
import org.junit.jupiter.api.Test;

import java.sql.*;
import java.util.*;

public class RedisTest {

    public ResultSet sql(String sql) throws SQLException {
        String path = Sources.of(Objects.requireNonNull(ClassLoader.getSystemClassLoader().getResource("model.json"))).file().getAbsolutePath();
        Properties info = new Properties();
        info.put("model", path);
        info.put("unquotedCasing", "UNCHANGED");
        Connection connection = DriverManager.getConnection("jdbc:calcite:", info);
        Statement st = connection.createStatement();
        return st.executeQuery(sql);
    }

    public void printResult(ResultSet resultSet) throws SQLException {
        ResultSetMetaData resultSetMetaData = resultSet.getMetaData();
        int count = resultSetMetaData.getColumnCount();
        while (resultSet.next()) {
            List<String> stringList = new ArrayList<>();
            for (int i = 1; i <= count; i++) {
                stringList.add(resultSet.getString(i));
            }
            System.out.println(String.join(",", stringList));
        }
    }

    @Test
    public void testRedisStringRaw() throws SQLException {
        String sql = "select key from \"user:token:s0001\"";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

    @Test
    public void testRedisStringJson() throws SQLException {
        String sql = "select * from \"user:message:s0001\"";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

    @Test
    public void testRedisListRaw() throws SQLException {
        String sql = "select key from \"list_raw_01\"";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

    @Test
    public void testRedisListJson() throws SQLException {
        String sql = "select * from \"list_json_01\"";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

    @Test
    public void testRedisJoin1() throws SQLException {
        String sql = "select json.name, json.sex from \"list_json_01\" as json join \"list_raw_01\" as raw on json.name=raw.name";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

    @Test
    public void testRedisHashRaw() throws SQLException {
        String sql = "select name, description from \"hash_raw_01\"";
        ResultSet resultSet = sql(sql);
        printResult(resultSet);
    }

}

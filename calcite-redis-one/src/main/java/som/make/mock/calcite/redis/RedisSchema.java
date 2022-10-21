package som.make.mock.calcite.redis;

import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class RedisSchema extends AbstractSchema {

    private final List<JsonCustomTable> tables;

    private final RedisConfig redisConfig;
    private final Map<String, Table> tableMap = null;


    public RedisSchema(String host, int port, int database, String password, List<JsonCustomTable> tables) {
        this.tables = tables;
        this.redisConfig = new RedisConfig(host, port, password, database);
    }

    public RedisConfig getRedisConfig() {
        return redisConfig;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        Map<String, Table> tableMap = new HashMap<>(tables.size());
        for (JsonCustomTable jsonCustomTable: tables) {
            Map<String, Object> operand = jsonCustomTable.operand;
            String tableName = jsonCustomTable.name;
            tableMap.put(tableName, table(tableName, operand));
        }
        return tableMap;
    }

    private Table table(String tableName, Map<String, Object> operand) {
        return RedisTable.create(RedisSchema.this, redisConfig, tableName, operand, null);
    }
}

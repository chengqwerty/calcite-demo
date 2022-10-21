package som.make.mock.calcite.redis;

import org.apache.calcite.model.JsonCustomTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaFactory;
import org.apache.calcite.schema.SchemaPlus;

import java.util.List;
import java.util.Map;

public class RedisSchemaFactory implements SchemaFactory {

    @Override
    public Schema create(SchemaPlus schemaPlus, String s, Map<String, Object> map) {
        assert map.get("tables")!=null:"tables属性不能为空";
        assert map.get("host")!=null:"host属性不能为空";
        assert map.get("port")!=null:"port属性不能为空";
        assert map.get("database")!=null:"database属性不能为空";
        String host = map.get("host").toString();
        int port = (int) map.get("port");
        int database = Integer.parseInt(map.get("database").toString());
        String password = map.get("password") == null ? null
                : map.get("password").toString();
        List<JsonCustomTable> tables = (List<JsonCustomTable>) map.get("tables");
        return new RedisSchema(host, port, database, password, tables);
    }
}

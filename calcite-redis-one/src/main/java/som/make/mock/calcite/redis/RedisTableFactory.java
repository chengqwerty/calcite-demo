package som.make.mock.calcite.redis;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeImpl;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.TableFactory;
import org.checkerframework.checker.nullness.qual.Nullable;

import java.util.Map;

public class RedisTableFactory implements TableFactory<RedisTable> {

    @Override
    public RedisTable create(SchemaPlus schema, String tableName, Map<String, Object> operand, @Nullable RelDataType rowType) {
        final RedisSchema redisSchema = schema.unwrap(RedisSchema.class);
        final RelProtoDataType protoRowType =
                rowType != null ? RelDataTypeImpl.proto(rowType) : null;
        assert redisSchema != null;
        return RedisTable.create(redisSchema, redisSchema.getRedisConfig(), tableName,operand, protoRowType);
    }

}

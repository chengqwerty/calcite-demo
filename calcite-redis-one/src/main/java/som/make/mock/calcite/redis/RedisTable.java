package som.make.mock.calcite.redis;

import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelProtoDataType;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.util.Pair;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class RedisTable extends AbstractTable implements ScannableTable {

    private RedisSchema redisSchema;
    private RedisConfig redisConfig;
    private String tableName;
    private RedisTableFieldInfo redisTableFieldInfo;
    private final LinkedHashMap<String, Object> fields;
    private RelProtoDataType protoRowType;
    private RedisEnumerator redisEnumerator;

    public RedisTable(RedisSchema redisSchema, RedisConfig redisConfig, String tableName, Map<String, Object> operand, RelProtoDataType protoRowType) {
        this.redisSchema = redisSchema;
        this.redisConfig = redisConfig;
        this.tableName = tableName;
        this.protoRowType = protoRowType;
        this.redisTableFieldInfo = generateFieldInfo(operand);
        this.fields = deduceRowType(this.redisTableFieldInfo);
    }

    public static RedisTable create(RedisSchema redisSchema, RedisConfig redisConfig, String tableName, Map<String, Object> operand, RelProtoDataType protoRowType) {
        return new RedisTable(redisSchema, redisConfig, tableName, operand, protoRowType);
    }

    @Override
    public Enumerable<Object[]> scan(DataContext root) {
        return new AbstractEnumerable<>() {
            @Override
            public Enumerator<Object[]> enumerator() {
                return new RedisEnumerator(redisConfig, tableName, redisTableFieldInfo);
            }
        };
    }

    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        if (protoRowType != null) {
            return protoRowType.apply(typeFactory);
        }
        final List<RelDataType> types = new ArrayList<>(fields.size());
        final List<String> names = new ArrayList<>(fields.size());
        for (String key : fields.keySet()) {
            final RelDataType type = typeFactory.createJavaType(fields.get(key).getClass());
            names.add(key);
            types.add(type);
        }
        return typeFactory.createStructType(Pair.zip(names, types));
    }

    /**
     * 原始字段信息转化为calcite需要的字段信息，主要将原始字段type转为calcite type
     * @param tableFieldInfo 原始字段信息
     * @return calcite 字段信息
     */
    public LinkedHashMap<String, Object> deduceRowType(RedisTableFieldInfo tableFieldInfo) {
        LinkedHashMap<String, Object> fields = new LinkedHashMap<>();
        RedisDataFormat redisDataFormat = RedisDataFormat.fromTypeName(tableFieldInfo.getDataFormat());
        assert redisDataFormat != null;
        for (LinkedHashMap<String, Object> field : tableFieldInfo.getFields()) {
            // 这里因为redis都是字符串类型，所以也都是字符串类型
            fields.put(field.get("name").toString(), field.get("type").toString());
        }
        return fields;
    }

    /**
     * 获取model中配置的原始字段信息
     * @param operand 配置信息
     * @return 原始字段信息
     */
    public RedisTableFieldInfo generateFieldInfo(Map<String, Object> operand) {
        RedisTableFieldInfo tableFieldInfo = new RedisTableFieldInfo();
        List<LinkedHashMap<String, Object>> fields;
        String dataFormat;
        String keyDelimiter = "";
        if (operand.get("dataFormat") == null) {
            throw new RuntimeException("dataFormat is null");
        }
        if (operand.get("fields") == null) {
            throw new RuntimeException("fields is null");
        }
        dataFormat = operand.get("dataFormat").toString();
        fields = (List<LinkedHashMap<String, Object>>) operand.get("fields");
        if (operand.get("keyDelimiter") != null) {
            keyDelimiter = operand.get("keyDelimiter").toString();
        }
        tableFieldInfo.setTableName(tableName);
        tableFieldInfo.setDataFormat(dataFormat);
        tableFieldInfo.setFields(fields);
        if (keyDelimiter != null && !keyDelimiter.equals("")) {
            tableFieldInfo.setKeyDelimiter(keyDelimiter);
        }
        return tableFieldInfo;
    }

}

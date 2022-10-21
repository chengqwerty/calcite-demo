package som.make.mock.calcite.redis;

import java.util.LinkedHashMap;
import java.util.List;

public class RedisTableFieldInfo {

    private String tableName;
    private String dataFormat;
    private List<LinkedHashMap<String, Object>> fields;
    private String keyDelimiter = ":";

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public String getDataFormat() {
        return dataFormat;
    }

    public void setDataFormat(String dataFormat) {
        this.dataFormat = dataFormat;
    }

    public List<LinkedHashMap<String, Object>> getFields() {
        return fields;
    }

    public void setFields(List<LinkedHashMap<String, Object>> fields) {
        this.fields = fields;
    }

    public String getKeyDelimiter() {
        return keyDelimiter;
    }

    public void setKeyDelimiter(String keyDelimiter) {
        this.keyDelimiter = keyDelimiter;
    }
}

package som.make.mock.calcite.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.lettuce.core.RedisClient;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;

import java.util.*;

public class RedisDataProcess {

    private final String tableName;
    private final RedisClient redisClient;
    private final RedisTableFieldInfo redisTableFieldInfo;
    private final RedisDataFormat redisDataFormat;

    private final ObjectMapper objectMapper = new ObjectMapper();

    public RedisDataProcess(RedisClient redisClient, String tableName, RedisTableFieldInfo redisTableFieldInfo) {
        this.redisClient = redisClient;
        this.tableName = tableName;
        this.redisTableFieldInfo = redisTableFieldInfo;
        this.redisDataFormat = RedisDataFormat.fromTypeName(redisTableFieldInfo.getDataFormat());
    }

    public List<Object[]> read() throws JsonProcessingException {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        RedisCommands<String, String> syncCommands = connection.sync();
        RedisDataType redisDataType = RedisDataType.fromTypeName(syncCommands.type(tableName));
        return switch (Objects.requireNonNull(redisDataType)) {
            case STRING -> parseRedisString(syncCommands, tableName);
            case LIST -> parseRedisList(syncCommands.lrange(tableName, 0, -1));
            case HASH -> parseRedisHash(syncCommands.hgetall(tableName));
            default -> null;
        };
    }

    private List<Object[]> parseRedisString(RedisCommands<String, String> syncCommands, String pattern) throws JsonProcessingException {
        List<Object[]> result = new ArrayList<>();
        List<String> keys = syncCommands.keys(pattern);
        for (String key: keys) {
            String value = syncCommands.get(key);
            if (redisDataFormat == RedisDataFormat.RAW) {
                result.add(new Object[]{value});
            } else if (redisDataFormat == RedisDataFormat.JSON) {
                result.add(parseJson(value));
            } else {
                throw new RuntimeException("redis数据类型与格式转化类型不匹配!");
            }
        }
        return result;
    }

    private List<Object[]> parseRedisList(List<String> list) throws JsonProcessingException {
        List<Object[]> result = new ArrayList<>();
        for (String value: list) {
            if (redisDataFormat == RedisDataFormat.RAW) {
                result.add(new Object[]{value});
            } else if (redisDataFormat == RedisDataFormat.JSON) {
                result.add(parseJson(value));
            } else {
                throw new RuntimeException("redis数据类型与格式转化类型不匹配!");
            }
        }
        return result;
    }

    private List<Object[]> parseRedisHash(Map<String, String> map) {
        List<Object[]> result = new ArrayList<>();
        if (redisDataFormat == RedisDataFormat.RAW) {
            List<LinkedHashMap<String, Object>> fields = redisTableFieldInfo.getFields();
            Object[] arr = new Object[fields.size()];
            Object obj;
            for (int i = 0; i < arr.length; i++) {
                obj = fields.get(i).get("mapping");
                if (obj == null) {
                    arr[i] = "";
                } else {
                    arr[i] = map.get(fields.get(i).get("mapping").toString());
                }
            }
            result.add(arr);
        } else {
            throw new RuntimeException("redis数据类型与格式转化类型不匹配!");
        }
        return result;
    }

    /**
     * 解析json字符串到数组
     * @param content 要解析的字符串
     * @return 字符串数组
     * @throws JsonProcessingException 解析异常
     */
    private Object[] parseJson(String content) throws JsonProcessingException {
        List<LinkedHashMap<String, Object>> fields = redisTableFieldInfo.getFields();
        JsonNode jsonNode = objectMapper.readTree(content);
        Object[] arr = new Object[fields.size()];
        Object obj;
        for (int i = 0; i < arr.length; i++) {
            obj = fields.get(i).get("mapping");
            if (obj == null) {
                arr[i] = "";
            } else {
                arr[i] = jsonNode.findValue(fields.get(i).get("mapping").toString()).textValue();
            }
        }
        return arr;
    }

}

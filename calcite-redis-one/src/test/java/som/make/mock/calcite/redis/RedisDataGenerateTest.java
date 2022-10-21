package som.make.mock.calcite.redis;

import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import io.lettuce.core.api.StatefulRedisConnection;
import io.lettuce.core.api.sync.RedisCommands;
import org.junit.jupiter.api.*;

import java.util.HashMap;
import java.util.Map;

/**
 * 此类用来生产redis测试数据
 * 请修改redis的ip、port、密码
 */
public class RedisDataGenerateTest {

    private String redisHost = "172.16.180.242";
    private int redisPort = 6379;
    private final StringBuilder password = new StringBuilder();
    private RedisClient redisClient;

    @BeforeEach
    public void redisClient() {
        RedisURI redisURI = RedisURI.builder().withHost(redisHost).withPort(redisPort).withPassword(password).build();
        redisClient = RedisClient.create(redisURI);
    }

    private RedisCommands<String, String> getRedisSyncCommands() {
        StatefulRedisConnection<String, String> connection = redisClient.connect();
        return connection.sync();
    }

    @Test
    public void generateData() {
        RedisCommands<String, String> syncCommands = getRedisSyncCommands();
        syncCommands.set("user:token:s0001", "mock");
        syncCommands.set("user:message:s0001", "{\"name\":\"谢润萍\",\"sex\":\"女\"}");
        syncCommands.rpush("list_raw_01", "谢润萍", "姚婻", "王凤", "韩妙玉");
        syncCommands.rpush("list_json_01", "{\"name\":\"谢润萍\",\"sex\":\"女\"}", "{\"name\":\"姚婻\",\"sex\":\"女\"}");
        Map<String, String> map = new HashMap<>();
        map.put("name", "海棠");
        map.put("description", "海棠白");
        map.put("age", "20");
        syncCommands.hmset("hash_raw_01", map);
    }

    @Test
    public void removeData() {
        RedisCommands<String, String> syncCommands = getRedisSyncCommands();
        syncCommands.del("user:token:s0001", "user:message:s0001", "list_raw_01", "list_json_01", "hash_raw_01");
    }

    @AfterEach
    public void destroy() {
        redisClient.close();
    }

}

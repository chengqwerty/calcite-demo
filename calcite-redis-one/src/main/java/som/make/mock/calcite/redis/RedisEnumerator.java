package som.make.mock.calcite.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import io.lettuce.core.RedisClient;
import io.lettuce.core.RedisURI;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.linq4j.Linq4j;

public class RedisEnumerator implements Enumerator<Object[]> {

    private final Enumerator<Object[]> enumerator;

    public RedisEnumerator(RedisConfig redisConfig, String tableName, RedisTableFieldInfo redisTableFieldInfo) {
        RedisURI redisURI = RedisURI.builder().withHost(redisConfig.getHost()).withPort(redisConfig.getPort()).build();
        try (RedisClient redisClient = RedisClient.create(redisURI)) {
            RedisDataProcess redisDataProcess = new RedisDataProcess(redisClient, tableName, redisTableFieldInfo);
            enumerator = Linq4j.enumerator(redisDataProcess.read());
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Object[] current() {
        return enumerator.current();
    }

    @Override
    public boolean moveNext() {
        return enumerator.moveNext();
    }

    @Override
    public void reset() {
        enumerator.reset();
    }

    @Override
    public void close() {
        enumerator.close();
    }
}

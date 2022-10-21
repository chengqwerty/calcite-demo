package som.make.mock.calcite.redis;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

public class ObjectMapperTest {

    @Test
    public void test1() throws JsonProcessingException {
        String name = "谢润萍";
        System.out.println(name);
        String json = "{\"sex\":\"女\",\"name\":\"谢润萍\"}";
        ObjectMapper objectMapper = new ObjectMapper();
        JsonNode jsonNode = objectMapper.readTree(json);
        System.out.println(jsonNode.findValue("name").textValue());
        User user = objectMapper.readValue(json, User.class);
        System.out.println(user.getName());
    }

    @Test
    public void test2() throws JsonProcessingException {
        Map<String, String> map = new HashMap<>();
        map.put("name", "谢润萍");
        map.put("sex", "女");
        ObjectMapper objectMapper = new ObjectMapper();
        System.out.println(objectMapper.writeValueAsString(map));
    }
}

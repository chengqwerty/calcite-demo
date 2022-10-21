package som.make.mock.calcite.redis;

public enum RedisDataFormat {

    RAW("raw"),
    JSON("json");

    private final String typeName;

    RedisDataFormat(String typeName) {
        this.typeName = typeName;
    }

    public static RedisDataFormat fromTypeName(String typeName) {
        for (RedisDataFormat type : RedisDataFormat.values()) {
            if (type.getTypeName().equals(typeName)) {
                return type;
            }
        }
        return null;
    }

    public String getTypeName() {
        return this.typeName;
    }

}

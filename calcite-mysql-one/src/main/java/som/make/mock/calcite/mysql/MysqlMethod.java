package som.make.mock.calcite.mysql;

import org.apache.calcite.linq4j.tree.Types;

import java.lang.reflect.Method;
import java.util.List;

public enum MysqlMethod {

    Mysql_Method_QUERYABLE_QUERY(MysqlQueryable.class, "query", List.class, List.class);

    public final Method method;

    MysqlMethod(Class<?> clazz, String methodName, Class<?>... argumentTypes) {
        this.method = Types.lookupMethod(clazz, methodName, argumentTypes);
    }

}

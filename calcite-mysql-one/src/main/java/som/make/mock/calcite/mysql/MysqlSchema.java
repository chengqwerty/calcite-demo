package som.make.mock.calcite.mysql;

import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;

import java.util.Map;

public class MysqlSchema extends AbstractSchema {

    private final Map<String, Table> tableMap;

    public MysqlSchema(Map<String, Table> tableMap) {
        this.tableMap = tableMap;
    }

    @Override
    protected Map<String, Table> getTableMap() {
        return tableMap;
    }

}

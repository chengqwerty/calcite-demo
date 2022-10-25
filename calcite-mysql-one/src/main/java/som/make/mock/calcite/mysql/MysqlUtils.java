package som.make.mock.calcite.mysql;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.validate.SqlValidatorUtil;

import java.util.List;

public class MysqlUtils {
    
    public static List<String> mysqlFieldNames(final RelDataType rowType) {
        return SqlValidatorUtil.uniquify(rowType.getFieldNames(), SqlValidatorUtil.EXPR_SUGGESTER,
                true);
    }

}

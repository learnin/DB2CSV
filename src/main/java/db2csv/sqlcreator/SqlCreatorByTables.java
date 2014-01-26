package db2csv.sqlcreator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

public class SqlCreatorByTables implements SqlCreator {

    private String tableNames;

    public SqlCreatorByTables(String tableNames) {
        this.tableNames = tableNames;
    }

    @Override
    public Iterator<Sql> iterator() {
        List<Sql> sqls = new ArrayList<>();
        for (String tableName : tableNames.split(",")) {
            sqls.add(new Sql(tableName, "SELECT * FROM " + tableName));
        }
        return sqls.iterator();
    }
}

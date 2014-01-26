package db2csv.sqlcreator;

public class SqlCreatorFactory {

    public static SqlCreator create(String... args) {
        switch (args[0]) {
            case "-tables":
                return new SqlCreatorByTables(args[1]);
            case "-sql":
                return new SqlCreatorBySql(args[1]);
            case "-sqlFile":
                return new SqlCreatorBySqlFile(args[1]);
            case "-sqlFilesDir":
                return new SqlCreatorBySqlFilesDir(args[1]);
        }
        throw new IllegalArgumentException();

    }
}

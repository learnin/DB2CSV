package db2csv.sqlcreator;

public class Sql {

    private String sqlName;
    private String sql;

    public Sql(String sqlName, String sql) {
        this.sqlName = sqlName;
        this.sql = sql;
    }

    public String toString() {
        return sql;
    }

    public String getSqlName() {
        return sqlName;
    }
}

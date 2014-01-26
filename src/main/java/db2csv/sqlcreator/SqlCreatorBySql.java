package db2csv.sqlcreator;

import java.util.Iterator;

public class SqlCreatorBySql implements SqlCreator {

    private Sql sql;

    public SqlCreatorBySql(String sql) {
        this.sql = new Sql("sql", sql);
    }

    @Override
    public Iterator<Sql> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<Sql> {
        private boolean hasNext = true;

        @Override
        public boolean hasNext() {
            return hasNext;
        }

        @Override
        public Sql next() {
            hasNext = false;
            return sql;
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

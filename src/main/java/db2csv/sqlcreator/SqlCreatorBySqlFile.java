package db2csv.sqlcreator;

import db2csv.util.FilesUtil;

import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Iterator;

public class SqlCreatorBySqlFile implements SqlCreator {

    private String sqlFilePath;

    public SqlCreatorBySqlFile(String sqlFilePath) {
        this.sqlFilePath = sqlFilePath;
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
            Path path = FileSystems.getDefault().getPath(sqlFilePath);
            if (!Files.exists(path)) {
                throw new IllegalArgumentException(sqlFilePath + " is not found.");
            }
            String sqlFileName = path.getFileName().toString();
            return new Sql(sqlFileName.substring(0, sqlFileName.lastIndexOf(".")), FilesUtil.readAllLines(path, StandardCharsets.UTF_8));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

package db2csv.sqlcreator;

import db2csv.util.FilesUtil;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.FileVisitOption;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.ConcurrentModificationException;
import java.util.EnumSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

public class SqlCreatorBySqlFilesDir implements SqlCreator {

    private List<Path> sqlFilePaths;

    public SqlCreatorBySqlFilesDir(String sqlFilesDir) {
        sqlFilePaths = new ArrayList<>();
        try {
            Files.walkFileTree(FileSystems.getDefault().getPath(sqlFilesDir), EnumSet.noneOf(FileVisitOption.class), 1, new SimpleFileVisitor<Path>() {
                @Override
                public FileVisitResult visitFile(Path path, BasicFileAttributes attrs) throws IOException {
                    if (path.toString().toLowerCase().endsWith(".sql")) {
                        sqlFilePaths.add(path);
                    }
                    return FileVisitResult.CONTINUE;
                }
            });
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public Iterator<Sql> iterator() {
        return new Itr();
    }

    private class Itr implements Iterator<Sql> {
        // index of next element to return
        int cursor;

        @Override
        public boolean hasNext() {
            return cursor != sqlFilePaths.size();
        }

        @Override
        public Sql next() {
            int i = cursor;
            if (i >= sqlFilePaths.size()) {
                throw new NoSuchElementException();
            }
            List<Path> sqlFilePaths = SqlCreatorBySqlFilesDir.this.sqlFilePaths;
            if (i >= sqlFilePaths.size())
                throw new ConcurrentModificationException();
            cursor = i + 1;
            Path sqlFilePath = sqlFilePaths.get(i);
            String sqlFileName = sqlFilePath.getFileName().toString();
            return new Sql(sqlFileName.substring(0, sqlFileName.lastIndexOf(".")), FilesUtil.readAllLines(sqlFilePath, StandardCharsets.UTF_8));
        }

        @Override
        public void remove() {
            throw new UnsupportedOperationException();
        }
    }
}

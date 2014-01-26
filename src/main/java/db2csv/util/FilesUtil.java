package db2csv.util;

import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class FilesUtil {

    public static String readAllLines(Path path, Charset charset) {
        List<String> lines;
        try {
            lines = Files.readAllLines(path, charset);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        StringBuilder result = new StringBuilder();
        for (String line : lines) {
            result.append(line).append(System.lineSeparator());
        }
        return result.toString();
    }
}
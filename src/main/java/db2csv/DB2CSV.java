package db2csv;

import db2csv.sqlcreator.Sql;
import db2csv.sqlcreator.SqlCreator;
import db2csv.sqlcreator.SqlCreatorFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystem;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ResourceBundle;

public class DB2CSV {

    private String jdbcUrl;
    private String username;
    private String password;

    private String distDir = "dist";

    private String dateFormat = "yyyy-MM-dd'T'HH:mm:ss";
    private String separator = ",";
    private String lineSeparator = System.lineSeparator();
    private Quote quote = Quote.ONLY_STRING;
    private String quoteChar = "\"";
    private String escapeChar = "\"";
    private boolean isHeader = true;
    private Charset csvCharset = StandardCharsets.UTF_8;

    private SqlCreator sqlCreator;

    private DB2CSV(SqlCreator sqlCreator) {
        this.sqlCreator = sqlCreator;
    }

    public static void main(String... args) {

        if (args == null || args.length != 2) {
            printUsage();
            System.exit(1);
        }
        SqlCreator sqlCreator = null;
        try {
            sqlCreator = SqlCreatorFactory.create(args);
        } catch (IllegalArgumentException e) {
            printUsage();
            System.exit(1);
        }
        DB2CSV db2csv = new DB2CSV(sqlCreator);
        try {
            db2csv.loadSetting();
            db2csv.execute();
        } catch (IllegalSettingException e) {
            System.err.println(e.getMessage());
            System.exit(1);
        } catch (SQLException | IOException e) {
            e.printStackTrace();
            System.exit(2);
        }
    }

    private void loadSetting() {
        jdbcUrl = Setting.get("jdbcUrl");
        username = Setting.get("username");
        password = Setting.get("password");

        if (!Setting.get("distDir").isEmpty()) {
            distDir = Setting.get("distDir");
        }
        if (!Setting.get("dateFormat").isEmpty()) {
            dateFormat = Setting.get("dateFormat");
        }
        if (!Setting.get("separator").isEmpty()) {
            separator = Setting.get("separator");
        }
        if (!Setting.get("lineSeparator").isEmpty()) {
            switch (Setting.get("lineSeparator")) {
                case "CR":
                    lineSeparator = "\r";
                    break;
                case "LF":
                    lineSeparator = "\n";
                    break;
                case "CRLF":
                case "CR+LF":
                    lineSeparator = "\r\n";
                    break;
                default:
                    throw new IllegalSettingException("unsupported lineSeparator. you can set it to 'CR' or 'LF' or 'CR+LF'.");
            }
        }
        if (!Setting.get("quote").isEmpty()) {
            switch (Setting.get("quote")) {
                case "onlyString":
                    quote = Quote.ONLY_STRING;
                    break;
                case "always":
                    quote = Quote.ALWAYS;
                    break;
                case "none":
                    quote = Quote.NONE;
                    break;
                default:
                    throw new IllegalSettingException("unsupported quote. you can set it to 'onlyString' or 'always' or 'none'.");
            }
        }
        if (quote != Quote.NONE) {
            if (!Setting.get("quoteChar").isEmpty()) {
                quoteChar = Setting.get("quoteChar");
            }
            if (!Setting.get("escapeChar").isEmpty()) {
                switch (Setting.get("escapeChar")) {
                    case "quoteChar":
                        escapeChar = quoteChar;
                        break;
                    case "backSlash":
                        escapeChar = "\\\\";
                        break;
                    default:
                        throw new IllegalSettingException("unsupported escapeChar. you can set it to 'quoteChar' or 'backSlash'.");
                }
            }
        }
        if (!Setting.get("outputHeader?").isEmpty()) {
            switch (Setting.get("outputHeader?")) {
                case "true":
                    isHeader = true;
                    break;
                case "false":
                    isHeader = false;
                    break;
                default:
                    throw new IllegalSettingException("unsupported outputHeader. you can set it to 'true' or 'false'.");
            }
        }
        if (!Setting.get("csvCharset").isEmpty()) {
            csvCharset = Charset.forName(Setting.get("csvCharset"));
        }
    }

    private void execute() throws SQLException, IOException {
        FileSystem fs = FileSystems.getDefault();
        Path distPath = Files.createDirectories(fs.getPath(distDir));
        try (Connection conn = DriverManager.getConnection(jdbcUrl, username, password)) {
            for (Sql sql : sqlCreator) {
                try (PreparedStatement ps = conn.prepareStatement(sql.toString());
                     ResultSet rs = ps.executeQuery();
                     BufferedWriter writer = Files.newBufferedWriter(fs.getPath(distPath.toString(), sql.getSqlName() + ".csv"), csvCharset, StandardOpenOption.CREATE_NEW, StandardOpenOption.APPEND)) {
                    ResultSetMetaData rmd = rs.getMetaData();
                    if (isHeader) {
                        for (int i = 1; i <= rmd.getColumnCount(); i++) {
                            if (i > 1) {
                                writer.write(separator);
                            }
                            String text = rmd.getColumnName(i);
                            if (quote == Quote.ALWAYS || quote == Quote.ONLY_STRING) {
                                text = quoteChar + text + quoteChar;
                            }
                            writer.write(text);
                        }
                        writer.write(lineSeparator);
                    }
                    while (rs.next()) {
                        for (int i = 1; i <= rmd.getColumnCount(); i++) {
                            if (i > 1) {
                                writer.write(separator);
                            }
                            String text;
                            if (rs.getString(i) == null) {
                                text = "";
                            } else if ("java.sql.Timestamp".equals(rmd.getColumnClassName(i))) {
                                DateFormat format = new SimpleDateFormat(dateFormat);
                                text = format.format(rs.getTimestamp(i));
                            } else {
                                text = rs.getString(i);
                            }
                            if (quote == Quote.ALWAYS || (quote == Quote.ONLY_STRING && "java.lang.String".equals(rmd.getColumnClassName(i)))) {
                                text = quoteChar + text.replaceAll(quoteChar, escapeChar + quoteChar) + quoteChar;
                            }
                            writer.write(text);

                        }
                        writer.write(lineSeparator);
                    }
                }
            }
        }
        System.out.println("output csv files to " + distDir);
    }

    private static void printUsage() {
        System.out.println("Usage: DB2CSV -tables tableName,tableName,...");
        System.out.println("       e.g. DB2CSV -tables foo,bar");
        System.out.println("");
        System.out.println(" or DB2CSV -sql 'sql string'");
        System.out.println("       e.g. DB2CSV -sql 'SELECT * FROM foo'");
        System.out.println("");
        System.out.println(" or DB2CSV -sqlFile path/to/sqlfile");
        System.out.println("       e.g. DB2CSV -sqlFile foo.sql");
        System.out.println("");
        System.out.println(" or DB2CSV -sqlFilesDir path/to/sqlfileDir");
        System.out.println("       e.g. DB2CSV -sqlFilesDir foo");
    }

    private enum Quote {
        ONLY_STRING,
        ALWAYS,
        NONE;
    }

    private static class Setting {

        private static String get(String key) {
            ResourceBundle rb = ResourceBundle.getBundle("setting");
            return rb.getString(key);
        }
    }

    private static class IllegalSettingException extends RuntimeException {

        private IllegalSettingException(String message) {
            super(message);
        }
    }

}

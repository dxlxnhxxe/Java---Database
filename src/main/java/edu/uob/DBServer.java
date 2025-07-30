package edu.uob;

import java.io.*;
import java.util.*;
import java.util.regex.*;
import java.io.File;
import java.net.ServerSocket;
import java.net.Socket;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.stream.Collectors;

/** This class implements the DB server. */
public class DBServer {

    private static final Set<String> RESERVED_KEYWORDS = new HashSet<>(Arrays.asList(
            "use", "select", "insert", "update", "delete", "drop", "join", "*", "into",
            "create", "from", "where", "and", "or", "like", "table", "database", "like"
    ));
    private static final char END_OF_TRANSMISSION = 4;
    private String storageFolderPath;
    private String activeDatabase = null;

    public enum ValueType {
        STRING,
        INTEGER,
        FLOAT,
        BOOLEAN,
        NULL,
    }
    /**
     * KEEP this signature otherwise we won't be able to mark your submission correctly.
     */
    public DBServer() {
        storageFolderPath = Paths.get("databases").toAbsolutePath().toString();
        try {
            // Create the database storage folder if it doesn't already exist !
            Files.createDirectories(Paths.get(storageFolderPath));
        } catch(IOException ioe) {
            System.out.println("Can't seem to create database storage folder " + storageFolderPath);
        }
    }

    public static void main(String args[]) throws IOException {
        edu.uob.DBServer server = new edu.uob.DBServer();
        server.blockingListenOn(8888);
    }

    public boolean deleteDirectory(File directory) {
        // If the directory doesn't exist or isn't a directory, return false
        if (directory == null || !directory.exists() || !directory.isDirectory()) {
            return false;
        }
        // Get all files in the directory
        File[] files = directory.listFiles();
        // If the directory is empty, just delete it
        if (files != null) {
            for (File file : files) {
                // Recursively delete files/subdirectories
                if (file.isDirectory()) {
                    deleteDirectory(file); // Recursively delete subdirectories
                } else {
                    file.delete(); // Delete the file
                }
            }
        }
        // Finally, delete the directory itself
        return directory.delete();
    }

    private double compareAsDouble(Object a, Object b) {
        return Double.compare(Double.parseDouble(a.toString()), Double.parseDouble(b.toString()));
    }

    public edu.uob.DBServer.ValueType identifyValueType(String value) {
        value = value.trim();
        if (value.startsWith("'") && value.endsWith("'")){
            return edu.uob.DBServer.ValueType.STRING;
        }
        if (value.equalsIgnoreCase("TRUE") || value.equalsIgnoreCase("FALSE")){
            return edu.uob.DBServer.ValueType.BOOLEAN;
        }
        if (value.equalsIgnoreCase("NULL")){
            return edu.uob.DBServer.ValueType.NULL;
        }
        try {
            Integer.parseInt(value);
            return edu.uob.DBServer.ValueType.INTEGER;
        } catch (NumberFormatException ignored) {}
        try {
            Float.parseFloat(value);
            return edu.uob.DBServer.ValueType.FLOAT;
        } catch(NumberFormatException ignored){}
        if (value.matches(".*[\\(\\)\\+\\-\\s].*") && !(value.startsWith("'") && value.endsWith("'"))) {
            return edu.uob.DBServer.ValueType.STRING;
        }
        return edu.uob.DBServer.ValueType.NULL;
    }
    private Object parseValueByType(String value, edu.uob.DBServer.ValueType type) {
        return switch (type) {
            case STRING -> value.substring(1, value.length() - 1);
            case INTEGER -> Integer.parseInt(value);
            case FLOAT -> Float.parseFloat(value);
            case BOOLEAN -> Boolean.parseBoolean(value);
            case NULL ->  null;
            default ->  value;
        };
    }

    private List<String> tokenizeCondition(String condition) {
        List<String> tokens = new ArrayList<>();
        Pattern pattern = Pattern.compile(
                "\\(|\\)|" +
                        "(?i)\\bAND\\b|(?i)\\bOR\\b|" +
                        ">=|<=|==|!=|>|<|(?i)LIKE|" +
                        "'[^']*'|" +
                        "\\d+\\.\\d+|\\d+|" +
                        "[a-zA-Z_][a-zA-Z0-9_]*"
        );
        Matcher matcher = pattern.matcher(condition);
        while (matcher.find()) {
            tokens.add(matcher.group().trim());
        }
        return tokens;
    }

    private int currentTokenIndex = 0;

    private edu.uob.DBServer.ConditionNode parsePrimary(List<String> tokens) {
        String token = tokens.get(currentTokenIndex);
        if (token.equals("(")) {
            currentTokenIndex++;
            edu.uob.DBServer.ConditionNode node = parseCondition(tokens);
            currentTokenIndex++;
            return node;
        } else {
            String attribute = tokens.get(currentTokenIndex++);
            String comparator = tokens.get(currentTokenIndex++);
            String value = tokens.get(currentTokenIndex++);

            return new edu.uob.DBServer.SimpleCondition(attribute, comparator, value);

        }
    }

    private edu.uob.DBServer.ConditionNode parseCondition(List<String> tokens) {
        edu.uob.DBServer.ConditionNode node = parsePrimary(tokens);
        while (currentTokenIndex < tokens.size()){
            String token = tokens.get(currentTokenIndex);
            if (token.equalsIgnoreCase("AND") || token.equalsIgnoreCase("OR")){
                currentTokenIndex++;
                edu.uob.DBServer.ConditionNode rightNode = parsePrimary(tokens);
                node = new edu.uob.DBServer.BooleanCondition(node, token, rightNode);
            } else {
                break;
            }
        }
        return node;
    }

    public static abstract class ConditionNode {
        public abstract boolean evaluate(Map<String, String> row);
    }

    public static class SimpleCondition extends edu.uob.DBServer.ConditionNode {
        private final String attribute;
        private final String comparator;
        private final String value;
        private final edu.uob.DBServer.ValueType valueType;
        private final Object parsedValue;

        public SimpleCondition(String attribute, String comparator, String value) {
            this.attribute = attribute;
            this.comparator = comparator;
            this.value = value;
            this.valueType = new edu.uob.DBServer().identifyValueType(value);
            this.parsedValue = new edu.uob.DBServer().parseValueByType(value, valueType);
        }

        @Override
        public boolean evaluate(Map<String, String> row) {
            String cellValueRaw = row.getOrDefault(attribute, "NULL");
            Object cellValue;
            try {
                cellValue = switch (valueType) {
                    case STRING ->  cellValueRaw;
                    case INTEGER -> Integer.parseInt(cellValueRaw);
                    case FLOAT -> Float.parseFloat(cellValueRaw);
                    case BOOLEAN -> Boolean.parseBoolean(cellValueRaw);
                    case NULL -> cellValueRaw.equalsIgnoreCase("NULL") ? null : cellValueRaw;
                };
            } catch (Exception e) {
                cellValue = cellValueRaw;
            }

            return switch (comparator) {
                case "==" -> Objects.equals(cellValue, parsedValue);
                case "!=" -> !Objects.equals(cellValue, parsedValue);
                case ">" -> new edu.uob.DBServer().compareAsDouble(cellValue, parsedValue) > 0;
                case "<" -> new edu.uob.DBServer().compareAsDouble(cellValue, parsedValue) < 0;
                case "<=" -> new edu.uob.DBServer().compareAsDouble(cellValue, parsedValue) <= 0;
                case ">=" -> new edu.uob.DBServer().compareAsDouble(cellValue, parsedValue) >= 0;
                case "LIKE" -> String.valueOf(cellValue).toLowerCase().contains(value.replace("'", ""));
                default -> false;
            };
        }
    }

    public static class BooleanCondition extends edu.uob.DBServer.ConditionNode {
        private final String operator;
        private final edu.uob.DBServer.ConditionNode left;
        private final edu.uob.DBServer.ConditionNode right;

        public BooleanCondition(edu.uob.DBServer.ConditionNode left, String operator, edu.uob.DBServer.ConditionNode right){
            this.left = left;
            this.operator = operator;
            this.right = right;
        }

        @Override
        public boolean evaluate(Map<String, String> row) {
            if (operator.equalsIgnoreCase("AND")){
                return left.evaluate(row) && right.evaluate(row);
            } else if (operator.equalsIgnoreCase("OR")){
                return left.evaluate(row) || right.evaluate(row);
            }
            return false;
        }
    }

    /**
     * KEEP this signature (i.e. {@code edu.uob.DBServer.handleCommand(String)}) otherwise we won't be
     * able to mark your submission correctly.
     *
     * <p>This method handles all incoming DB commands and carries out the required actions.
     */
    public String handleCommand(String command) {
        // TODO implement your server logic here
        // If no command return error
        try {
            if (command == null || command.trim().isEmpty()){
                return "[ERROR] Empty command";
            }
            String formattedCommand = command.trim().replaceAll("\\s+", " ");
            String[] parts = formattedCommand.split("\\s+");
            if (parts.length < 2) {
                return "[ERROR] Invalid syntax or missing argument";
            }
            String keyword = parts[0].toUpperCase();

            //USE database;
            if (keyword.equals("USE")) {
                String useCommand = parts[0] + " " + parts[1];
                if (!useCommand.endsWith(";")) {
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                String databaseName = parts[1].replace(";", "").toLowerCase();
                if (RESERVED_KEYWORDS.contains(databaseName)) {
                    return "[ERROR] Reserved SQL keyword used as a database name.";
                }
                File dbFolder = new File(storageFolderPath, databaseName);
                if (dbFolder.exists() && dbFolder.isDirectory()) {
                    activeDatabase = databaseName;
                    return "[OK] Database " + activeDatabase + " selected.";
                } else {
                    return "[ERROR] Database does not exist in this directory";
                }
            }
            //CREATE DATABASE/TABLE database_name/table_name (name, age, phone number, ...):
            if (keyword.equals("CREATE")) {
                if (!formattedCommand.endsWith(";")) {
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                formattedCommand = formattedCommand.substring(0, formattedCommand.length() - 1).trim();
                if (parts.length < 3) {
                    return "Invalid syntax for CREATE.";
                }
                String objectType = parts[1].toUpperCase();
                String objectName = parts[2].toLowerCase().replace(";", "");
                if (RESERVED_KEYWORDS.contains(objectName)) {
                    return "[ERROR] Reserved SQL keyword used as a Table/Database name";
                }
                if (objectType.equals("DATABASE")) {
                    if (parts.length != 3) {
                        return "[ERROR] Invalid syntax for CREATE DATABASE. Expected format: CREATE DATABASE database_name;";
                    }
                    File dbFolder = new File(storageFolderPath, objectName);
                    if (dbFolder.exists() && dbFolder.isDirectory()) {
                        return "[ERROR] Database already exists in this path";
                    }
                    if (!dbFolder.mkdir()) {
                        return "[ERROR] Failed to create database";
                    } else {
                        return "[OK] Database created.";
                    }
                }
                if (objectType.equals("TABLE")) {
                    if (activeDatabase == null) {
                        return "[ERROR] No database selected. Use 'Use database_name' first.";
                    }
                    int startIndex = formattedCommand.indexOf("(");
                    int endIndex = formattedCommand.indexOf(")");
                    if (startIndex ==-1 || endIndex ==-1 || startIndex > endIndex) {
                        return "[ERROR] Invalid syntax for CREATE TABLE. Missing column definitions.";
                    }
                    String tableColumns = formattedCommand.substring(startIndex + 1, endIndex).trim();
                    if (tableColumns.isEmpty()) {
                        return "[ERROR] No columns defined";
                    }

                    String[] columns = tableColumns.split(",");
                    for (int i = 0; i < columns.length; i++) {
                        columns[i] = columns[i].trim();
                        if (columns[i].isEmpty()) {
                            return "[ERROR] Column names cannot be Empty";
                        }
                    }
                    File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + objectName + ".tab");
                    if (tableFile.exists() && tableFile.isFile()) {
                        return "[ERROR] Table already exists in this database";
                    }
                    try {
                        if (tableFile.createNewFile()) {
                            String formattedColumnNames = tableColumns.replaceAll("\\s*,\\s*", "\t");
                            try (BufferedWriter bufferedWriter = new BufferedWriter(new FileWriter(tableFile))) {
                                bufferedWriter.write("id\t" + formattedColumnNames);
                                bufferedWriter.newLine();
                                return "[OK] Table created.";
                            } catch (IOException e) {
                                return "[ERROR] Failed to create table";
                            }
                        }
                    } catch (IOException e) {
                        return "[ERROR] Error occurred while creating table";
                    }
                }
            }
            //"DROP DATABASE/TABLE table_name":
            if (keyword.equals("DROP")) {
                if (parts.length != 3) {
                    return "[ERROR] Invalid DROP command. Expected format 'DROP DATABASE/TABLE database_name/table_name;";
                }
                String objectType = parts[1].toUpperCase();
                String objectName = parts[2].toLowerCase();
                String dropCommand = String.join(" ", parts);

                if (!dropCommand.endsWith(";")) {
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                objectName = objectName.substring(0,objectName.length() - 1);
                if (RESERVED_KEYWORDS.contains(objectName)) {
                    return "[ERROR] Reserved SQL keyword used as a name";
                }
                if (objectType.equalsIgnoreCase("DATABASE")) {
                    File dbFolder = new File(storageFolderPath, objectName);
                    if (!dbFolder.exists() || !dbFolder.isDirectory()) {
                        return "[ERROR] Database does not exist in this directory";
                    }
                    if (deleteDirectory(dbFolder)) {
                        if (activeDatabase != null && activeDatabase.equals(objectName)    ) {
                            activeDatabase = null;
                        }
                        return "[OK] Database dropped.";
                    } else {
                        return "Failed to drop database.";
                    }

                } else if (objectType.equalsIgnoreCase("TABLE")) {
                    if (activeDatabase == null) {
                        return "[ERROR] No database selected. Use 'Use database_name' first.";
                    }
                    File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + objectName + ".tab");
                    if (!tableFile.exists() || !tableFile.isFile()) {
                        return "[ERROR] Table does not exist in this database";
                    }
                    if (tableFile.delete()) {
                        return "[OK] Table dropped.";
                    }
                    else{
                        return "[ERROR] Failed to delete table";
                    }
                }
            }
            //ALTER TABLE table_name ADD / DROP  columnToAlter:
            if (keyword.equals("ALTER")) {
                if (parts.length < 5){
                    return "[ERROR] Invalid syntax for ALTER TABLE. Expected format: ALTER TABLE table_name ADD/DROP column_name";
                }
                String alterCommand = String.join(" ", parts);
                if (!alterCommand.endsWith(";")) {
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                String tableObject = parts[1].toLowerCase();
                String tableName = parts[2].toLowerCase();
                String subCommand = parts[3].toUpperCase();
                String columnToAlter = parts[4].substring(0,parts[4].length() - 1);
                if (RESERVED_KEYWORDS.contains(tableName)) {
                    return "[ERROR] Reserved SQL keyword used as a name";
                }
                if (!tableObject.equals("table")) {
                    return "[ERROR] Invalid ALTER command. Only ALTER TABLE is supported";
                }
                if (activeDatabase == null) {
                    return "[ERROR] No database selected. Use 'Use database_name' first.";
                }
                if (columnToAlter.equals("id")) {
                    return "[ERROR] Cannot drop this column.";
                }
                File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + tableName + ".tab");
                if (!tableFile.exists() || !tableFile.isFile()) {
                    return "[ERROR] Table does not exist in this database";
                }
                if (subCommand.equals("ADD") && !columnToAlter.isEmpty()){
                    try {
                        List<String> lines = new ArrayList<>();
                        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                            String header = reader.readLine();
                            if (header.contains("\t" + columnToAlter)) {
                                return "[ERROR] Column '" + columnToAlter + "' already exists in this table" + tableName + "'.";
                            }
                            lines.add(header + "\t" + columnToAlter);
                            String line;
                            while ((line = reader.readLine()) != null) {
                                lines.add(line + "\tNULL");
                            }
                        }
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
                            for (String line : lines) {
                                writer.write(line);
                                writer.newLine();
                            }
                            return "[OK] Column '" + columnToAlter + "' added to '" + tableName + "'.";
                        }
                    } catch (IOException e) {
                        return "[ERROR] Failed to add column " + columnToAlter + " to table " + tableObject;
                    }
                }
                //HANDLE DROP column_name
                if (subCommand.equals("DROP") && !columnToAlter.isEmpty()){
                    try{
                        List<String> lines = new ArrayList<>();
                        boolean columnFound = false;

                        try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                            String[] headers = reader.readLine().split("\t");
                            List<String> newHeaders = new ArrayList<>();
                            for (String header : headers) {
                                if (!header.equalsIgnoreCase(columnToAlter)) {
                                    newHeaders.add(header);
                                } else {
                                    columnFound = true;
                                }
                            }
                            if (!columnFound) {
                                return "[ERROR] Column '" + columnToAlter + "' does not exist in the table '" + tableName + "'.";
                            }
                            lines.add(String.join("\t", newHeaders));

                            String line;
                            while ((line = reader.readLine()) != null) {
                                String[] values = line.split("\t");
                                List<String> newValues = new ArrayList<>();
                                for (int i = 0; i < values.length; i++) {
                                    if (!headers[i].equalsIgnoreCase(columnToAlter)) {
                                        newValues.add(values[i]);
                                    }
                                }
                                lines.add(String.join("\t", newValues));
                            }
                        }
                        try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile))) {
                            for (String line : lines){
                                writer.write(line);
                                writer.newLine();
                            }
                            return "[OK] Column '" + columnToAlter + "' dropped from '" + tableName + "'.";
                        }
                    } catch (IOException e) {
                        return "[ERROR] Failed to drop column " + columnToAlter + " from table " + tableObject;
                    }
                }
                return "[ERROR] Invalid ALTER TABLE command";
            }

            //INSERT INTO table_name VALUES (x, y, z)":
            if (keyword.equals("INSERT")) {
                if (!formattedCommand.endsWith(";")){
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                if (parts.length < 5 || !parts[1].equalsIgnoreCase("INTO") || !parts[3].equalsIgnoreCase("VALUES")) {
                    return "[ERROR] Invalid INSERT command. Expected format: INSERT INTO table_name VALUES (x, y, z);";
                }
                String tableName = parts[2].toLowerCase();
                if (RESERVED_KEYWORDS.contains(tableName)) {
                    return "[ERROR] Reserved SQL keyword used as a table name";
                }
                if (activeDatabase == null) {
                    return "[ERROR] No database selected. Use 'Use database_name' first.";
                }

                int valuesIndex = formattedCommand.toUpperCase().indexOf("VALUES");
                int startIndex = formattedCommand.indexOf("(", valuesIndex);
                int endIndex = formattedCommand.indexOf(");", startIndex);

                if (valuesIndex == -1 || startIndex ==-1 || endIndex ==-1) {
                    return "[ERROR] Invalid syntax for INSERT. Missing column definitions.";
                }

                String valuesPart = formattedCommand.substring(startIndex + 1, endIndex).trim();
                Pattern pattern = Pattern.compile("'([^']*)'|\\d+\\.\\d+|\\d+|TRUE|FALSE|NULL|[^,]+");
                Matcher matcher = pattern.matcher(valuesPart);

                ArrayList<String> newRow = new ArrayList<>();
                while (matcher.find()) {
                    String value = matcher.group().trim();
                    if (value.startsWith("'") && value.endsWith("'")){
                        value = value.substring(1, value.length()-1);
                    }
                    if (value.equalsIgnoreCase("NULL")) {
                        value = "";
                    }
                    newRow.add(value.equalsIgnoreCase("NULL") ? "" : value);
                }

                File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + tableName + ".tab");
                if (!tableFile.exists() || !tableFile.isFile()) {
                    return "[ERROR] Table does not exist in this database.";
                }
                int lastRowId = 0;
                boolean hasIdColumn = false;
                int columnCount = 0;

                try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                    String headerLine = reader.readLine();
                    if (headerLine == null) {
                        return "[ERROR] Table is empty or has no header row";
                    }
                    String[] headers = headerLine.split("\t");
                    columnCount = headers.length;
                    hasIdColumn = Arrays.stream(headers).anyMatch(h -> h.trim().equalsIgnoreCase("id"));

                    if (hasIdColumn) {
                        String line;
                        String lastLine = null;
                        while ((line = reader.readLine()) != null) {
                            lastLine = line;
                        }
                        if (lastLine != null) {
                            String[] columns = lastLine.split("\t");
                            try {
                                lastRowId = Integer.parseInt(columns[0]);
                            } catch (NumberFormatException e) {
                                lastRowId = 0;
                            }
                        }
                    }
                } catch (IOException e) {
                    return "[ERROR] Failed to read table File";
                }

                int newRowId = hasIdColumn ? lastRowId + 1 : -1;
                int expectedValuesCount = hasIdColumn ? columnCount - 1 : columnCount;

                if (newRow.size() != expectedValuesCount) {
                    return "[ERROR] Mismatched column count. Expected " + expectedValuesCount + " but got " + newRow.size();
                }
                if (hasIdColumn) {
                    newRow.add(0, String.valueOf(newRowId));
                }
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, true))) {
                    writer.write(String.join("\t", newRow));
                    writer.newLine();
                } catch (IOException e) {
                    return "[ERROR] Failed to insert data into the table";
                }
                return hasIdColumn ? "[OK] Row inserted successfully with ID: " + newRowId : "[OK] Row inserted successfully.";
            }

            //DELETE FROM table_name WHERE (condition)
            if (keyword.equals("DELETE")) {
                if (parts.length < 5) {
                    return "[ERROR] Invalid DELETE command. Expected format 'DELETE FROM table_name WHERE (condition);";
                }
                if (!command.trim().endsWith(";")){
                    return "[ERROR] Missing semicolon at the end of the command.";
                }
                String tableName = parts[2].toLowerCase();
                if (RESERVED_KEYWORDS.contains(tableName)) {
                    return "[ERROR] Reserved SQL keyword used as a name";
                }
                String specifierFrom = parts[1].toUpperCase();
                String specifierWhere = parts[3].toUpperCase();
                if (!specifierFrom.equals("FROM") || !specifierWhere.equals("WHERE")) {
                    return "[ERROR] Invalid DELETE command. Expected format: DELETE FROM table_name WHERE (condition);";
                }
                if (formattedCommand.contains(" = ")){
                    return "[ERROR] = is not valid for comparing";
                }
                if (activeDatabase == null) {
                    return "[ERROR] No database selected. Use 'Use database_name' first.";
                }
                int whereIndex = formattedCommand.toUpperCase().indexOf("WHERE");
                if (whereIndex == -1) {
                    return "[ERROR] Missing WHERE clause in DELETE command.";
                }
                String conditionStatement = formattedCommand.substring(whereIndex + 5).trim();
                if (conditionStatement.endsWith(";")) {
                    conditionStatement = conditionStatement.substring(0, conditionStatement.length() - 1);
                }
                if (conditionStatement.startsWith("(") && conditionStatement.endsWith(")")) {
                    conditionStatement = conditionStatement.substring(1, conditionStatement.length() - 1);
                }
                File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + tableName + ".tab");
                if (!tableFile.exists() || !tableFile.isFile()) {
                    return "[ERROR] Table does not exist in this database";
                }

                List<String> conditionTokens = tokenizeCondition(conditionStatement);
                currentTokenIndex = 0;
                edu.uob.DBServer.ConditionNode conditionTree = parseCondition(conditionTokens);

                List<String> linesToKeep = new ArrayList<>();
                try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                    String headerLine = reader.readLine();
                    if (headerLine == null) {
                        return "[ERROR] Header line is empty.";
                    }
                    linesToKeep.add(headerLine);
                    String[] columns = headerLine.split("\t");

                    String row;
                    while ((row = reader.readLine()) != null) {
                        String[] values = row.split("\t");
                        if (values.length != columns.length) {
                            values = row.split("\\|");
                        }
                        Map<String, String> rowMap = new HashMap<>();
                        for (int i = 0; i < columns.length; i++) {
                            rowMap.put(columns[i].trim(), values[i].trim());
                        }
                        if (!conditionTree.evaluate(rowMap)) {
                            linesToKeep.add(row);
                        }
                    }
                } catch (IOException e) {
                    return "[ERROR] Error reading the table file.";
                }
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(tableFile, false))) {
                    for (String line : linesToKeep) {
                        writer.write(line);
                        writer.newLine();
                    }
                } catch (IOException e) {
                    return "[ERROR] Error writing to the table file";
                }
                return "[OK] Records deleted successfully";
            }

            //SELECT attributes FROM table_name WHERE (condition);
            if (keyword.equals("SELECT")) {
                if (!command.trim().endsWith(";")) {
                    return "[ERROR] Missing semicolon at the end of the command.";
                }

                String commandWithoutSemicolon = formattedCommand.trim().substring(0, command.length() - 1).trim();
                Pattern selectPattern = Pattern.compile("SELECT\\s+(.*?)\\s+FROM\\s+(\\w+)(?:\\s+WHERE\\s+(.*))?", Pattern.CASE_INSENSITIVE);
                Matcher matcher = selectPattern.matcher(commandWithoutSemicolon);

                if (!matcher.matches()) {
                    return "[ERROR] Invalid SELECT command. Expected format: SELECT attributes FROM table_name WHERE (condition);";
                }

                String attributesPart = matcher.group(1).trim();
                String tableName = matcher.group(2).toLowerCase();
                String whereClause = matcher.group(3) != null ? matcher.group(3).trim() : null;

                if (activeDatabase == null) {
                    return "[ERROR] No database selected. Use 'Use database_name' first.";
                }
                File tableFile = new File(storageFolderPath + File.separator + activeDatabase + File.separator + tableName + ".tab");
                if (!tableFile.exists() || !tableFile.isFile()) {
                    return "[ERROR] Table does not exist in this database";
                }

                List<String> tableData = new ArrayList<>();
                try (BufferedReader reader = new BufferedReader(new FileReader(tableFile))) {
                    String line;
                    while ((line = reader.readLine()) != null) {
                        tableData.add(line);
                    }
                } catch (IOException e) {
                    return "[ERROR] Error reading the table file.";
                }
                if (tableData.isEmpty()) {
                    return "[OK] Table is empty";
                }

                String headerLine = tableData.get(0);
                String[] headers = headerLine.split("\t");
                List<String> attributesToShow;

                if (attributesPart.equals("*")) {
                    attributesToShow = Arrays.asList(headers);
                } else {
                    attributesToShow = Arrays.stream(attributesPart.split(","))
                            .map(String::trim)
                            .collect(Collectors.toList());
                    for (String attr : attributesToShow) {
                        boolean columnExists = Arrays.stream(headers)
                                .anyMatch(header -> header.trim().equalsIgnoreCase(attr));
                        if (!columnExists) {
                            return "[ERROR] Attribute '" + attr + "' does not exist";
                        }
                    }
                }
                List<String> rowsToShow = new ArrayList<>();
                rowsToShow.add(String.join(", ", attributesToShow));

                for (int i = 1; i < tableData.size(); i++) {
                    String rowLine = tableData.get(i);
                    String[] rowValues = rowLine.split("\t");
                    Map<String, String> rowMap = new HashMap<>();
                    for (int c = 0; c < headers.length; c++) {
                        rowMap.put(headers[c].trim(), rowValues[c].trim());
                    }
                    boolean skipRow = false;
                    if (whereClause != null) {
                        String[] conditionParts = whereClause.split("==");
                        if (conditionParts.length != 2) {
                            return "[ERROR] Invalid WHERE condition.";
                        }
                        String whereAttr = conditionParts[0].trim();
                        String whereValue = conditionParts[1].trim().replaceAll("'", "");

                        if (!rowMap.containsKey(whereAttr)) {
                            return "[ERROR] Attribute '" + whereAttr + "' not found in table.";
                        }
                        String actualValue = rowMap.getOrDefault(whereAttr, "NULL");
                        if (actualValue.equalsIgnoreCase("NULL")) {
                            actualValue = "NULL";
                        }
                        boolean conditionMatched = actualValue.equals(whereValue);
                        skipRow = !conditionMatched;
                    }
                    if (!skipRow) {
                        List<String> rowOutput = new ArrayList<>();
                        for (String attr : attributesToShow) {
                            String value = rowMap.get(attr);
                            if (value == null || value.equalsIgnoreCase("NULL")) {
                                value = "NULL";
                            }
                            rowOutput.add(value);
                        }
                        rowsToShow.add(String.join(", ", rowOutput));
                    }
                }
                boolean isSelectedIDOnly = attributesToShow.size() == 1 && attributesToShow.get(0).equalsIgnoreCase("id");
                if (rowsToShow.size() == 1 && isSelectedIDOnly){
                    return "[ERROR] No matching record found.";
                }
                return String.join("\n", rowsToShow) + (isSelectedIDOnly ? "" :  "\n[OK]");
            }
        }
        catch (Exception e) {
            e.printStackTrace();
            return "[ERROR] Server encountered an unexpected issue" + e.getMessage();
        }
        return "[ERROR] Command failed.";
    }


    //  === Methods below handle networking aspects of the project - you will not need to change these ! ===

    public void blockingListenOn(int portNumber) throws IOException {
        try (ServerSocket s = new ServerSocket(portNumber)) {
            System.out.println("Server listening on port " + portNumber);
            while (!Thread.interrupted()) {
                try {
                    blockingHandleConnection(s);
                } catch (IOException e) {
                    System.err.println("Server encountered a non-fatal IO error:");
                    e.printStackTrace();
                    System.err.println("Continuing...");
                }
            }
        }
    }

    private void blockingHandleConnection(ServerSocket serverSocket) throws IOException {
        try (Socket s = serverSocket.accept();
             BufferedReader reader = new BufferedReader(new InputStreamReader(s.getInputStream()));
             BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(s.getOutputStream()))) {

            System.out.println("Connection established: " + serverSocket.getInetAddress());
            while (!Thread.interrupted()) {
                String incomingCommand = reader.readLine();
                System.out.println("Received message: " + incomingCommand);
                String result = handleCommand(incomingCommand);
                writer.write(result);
                writer.write("\n" + END_OF_TRANSMISSION + "\n");
                writer.flush();
            }
        }
    }

    String query = "  INSERT  INTO  people   VALUES(  'Simon Lock'  ,35, 'simon@bristol.ac.uk' , 1.8  ) ; ";
    String[] specialCharacters = {"(",")",",",";"};
    ArrayList<String> tokens = new ArrayList<String>();

    void setup()
    {
        // Split the query on single quotes (to separate out query text from string literals)
        String[] fragments = query.split("'");
        for (int i=0; i<fragments.length; i++) {
            // Every other fragment is a string literal, so just add it straight to "result" token list
            if (i%2 != 0) tokens.add("'" + fragments[i] + "'");
                // If it's not a string literal, it must be query text (which needs further processing)
            else {
                // Tokenise the fragment into an array of strings - this is the "clever" bit !
                String[] nextBatchOfTokens = tokenise(fragments[i]);
                // Then copy all the tokens into the "result" list (needs a bit of conversion)
                tokens.addAll(Arrays.asList(nextBatchOfTokens));
            }
        }
        // Finally, loop through the result array list, printing out each token a line at a time
        for (int i=0; i<tokens.size(); i++) System.out.println(tokens.get(i));
    }

    String[] tokenise(String input)
    {
        // Add in some extra padding spaces either side of the "special characters"...
        // so we can be SURE that they are separated by AT LEAST one space (possibly more)
        for (int i=0; i<specialCharacters.length ;i++) {
            input = input.replace(specialCharacters[i], " " + specialCharacters[i] + " ");
        }
        // Remove any double spaces (the previous padding activity might have introduced some of these)
        while (input.contains("  ")) input = input.replace("  ", " "); // Replace two spaces by one
        // Remove any whitespace from the beginning and the end that might have been introduced
        input = input.trim();
        // Finally split on the space char (since there will now ALWAYS be a SINGLE space between tokens)
        return input.split(" ");
    }
}

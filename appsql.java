import java.io.*;
import java.math.BigDecimal;
import java.sql.*;
import java.util.*;

public class appsql {
    private static final Scanner scanner = new Scanner(System.in);

    public static void main(String[] args) {
        String jdbcURL = "jdbc:mysql://localhost:3306/testdb";
        String username = "root";
        String password = "lab1";

        try (Connection conn = DriverManager.getConnection(jdbcURL, username, password)) {
            while (true) {
                System.out.println("\nMenu:");
                System.out.println("1. Create Table");
                System.out.println("2. Insert Data (Manual)");
                System.out.println("3. Insert Data (From CSV File)");
                System.out.println("4. Update Data (Manual)");
                System.out.println("5. Update Data (From CSV File)");
                System.out.println("6. Delete Record (Manual)");
                System.out.println("7. Delete Record (From File)");
                System.out.println("8. Clear All Data (Truncate)");
                System.out.println("9. Delete Entire Table");
                System.out.println("10. View Records (Paginated)");
                System.out.println("11. Exit");
                System.out.print("Enter your choice: ");
                String choice = scanner.nextLine();

                switch (choice) {
                    case "1": createTable(conn); break;
                    case "2": insertManual(conn); break;
                    case "3": insertFromFile(conn); break;
                    case "4": updateManual(conn); break;
                    case "5": updateFromFile(conn); break;
                    case "6": deleteManual(conn); break;
                    case "7": deleteFromFile(conn); break;
                    case "8": truncateTable(conn); break;
                    case "9": deleteTable(conn); break;
                    case "10": selectRecords(conn); break;
                    case "11": System.out.println("Exiting..."); return;
                    default: System.out.println("Invalid choice.");
                }
            }
        } catch (SQLException e) {
            System.out.println("Database Error: " + e.getMessage());
        }
    }

    private static void createTable(Connection conn) throws SQLException {
        System.out.print("Enter table name: ");
        String table = scanner.nextLine();
        System.out.print("Enter number of columns: ");
        int colCount = Integer.parseInt(scanner.nextLine());
        StringBuilder sb = new StringBuilder("CREATE TABLE ").append(table).append(" (");

        for (int i = 0; i < colCount; i++) {
            System.out.print("Enter column " + (i + 1) + " name: ");
            String colName = scanner.nextLine();
            System.out.print("Enter column " + (i + 1) + " type (e.g., VARCHAR(100), INT, DATE): ");
            String colType = scanner.nextLine();
            sb.append(colName).append(" ").append(colType);
            if (i != colCount - 1) sb.append(", ");
        }
        sb.append(")");

        try (Statement stmt = conn.createStatement()) {
            stmt.execute(sb.toString());
            System.out.println("Table created successfully.");
        }
    }

    private static List<ColumnInfo> getTableColumns(Connection conn, String table) {
        List<ColumnInfo> list = new ArrayList<>();
        String sql = "SELECT COLUMN_NAME, DATA_TYPE FROM INFORMATION_SCHEMA.COLUMNS " +
                     "WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = ?";
        try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
            pstmt.setString(1, table);
            ResultSet rs = pstmt.executeQuery();
            while (rs.next()) {
                list.add(new ColumnInfo(rs.getString("COLUMN_NAME"), rs.getString("DATA_TYPE")));
            }
        } catch (SQLException e) {
            System.out.println("Error fetching column metadata: " + e.getMessage());
        }
        return list;
    }

    private static void insertManual(Connection conn) throws SQLException {
        System.out.print("Enter table name: ");
        String table = scanner.nextLine();
        List<ColumnInfo> columns = getTableColumns(conn, table);
        if (columns.isEmpty()) {
            System.out.println("Invalid table or no columns found.");
            return;
        }

        StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
        for (ColumnInfo col : columns) sql.append(col.name).append(", ");
        sql.setLength(sql.length() - 2);
        sql.append(") VALUES (").append("?,".repeat(columns.size()));
        sql.setLength(sql.length() - 1);
        sql.append(")");

        try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
            for (int i = 0; i < columns.size(); i++) {
                ColumnInfo col = columns.get(i);
                while (true) {
                    System.out.print("Enter value for " + col.name + " (" + col.type + "): ");
                    String input = scanner.nextLine().trim();
                    if (input.equalsIgnoreCase("null") || input.isEmpty()) {
                        pstmt.setNull(i + 1, java.sql.Types.NULL);
                        break;
                    }
                    try {
                        switch (col.type.toLowerCase()) {
                            case "int": case "tinyint": case "smallint":
                            case "mediumint": case "bigint":
                                pstmt.setLong(i + 1, Long.parseLong(input)); break;
                            case "decimal": case "numeric": case "float": case "double":
                                pstmt.setBigDecimal(i + 1, new BigDecimal(input)); break;
                            case "date":
                                pstmt.setDate(i + 1, java.sql.Date.valueOf(input)); break;
                            case "datetime": case "timestamp":
                                pstmt.setTimestamp(i + 1, Timestamp.valueOf(input)); break;
                            default:
                                pstmt.setString(i + 1, input);
                        }
                        break;
                    } catch (Exception e) {
                        System.out.println("Invalid input for type " + col.type + ". Try again.");
                    }
                }
            }
            pstmt.executeUpdate();
            System.out.println("Record inserted.");
        }
    }

    private static void insertFromFile(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter CSV file path: ");
            String filePath = scanner.nextLine();
            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String[] headers = br.readLine().split(",");
                StringBuilder sql = new StringBuilder("INSERT INTO ").append(table).append(" (");
                for (String h : headers) sql.append(h).append(", ");
                sql.setLength(sql.length() - 2);
                sql.append(") VALUES (").append("?,".repeat(headers.length));
                sql.setLength(sql.length() - 1);
                sql.append(")");

                try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        for (int i = 0; i < values.length; i++) {
                            pstmt.setString(i + 1, values[i].trim());
                        }
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                    System.out.println("CSV data inserted successfully.");
                }
            }
        } catch (Exception e) {
            System.out.println("Error inserting from file: " + e.getMessage());
        }
    }

    private static void updateManual(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter column to update: ");
            String col = scanner.nextLine();
            System.out.print("Enter new value: ");
            String newVal = scanner.nextLine();
            System.out.print("Enter condition (e.g., id=1): ");
            String cond = scanner.nextLine();

            String sql = "UPDATE " + table + " SET " + col + " = ? WHERE " + cond;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                pstmt.setString(1, newVal);
                int updated = pstmt.executeUpdate();
                System.out.println(updated + " record(s) updated.");
            }
        } catch (SQLException e) {
            System.out.println("Update error: " + e.getMessage());
        }
    }

    private static void updateFromFile(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter CSV file path: ");
            String filePath = scanner.nextLine();
            System.out.print("Enter condition column name (e.g., id): ");
            String conditionCol = scanner.nextLine();

            try (BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String[] headers = br.readLine().split(",");
                List<String> updateCols = new ArrayList<>(Arrays.asList(headers));
                updateCols.remove(conditionCol);

                StringBuilder sql = new StringBuilder("UPDATE ").append(table).append(" SET ");
                for (String col : updateCols) sql.append(col).append(" = ?, ");
                sql.setLength(sql.length() - 2);
                sql.append(" WHERE ").append(conditionCol).append(" = ?");

                try (PreparedStatement pstmt = conn.prepareStatement(sql.toString())) {
                    String line;
                    while ((line = br.readLine()) != null) {
                        String[] values = line.split(",");
                        for (int i = 0; i < updateCols.size(); i++) {
                            pstmt.setString(i + 1, values[i + 1].trim());
                        }
                        pstmt.setString(updateCols.size() + 1, values[0].trim());
                        pstmt.addBatch();
                    }
                    pstmt.executeBatch();
                    System.out.println("Records updated from file.");
                }
            }
        } catch (Exception e) {
            System.out.println("Update from file error: " + e.getMessage());
        }
    }

    private static void deleteManual(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter condition (e.g., id=1): ");
            String cond = scanner.nextLine();
            String sql = "DELETE FROM " + table + " WHERE " + cond;
            try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                int deleted = pstmt.executeUpdate();
                System.out.println(deleted + " record(s) deleted.");
            }
        } catch (SQLException e) {
            System.out.println("Delete error: " + e.getMessage());
        }
    }

    private static void deleteFromFile(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter CSV file path with keys to delete: ");
            String filePath = scanner.nextLine();
            System.out.print("Enter condition column name (e.g., id): ");
            String condCol = scanner.nextLine();

            String sql = "DELETE FROM " + table + " WHERE " + condCol + " = ?";
            try (PreparedStatement pstmt = conn.prepareStatement(sql);
                 BufferedReader br = new BufferedReader(new FileReader(filePath))) {
                String line;
                while ((line = br.readLine()) != null) {
                    pstmt.setString(1, line.trim());
                    pstmt.addBatch();
                }
                pstmt.executeBatch();
                System.out.println("Records deleted from file.");
            }
        } catch (Exception e) {
            System.out.println("Delete from file error: " + e.getMessage());
        }
    }

    private static void truncateTable(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("TRUNCATE TABLE " + table);
                System.out.println("Table truncated.");
            }
        } catch (SQLException e) {
            System.out.println("Truncate error: " + e.getMessage());
        }
    }

    private static void deleteTable(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            try (Statement stmt = conn.createStatement()) {
                stmt.executeUpdate("DROP TABLE " + table);
                System.out.println("Table deleted.");
            }
        } catch (SQLException e) {
            System.out.println("Delete table error: " + e.getMessage());
        }
    }

    private static void selectRecords(Connection conn) {
        try {
            System.out.print("Enter table name: ");
            String table = scanner.nextLine();
            System.out.print("Enter page size: ");
            int pageSize = Integer.parseInt(scanner.nextLine());
            int offset = 0;
            while (true) {
                String sql = "SELECT * FROM " + table + " LIMIT ? OFFSET ?";
                try (PreparedStatement pstmt = conn.prepareStatement(sql)) {
                    pstmt.setInt(1, pageSize);
                    pstmt.setInt(2, offset);
                    ResultSet rs = pstmt.executeQuery();
                    ResultSetMetaData meta = rs.getMetaData();
                    int colCount = meta.getColumnCount();
                    boolean hasData = false;
                    while (rs.next()) {
                        hasData = true;
                        for (int i = 1; i <= colCount; i++) {
                            System.out.print(meta.getColumnName(i) + ": " + rs.getString(i) + " | ");
                        }
                        System.out.println();
                    }
                    if (!hasData) {
                        System.out.println("No more records.");
                        break;
                    }
                    System.out.print("Show next page? (y/n): ");
                    String next = scanner.nextLine();
                    if (!next.equalsIgnoreCase("y")) break;
                    offset += pageSize;
                }
            }
        } catch (Exception e) {
            System.out.println("Error viewing records: " + e.getMessage());
        }
    }
}

class ColumnInfo {
    String name;
    String type;

    ColumnInfo(String name, String type) {
        this.name = name;
        this.type = type;
    }
}
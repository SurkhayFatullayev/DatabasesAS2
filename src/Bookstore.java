import java.sql.*;

public class Bookstore {

    public static void main(String[] args) {
        try (Connection connection = DatabaseConnection.getConnection()) {
            

            // Insert
            insertAuthor(connection, "John Doe");
            
            insertBook(connection, "Sample Book", 3, 30);

            insertCustomer(connection, "Surkhay Fatullayev");

            // CRUD operations
            retrieveBooks(connection);
            updateBook(connection, 1, "Dead Space: Catalyst", 15);
            removeBook(connection, 2);

            // Transaction
            placeOrder(connection, 1, 3, 2); 

            // Metadata
            displayTableInfo(connection);
            displayColumnInfo(connection);
            displayKeyInfo(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }


    

    private static void insertAuthor(Connection connection, String authorName) throws SQLException {
        String insertAuthorQuery = "INSERT INTO Authors (author_name) VALUES (?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertAuthorQuery)) {
            preparedStatement.setString(1, authorName);
            preparedStatement.executeUpdate();
        }
    }

    private static void insertBook(Connection connection, String title, int authorId, int stockQuantity) throws SQLException {
        String insertBookQuery = "INSERT INTO Books (title, author_id, stock_quantity) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertBookQuery)) {
            preparedStatement.setString(1, title);
            preparedStatement.setInt(2, authorId);
            preparedStatement.setInt(3, stockQuantity);
            preparedStatement.executeUpdate();
        }
    }

    private static void insertCustomer(Connection connection, String customerName) throws SQLException {
        String insertCustomerQuery = "INSERT INTO Customers (customer_name) VALUES (?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertCustomerQuery)) {
            preparedStatement.setString(1, customerName);
            preparedStatement.executeUpdate();
        }
    }


    private static void retrieveBooks(Connection connection) throws SQLException {
        String retrieveBooksQuery = "SELECT b.book_id, b.title, a.author_name, b.stock_quantity " +
                "FROM Books b JOIN Authors a ON b.author_id = a.author_id";
        try (PreparedStatement preparedStatement = connection.prepareStatement(retrieveBooksQuery)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Book ID: " + resultSet.getInt("book_id") +
                        ", Title: " + resultSet.getString("title") +
                        ", Author: " + resultSet.getString("author_name") +
                        ", Stock Quantity: " + resultSet.getInt("stock_quantity"));
            }
        }
    }

    private static void updateBook(Connection connection, int bookId, String newTitle, int newStockQuantity) throws SQLException {
        String updateBookQuery = "UPDATE Books SET title=?, stock_quantity=? WHERE book_id=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(updateBookQuery)) {
            preparedStatement.setString(1, newTitle);
            preparedStatement.setInt(2, newStockQuantity);
            preparedStatement.setInt(3, bookId);
            preparedStatement.executeUpdate();
        }
    }

    private static void removeBook(Connection connection, int bookId) throws SQLException {
        String removeBookQuery = "DELETE FROM Books WHERE book_id=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(removeBookQuery)) {
            preparedStatement.setInt(1, bookId);
            preparedStatement.executeUpdate();
        }
    }

      private static void placeOrder(Connection connection, int customerId, int bookId, int quantity) throws SQLException {
        connection.setAutoCommit(false);

        String checkStockQuery = "SELECT stock_quantity FROM Books WHERE book_id=?";
        try (PreparedStatement checkStockStatement = connection.prepareStatement(checkStockQuery)) {
            checkStockStatement.setInt(1, bookId);
            ResultSet resultSet = checkStockStatement.executeQuery();
            if (resultSet.next()) {
                int stockQuantity = resultSet.getInt("stock_quantity");
                if (stockQuantity >= quantity) {
                    
                    String insertOrderQuery = "INSERT INTO Orders (customer_id, book_id, quantity) VALUES (?, ?, ?)";
                    try (PreparedStatement insertOrderStatement = connection.prepareStatement(insertOrderQuery)) {
                        insertOrderStatement.setInt(1, customerId);
                        insertOrderStatement.setInt(2, bookId);
                        insertOrderStatement.setInt(3, quantity);
                        insertOrderStatement.executeUpdate();
                    }

                    
                    String updateStockQuery = "UPDATE Books SET stock_quantity=? WHERE book_id=?";
                    try (PreparedStatement updateStockStatement = connection.prepareStatement(updateStockQuery)) {
                        updateStockStatement.setInt(1, stockQuantity - quantity);
                        updateStockStatement.setInt(2, bookId);
                        updateStockStatement.executeUpdate();
                    }

                    connection.commit();
                    System.out.println("Order placed successfully.");
                } else {
                    System.out.println("Not enough stock to place the order.");
                }
            }
        } catch (SQLException e) {
            connection.rollback();
            e.printStackTrace();
        } finally {
            connection.setAutoCommit(true);
        }
    }

    private static void displayTableInfo(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet tables = metaData.getTables(null, null, "%", new String[]{"TABLE"});
    
        System.out.println("User Table Names:");
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
            System.out.println(" - " + tableName);
        }
    }
    

    private static void displayColumnInfo(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, "books", "%");
    
        System.out.println("Columns of User Table (books):");
        while (columns.next()) {
            String columnName = columns.getString("COLUMN_NAME");
            String dataType = columns.getString("TYPE_NAME");
            System.out.println(" - Column Name: " + columnName + ", Data Type: " + dataType);
        }
    }
    

    private static void displayKeyInfo(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
    
        ResultSet tables = metaData.getTables(null, null, "%",  new String[]{"TABLE"});
        while (tables.next()) {
            String tableName = tables.getString("TABLE_NAME");
    
            if (!tableName.startsWith("pg_")) {
                System.out.println("Keys for Table (" + tableName + "):");
    
                ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, tableName);
                while (primaryKeys.next()) {
                    String primaryKeyColumn = primaryKeys.getString("COLUMN_NAME");
                    System.out.println(" - Primary Key: " + primaryKeyColumn);
                }
    
                ResultSet foreignKeys = metaData.getImportedKeys(null, null, tableName);
                while (foreignKeys.next()) {
                    String foreignKeyColumn = foreignKeys.getString("FKCOLUMN_NAME");
                    String referencedTable = foreignKeys.getString("PKTABLE_NAME");
                    String referencedColumnName = foreignKeys.getString("PKCOLUMN_NAME");
                    System.out.println(" - Foreign Key: " + foreignKeyColumn +
                            " references " + referencedTable +
                            "(" + referencedColumnName + ")");
                }
    
                System.out.println();
            }
        }
    }
    
}
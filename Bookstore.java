import java.sql.*;

public class Bookstore {

    public static void main(String[] args) {
        try (Connection connection = DatabaseConnection.getConnection()) {
            insertBook(connection, "The Great Gatsby", "F. Scott Fitzgerald", 50);

            retrieveBooks(connection);

            updateBook(connection, 1, "The Great Gatsby", "F. Scott Fitzgerald", 45);

            removeBook(connection, 2);

            placeOrder(connection, 1, 3);

            // Metadata Access
            displayTableInfo(connection);
            displayColumnInfo(connection);
            displayKeyInfo(connection);

        } catch (SQLException e) {
            e.printStackTrace();
        }
    }

    private static void insertBook(Connection connection, String title, String author, int stockQuantity) throws SQLException {
        String insertBookQuery = "INSERT INTO Books (title, author, stock_quantity) VALUES (?, ?, ?)";
        try (PreparedStatement preparedStatement = connection.prepareStatement(insertBookQuery)) {
            preparedStatement.setString(1, title);
            preparedStatement.setString(2, author);
            preparedStatement.setInt(3, stockQuantity);
            preparedStatement.executeUpdate();
        }
    }

    private static void retrieveBooks(Connection connection) throws SQLException {
        String retrieveBooksQuery = "SELECT * FROM Books";
        try (PreparedStatement preparedStatement = connection.prepareStatement(retrieveBooksQuery)) {
            ResultSet resultSet = preparedStatement.executeQuery();
            while (resultSet.next()) {
                System.out.println("Book ID: " + resultSet.getInt("book_id") +
                        ", Title: " + resultSet.getString("title") +
                        ", Author: " + resultSet.getString("author") +
                        ", Stock Quantity: " + resultSet.getInt("stock_quantity"));
            }
        }
    }

    private static void updateBook(Connection connection, int bookId, String newTitle, String newAuthor, int newStockQuantity) throws SQLException {
        String updateBookQuery = "UPDATE Books SET title=?, author=?, stock_quantity=? WHERE book_id=?";
        try (PreparedStatement preparedStatement = connection.prepareStatement(updateBookQuery)) {
            preparedStatement.setString(1, newTitle);
            preparedStatement.setString(2, newAuthor);
            preparedStatement.setInt(3, newStockQuantity);
            preparedStatement.setInt(4, bookId);
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

    private static void placeOrder(Connection connection, int customerId, int bookId) throws SQLException {
        connection.setAutoCommit(false);

        String checkStockQuery = "SELECT stock_quantity FROM Books WHERE book_id=?";
        try (PreparedStatement checkStockStatement = connection.prepareStatement(checkStockQuery)) {
            checkStockStatement.setInt(1, bookId);
            ResultSet resultSet = checkStockStatement.executeQuery();
            if (resultSet.next()) {
                int stockQuantity = resultSet.getInt("stock_quantity");
                if (stockQuantity > 0) {
                    String insertOrderQuery = "INSERT INTO Orders (customer_id, book_id, quantity) VALUES (?, ?, 1)";
                    try (PreparedStatement insertOrderStatement = connection.prepareStatement(insertOrderQuery)) {
                        insertOrderStatement.setInt(1, customerId);
                        insertOrderStatement.setInt(2, bookId);
                        insertOrderStatement.executeUpdate();
                    }

                    String updateStockQuery = "UPDATE Books SET stock_quantity=? WHERE book_id=?";
                    try (PreparedStatement updateStockStatement = connection.prepareStatement(updateStockQuery)) {
                        updateStockStatement.setInt(1, stockQuantity - 1);
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
        ResultSet tables = metaData.getTables(null, null, "%", null);
        while (tables.next()) {
            System.out.println("Table Name: " + tables.getString("TABLE_NAME"));
        }
    }

    private static void displayColumnInfo(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        ResultSet columns = metaData.getColumns(null, null, "Books", "%");
        while (columns.next()) {
            System.out.println("Column Name: " + columns.getString("COLUMN_NAME") +
                    ", Data Type: " + columns.getString("TYPE_NAME"));
        }
    }

        private static void displayKeyInfo(Connection connection) throws SQLException {
        DatabaseMetaData metaData = connection.getMetaData();
        
        ResultSet primaryKeys = metaData.getPrimaryKeys(null, null, "Books");
        System.out.println("Primary Keys:");
        while (primaryKeys.next()) {
            System.out.println(" - " + primaryKeys.getString("COLUMN_NAME"));
        }
        
        ResultSet foreignKeys = metaData.getImportedKeys(null, null, "Books");
        System.out.println("Foreign Keys:");
        while (foreignKeys.next()) {
            System.out.println(" - " + foreignKeys.getString("FKCOLUMN_NAME") +
                    " references " + foreignKeys.getString("PKTABLE_NAME") +
                    "(" + foreignKeys.getString("PKCOLUMN_NAME") + ")");
        }
        
        if (primaryKeys != null) {
            primaryKeys.close();
        }
        if (foreignKeys != null) {
            foreignKeys.close();
        }
    }
}

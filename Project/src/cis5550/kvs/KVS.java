package cis5550.kvs;

// Iteration utilities
import java.util.Iterator;

// File handling
import java.io.FileNotFoundException;
import java.io.IOException;

public interface KVS {
  // stores a single value in the specified table, row, and column
  // throws exceptions if file-related issues occur
  void put(String tableName, String row, String column, byte value[]) throws FileNotFoundException, IOException;

  // stores an entire row in the specified table
  // throws exceptions if file-related issues occur
  void putRow(String tableName, Row row) throws FileNotFoundException, IOException;

  // retrieves a single row by its key
  // throws exceptions if the row or table doesn't exist
  Row getRow(String tableName, String row) throws FileNotFoundException, IOException;

  // checks if a row exists in the table
  // returns true if the row is found, false otherwise
  boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException;

  // retrieves the value in the specified table, row, and column
  // throws exceptions if the column or table doesn't exist
  byte[] get(String tableName, String row, String column) throws FileNotFoundException, IOException;

  // scans the table and iterates over rows in the specified range
  // startRow is inclusive, endRowExclusive is exclusive
  Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException;

  // counts the number of rows in the specified table
  // throws an exception if the table doesn't exist
  int count(String tableName) throws FileNotFoundException, IOException;

  // renames a table from oldTableName to newTableName
  // returns true if successful, false if the table doesn't exist
  boolean rename(String oldTableName, String newTableName) throws IOException;

  // deletes a table by its name
  // throws an exception if the table doesn't exist
  void delete(String oldTableName) throws IOException;
};

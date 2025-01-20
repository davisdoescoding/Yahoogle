package cis5550.kvs;

import java.util.*;

import cis5550.tools.Logger;

import java.io.*;

public class Row implements Serializable {
	
	private static final Logger log = Logger.getLogger(Row.class);

  // stores the key of the row
  protected String key;

  // stores the values as a map of column name to byte array
  protected HashMap<String, byte[]> values;

  // constructor to init row with a key
  public Row(String keyArg) {
    log.debug("creating row with key: " + keyArg);
    key = keyArg;
    values = new HashMap<>();
  }

  // gets the key of the row
  public synchronized String key() {
    return key;
  }

  // clones the row (deep copy)
  public synchronized Row clone() {
	  log.debug("cloning row with key: " + key);
    Row theClone = new Row(key);
    for (String s : values.keySet()) {
      theClone.values.put(s, values.get(s));
    }
    return theClone;
  }

  // gets the set of column names in the row
  public synchronized Set<String> columns() {
	  log.debug("retrieving columns for row: " + key);
    return values.keySet();
  }

  // puts a string value into the row at the specified column
  public synchronized void put(String key, String value) {
	  log.debug("putting value for column: " + key + " in row: " + this.key);
    values.put(key, value.getBytes());
  }

  // puts a byte array value into the row at the specified column
  public synchronized void put(String key, byte[] value) {
	  log.debug("putting byte[] value for column: " + key + " in row: " + this.key);
    values.put(key, value);
  }

  // gets a string value from the row at the specified column
  public synchronized String get(String key) {
	  log.debug("retrieving value for column: " + key + " in row: " + this.key);
    if (values.get(key) == null) {
      return null;
    }
    return new String(values.get(key));
  }

  // gets a byte array value from the row at the specified column
  public synchronized byte[] getBytes(String key) {
	  log.debug("retrieving byte[] value for column: " + key + " in row: " + this.key);
    return values.get(key);
  }

  // reads a string followed by a space from an input stream
  static String readStringSpace(InputStream in) throws Exception {
    byte buffer[] = new byte[16384];
    int numRead = 0;
    while (true) {
      if (numRead == buffer.length) {
        throw new Exception("format error: expecting string+space");
      }
      int b = in.read();
      if ((b < 0) || (b == 10)) {
        return null;
      }
      buffer[numRead++] = (byte) b;
      if (b == ' ') {
        return new String(buffer, 0, numRead - 1);
      }
    }
  }

  // reads a string followed by a space from a random access file
  static String readStringSpace(RandomAccessFile in) throws Exception {
    byte buffer[] = new byte[16384];
    int numRead = 0;
    while (true) {
      if (numRead == buffer.length) {
        throw new Exception("format error: expecting string+space");
      }
      int b = in.read();
      if ((b < 0) || (b == 10)) {
        return null;
      }
      buffer[numRead++] = (byte) b;
      if (b == ' ') {
        return new String(buffer, 0, numRead - 1);
      }
    }
  }

  // reads a row from an input stream
  public static Row readFrom(InputStream in) throws Exception {
	  log.debug("reading row from input stream...");
    String theKey = readStringSpace(in);
    if (theKey == null) {
    	log.debug("no key found, returning null");
      return null;
    }

    Row newRow = new Row(theKey);
    while (true) {
      String keyOrMarker = readStringSpace(in);
      if (keyOrMarker == null) {
        return newRow;
      }
      int len = Integer.parseInt(readStringSpace(in));
      byte[] theValue = new byte[len];
      int bytesRead = 0;
      while (bytesRead < len) {
        int n = in.read(theValue, bytesRead, len - bytesRead);
        if (n < 0) {
          throw new Exception("premature end of stream while reading value for key '" + keyOrMarker + "' (read " + bytesRead + " bytes, expecting " + len + ")");
        }
        bytesRead += n;
      }
      byte b = (byte) in.read();
      if (b != ' ') {
        throw new Exception("expecting a space separator after value for key '" + keyOrMarker + "'");
      }
      newRow.put(keyOrMarker, theValue);
    }
  }

  // converts the row to a string representation
  public synchronized String toString() {
	  log.debug("converting row to string: " + key);
    String s = key + " {";
    boolean isFirst = true;
    for (String k : values.keySet()) {
      s = s + (isFirst ? " " : ", ") + k + ": " + new String(values.get(k));
      isFirst = false;
    }
    return s + " }";
  }

  // converts the row to a byte array representation
  public synchronized byte[] toByteArray() {
	  log.debug("converting row to byte array: " + key);
    ByteArrayOutputStream baos = new ByteArrayOutputStream();

    try {
      baos.write(key.getBytes());
      baos.write(' ');

      for (String s : values.keySet()) {
        baos.write(s.getBytes());
        baos.write(' ');
        baos.write(("" + values.get(s).length).getBytes());
        baos.write(' ');
        baos.write(values.get(s));
        baos.write(' ');
      }
    } catch (Exception e) {
      e.printStackTrace();
      throw new RuntimeException("this should not happen!");
    }

    return baos.toByteArray();
  }
}

package cis5550.kvs;

// Collections and utilities
import java.util.*;

// Networking
import java.net.*;

// File handling
import java.io.*;

import cis5550.jobs.Indexer;
// HTTP tools
import cis5550.tools.HTTP;
import cis5550.tools.Logger;

public class KVSClient implements KVS {
	
	private static final Logger logger = Logger.getLogger(KVSClient.class);

  String coordinator;

  // class to represent a worker in the cluster
  static class WorkerEntry implements Comparable<WorkerEntry> {
    String address; // worker's address
    String id; // worker's unique ID

    // constructor to init worker details
    WorkerEntry(String addressArg, String idArg) {
      address = addressArg;
      id = idArg;
    }

    // compare workers by their IDs
    public int compareTo(WorkerEntry e) {
      return id.compareTo(e.id);
    }
  }

  // list of workers and a flag to track if workers are downloaded
  Vector<WorkerEntry> workers;
  boolean haveWorkers;

  // returns the number of workers; downloads if not available
  public int numWorkers() throws IOException {
    if (!haveWorkers) {
      logger.debug("downloading workers...");
      downloadWorkers();
    }
    logger.debug("number of workers: " + workers.size());
    return workers.size();
  }

  // returns the version of the client
  public static String getVersion() {
    return "v1.5 Oct 20 2023"; // hardcoded version string
  }

  // get the coordinator address
  public String getCoordinator() {
    return coordinator;
  }

  // get a worker's address by index
  public String getWorkerAddress(int idx) throws IOException {
    if (!haveWorkers) {
    	logger.debug("workers not downloaded, downloading now...");
      downloadWorkers();
    }
    logger.debug("returning worker address for index " + idx);
    return workers.elementAt(idx).address;
  }

  // get a worker's ID by index
  public String getWorkerID(int idx) throws IOException {
    if (!haveWorkers) {
    	logger.debug("workers not downloaded, downloading now...");
      downloadWorkers();
    }
    logger.debug("returning worker ID for index " + idx);
    return workers.elementAt(idx).id;
  }

  // iterator class to scan over rows from workers
  class KVSIterator implements Iterator<Row> {
    InputStream in; // input stream for reading data
    boolean atEnd; // flag to check if we're done
    Row nextRow; // next row to return
    int currentRangeIndex; // index of current range in ranges
    String endRowExclusive; // end row for scanning
    String startRow; // start row for scanning
    String tableName; // table being scanned
    Vector<String> ranges; // list of ranges to scan

    // constructor to init the iterator
    KVSIterator(String tableNameArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      in = null; // no input stream initially
      currentRangeIndex = 0; // start at first range
      atEnd = false; // not at the end
      endRowExclusive = endRowExclusiveArg; // set range limit
      tableName = tableNameArg; // set table name
      startRow = startRowArg; // set start row
      ranges = new Vector<>(); // init empty range list

      // if no start row or it’s before the first worker ID
      if ((startRowArg == null) || (startRowArg.compareTo(getWorkerID(0)) < 0)) {
        // add URL for the last worker
        String url = getURL(tableNameArg, numWorkers() - 1, startRowArg,
                ((endRowExclusiveArg != null) && (endRowExclusiveArg.compareTo(getWorkerID(0)) < 0))
                        ? endRowExclusiveArg
                        : getWorkerID(0));
        ranges.add(url);
      }

      // iterate over all workers to add ranges
      for (int i = 0; i < numWorkers(); i++) {
        if ((startRowArg == null) || (i == numWorkers() - 1) || (startRowArg.compareTo(getWorkerID(i + 1)) < 0)) {
          if ((endRowExclusiveArg == null) || (endRowExclusiveArg.compareTo(getWorkerID(i)) > 0)) {
            boolean useActualStartRow = (startRowArg != null) && (startRowArg.compareTo(getWorkerID(i)) > 0);
            boolean useActualEndRow = (endRowExclusiveArg != null) &&
                    ((i == (numWorkers() - 1)) || (endRowExclusiveArg.compareTo(getWorkerID(i + 1)) < 0));
            String url = getURL(tableNameArg, i, useActualStartRow ? startRowArg : getWorkerID(i),
                    useActualEndRow ? endRowExclusiveArg : ((i < numWorkers() - 1) ? getWorkerID(i + 1) : null));
            ranges.add(url);
          }
        }
      }

      logger.debug("ranges to scan: " + ranges);
      openConnectionAndFill(); // open connection to start scanning
    }

    // helper to construct the URL for a range
    protected String getURL(String tableNameArg, int workerIndexArg, String startRowArg, String endRowExclusiveArg) throws IOException {
      String params = ""; // init empty params string
      if (startRowArg != null)
        params = "startRow=" + startRowArg;
      if (endRowExclusiveArg != null)
        params = (params.equals("") ? "" : (params + "&")) + "endRowExclusive=" + endRowExclusiveArg;
      String url = "http://" + getWorkerAddress(workerIndexArg) + "/data/" + tableNameArg + (params.equals("") ? "" : "?" + params);
      logger.debug("generated URL for worker " + workerIndexArg + ": " + url);
      return url;
    }

    void openConnectionAndFill() {
      try {
        // close the current stream if open
        if (in != null) {
//          System.out.println("closing existing input stream...");
          in.close();
          in = null;
        }

        // stop if we're at the end
        if (atEnd) {
//          System.out.println("all ranges processed, ending...");
          return;
        }

        // loop through ranges to establish connections
        while (true) {
          // check if we've processed all ranges
          if (currentRangeIndex >= ranges.size()) {
//            System.out.println("no more ranges, marking end...");
            atEnd = true;
            return;
          }

          try {
            // get URL for the current range and establish connection
        	  logger.debug("connecting to range: " + ranges.elementAt(currentRangeIndex));
            URL url = new URI(ranges.elementAt(currentRangeIndex)).toURL();
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("GET");
            con.connect();

            // read from the stream
            in = con.getInputStream();
            Row r = fill(); // try to fetch the next row
            if (r != null) {
//              System.out.println("row fetched successfully...");
              nextRow = r;
              break;
            }
          } catch (FileNotFoundException fnfe) {
        	  logger.debug("file not found for current range, skipping...");
          } catch (URISyntaxException use) {
        	  logger.debug("invalid URL syntax for range, skipping...");
          }

          // move to the next range
          currentRangeIndex++;
        }
      } catch (IOException ioe) {
        System.out.println("io exception during range processing, ending...");
        if (in != null) {
          try {
            in.close();
          } catch (Exception e) {
            System.out.println("error closing input stream...");
          }
          in = null;
        }
        atEnd = true; // mark as done
      }
    }

    synchronized Row fill() {
      try {
        // try to read the next row from the input stream
        Row r = Row.readFrom(in);
//        System.out.println("row read successfully...");
        return r;
      } catch (Exception e) {
    	  logger.debug("error reading row, returning null...");
        return null; // no more rows to read
      }
    }

    public synchronized Row next() {
      // return null if we've reached the end
      if (atEnd) {
    	  logger.debug("no next row, at end...");
        return null;
      }

      // store the current row to return
      Row r = nextRow;
      nextRow = fill(); // try to fetch the next row
      while ((nextRow == null) && !atEnd) {
        currentRangeIndex++;
        openConnectionAndFill(); // move to the next range if no rows
      }

      return r;
    }

    public synchronized boolean hasNext() {
      // check if there's more data to process
      return !atEnd;
    }
  }

  synchronized void downloadWorkers() throws IOException {
	  logger.debug("downloading workers from coordinator...");
    String result = new String(HTTP.doRequest("GET", "http://" + coordinator + "/workers", null).body());
    String[] pieces = result.split("\n");
    int numWorkers = Integer.parseInt(pieces[0]);

    // check if we have any workers
    if (numWorkers < 1) {
    	logger.debug("no active workers found...");
      throw new IOException("No active KVS workers");
    }

    // validate response length
    if (pieces.length != (numWorkers + 1)) {
    	logger.error("unexpected response length from coordinator...");
      throw new RuntimeException("Received truncated response when asking KVS coordinator for list of workers");
    }

    // clear existing workers and add new ones
    workers.clear();
    for (int i = 0; i < numWorkers; i++) {
      String[] pcs = pieces[1 + i].split(",");
      workers.add(new WorkerEntry(pcs[1], pcs[0]));
    }

    // sort workers and mark them as downloaded
    Collections.sort(workers);
    haveWorkers = true;
    logger.debug("workers downloaded and sorted...");
  }

  int workerIndexForKey(String key) {
    // default to the last worker
    int chosenWorker = workers.size() - 1;

    // find the appropriate worker for the key
    if (key != null) {
      for (int i = 0; i < workers.size() - 1; i++) {
        if ((key.compareTo(workers.elementAt(i).id) >= 0) && (key.compareTo(workers.elementAt(i + 1).id) < 0)) {
          chosenWorker = i;
          break;
        }
      }
    }

    logger.debug("worker chosen for key: " + chosenWorker);
    return chosenWorker;
  }

  // constructor to initialize client
  public KVSClient(String coordinatorArg) {
	  logger.debug("initializing KVS client with coordinator: " + coordinatorArg);
    coordinator = coordinatorArg;
    workers = new Vector<>();
    haveWorkers = false;
  }

  public boolean rename(String oldTableName, String newTableName) throws IOException {
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    boolean result = true;
    // send rename request to all workers
    for (WorkerEntry w : workers) {
      try {
    	  System.out.println("renaming table on worker: " + w.address);
        byte[] response = HTTP.doRequest("PUT", "http://" + w.address + "/rename/" + java.net.URLEncoder.encode(oldTableName, "UTF-8") + "/", newTableName.getBytes()).body();
        String res = new String(response);
        result &= res.equals("OK");
      } catch (Exception e) {
    	  System.out.println("error during rename on worker: " + w.address);
      }
    }

    return result;
  }

  public void delete(String oldTableName) throws IOException {
    // check if workers list is empty, download if needed
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // loop through all workers and send delete request
    for (WorkerEntry w : workers) {
      try {
    	  logger.debug("sending delete request for table '" + oldTableName + "' to worker: " + w.address);
        byte[] response = HTTP.doRequest("PUT", "http://" + w.address + "/delete/" + java.net.URLEncoder.encode(oldTableName, "UTF-8") + "/", null).body();
        String result = new String(response);
        System.out.println("delete response: " + result);
      } catch (Exception e) {
        System.out.println("error deleting table '" + oldTableName + "' on worker: " + w.address);
      }
    }
  }

  public void put(String tableName, String row, String column, byte[] value) throws IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    try {
      // build target URL for the worker responsible for this row
      String target = "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8");
      logger.debug("sending PUT request to: " + target);
      byte[] response = HTTP.doRequest("PUT", target, value).body();
      String result = new String(response);

      // check response for success
      if (!result.equals("OK")) {
        System.out.println("error: PUT returned '" + result + "' for target: " + target);
        throw new RuntimeException("PUT returned something other than OK: " + result + " (" + target + ")");
      }
    } catch (UnsupportedEncodingException uee) {
      System.out.println("utf-8 encoding not supported for row/column values");
      throw new RuntimeException("UTF-8 encoding not supported?!?");
    }
  }

  public void put(String tableName, String row, String column, String value) throws IOException {
    // delegate string version of put to byte[] version
    put(tableName, row, column, value.getBytes());
  }

  public void putRow(String tableName, Row row) throws FileNotFoundException, IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // send putRow request to appropriate worker
    logger.debug("sending putRow request for row: " + row.key());
    byte[] response = HTTP.doRequest("PUT", "http://" + workers.elementAt(workerIndexForKey(row.key())).address + "/data/" + tableName, row.toByteArray()).body();
    String result = new String(response);

    // check response for success
    if (!result.equals("OK")) {
     logger.error("error: PUT row returned '" + result + "'");
      throw new RuntimeException("PUT returned something other than OK: " + result);
    }
  }

  public Row getRow(String tableName, String row) throws IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // send getRow request to the worker
    logger.debug("fetching row '" + row + "' from table: " + tableName);
    HTTP.Response resp = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);

    // handle 404 (not found)
    if (resp.statusCode() == 404) {
      logger.error("row not found: " + row);
      return null;
    }

    // parse response into a Row object
    byte[] result = resp.body();
    try {
      return Row.readFrom(new ByteArrayInputStream(result));
    } catch (Exception e) {
      logger.error("error decoding row: " + row);
      throw new RuntimeException("Decoding error while reading Row '" + row + "' in table '" + tableName + "'");
    }
  }

  public byte[] get(String tableName, String row, String column) throws IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // send get request to the worker
    logger.debug("fetching column '" + column + "' for row '" + row + "' from table: " + tableName);
    HTTP.Response res = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8") + "/" + java.net.URLEncoder.encode(column, "UTF-8"), null);

    // return result or null if not found
    return ((res != null) && (res.statusCode() == 200)) ? res.body() : null;
  }

  public boolean existsRow(String tableName, String row) throws FileNotFoundException, IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // send existsRow request to the worker
    logger.debug("checking if row exists: " + row);
    HTTP.Response r = HTTP.doRequest("GET", "http://" + workers.elementAt(workerIndexForKey(row)).address + "/data/" + tableName + "/" + java.net.URLEncoder.encode(row, "UTF-8"), null);
    return r.statusCode() == 200;
  }

  public int count(String tableName) throws IOException {
    // ensure workers are loaded
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    int total = 0;
    logger.debug("counting rows in table: " + tableName);

    // request count from all workers
    for (WorkerEntry w : workers) {
    	logger.debug("requesting count from worker: " + w.address);
      HTTP.Response r = HTTP.doRequest("GET", "http://" + w.address + "/count/" + tableName, null);

      if ((r != null) && (r.statusCode() == 200)) {
        String result = new String(r.body());
        total += Integer.valueOf(result).intValue();
      }
    }

    logger.debug("total rows in table '" + tableName + "': " + total);
    return total;
  }

  public Iterator<Row> scan(String tableName) throws FileNotFoundException, IOException {
    // simple scan, no start/end row, delegates to full scan method
	  logger.debug("starting scan for table: " + tableName);
    return scan(tableName, null, null);
  }

  public Iterator<Row> scan(String tableName, String startRow, String endRowExclusive) throws FileNotFoundException, IOException {
    // ensure workers are downloaded if not already available
    if (!haveWorkers) {
    	logger.debug("workers not available, downloading...");
      downloadWorkers();
    }

    // create and return a new iterator for the scan
    logger.debug("scanning table: " + tableName + " from row: " + startRow + " to row: " + endRowExclusive);
    return new KVSIterator(tableName, startRow, endRowExclusive);
  }

  public static void main(String args[]) throws Exception {
    // check if enough args are provided
    if (args.length < 2) {
      System.err.println("syntax error. usage:");
      System.err.println("  client <coordinator> get <tableName> <row> <column>");
      System.err.println("  client <coordinator> put <tableName> <row> <column> <value>");
      System.err.println("  client <coordinator> scan <tableName>");
      System.err.println("  client <coordinator> count <tableName>");
      System.err.println("  client <coordinator> rename <oldTableName> <newTableName>");
      System.err.println("  client <coordinator> delete <tableName>");
      System.exit(1);
    }

    // create a KVS client connected to the coordinator
    KVSClient client = new KVSClient(args[0]);
    System.out.println("connected to coordinator at: " + args[0]);

    if (args[1].equals("put")) {
      // handle "put" command
      if (args.length != 6) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> put <tableName> <row> <column> <value>");
        System.exit(1);
      }
      logger.debug("putting value into table: " + args[2] + ", row: " + args[3] + ", column: " + args[4]);
      client.put(args[2], args[3], args[4], args[5].getBytes("UTF-8"));

    } else if (args[1].equals("get")) {
      // handle "get" command
      if (args.length != 5) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> get <tableName> <row> <column>");
        System.exit(1);
      }
      logger.debug("getting value from table: " + args[2] + ", row: " + args[3] + ", column: " + args[4]);
      byte[] val = client.get(args[2], args[3], args[4]);
      if (val == null)
        System.err.println("no value found");
//      else
//        System.out.write(val);

    } else if (args[1].equals("scan")) {
      // handle "scan" command
      if (args.length != 3) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> scan <tableName>");
        System.exit(1);
      }
      logger.debug("scanning table: " + args[2]);
      Iterator<Row> iter = client.scan(args[2], null, null);
      int count = 0;
      while (iter.hasNext()) {
//        System.out.println(iter.next());
        count++;
      }
      System.err.println(count + " row(s) scanned");

    } else if (args[1].equals("count")) {
      // handle "count" command
      if (args.length != 3) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> count <tableName>");
        System.exit(1);
      }
      logger.debug("counting rows in table: " + args[2]);
      logger.debug(client.count(args[2]) + " row(s) in table '" + args[2] + "'");

    } else if (args[1].equals("delete")) {
      // handle "delete" command
      if (args.length != 3) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> delete <tableName>");
        System.exit(1);
      }
      logger.debug("deleting table: " + args[2]);
      client.delete(args[2]);
      logger.debug("table '" + args[2] + "' deleted");

    } else if (args[1].equals("rename")) {
      // handle "rename" command
      if (args.length != 4) {
        System.err.println("syntax error. usage:");
        System.err.println("  client <coordinator> rename <oldTableName> <newTableName>");
        System.exit(1);
      }
      System.out.println("renaming table from '" + args[2] + "' to '" + args[3] + "'");
      if (client.rename(args[2], args[3]))
        System.out.println("rename successful");
      else
        System.out.println("rename failed");

    } else {
      // unknown command
      System.err.println("unknown command: " + args[1]);
      System.exit(1);
    }
  }
};
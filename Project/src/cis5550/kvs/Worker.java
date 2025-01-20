package cis5550.kvs;

// data structures and concurrency
import java.io.*;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

// tools
import cis5550.tools.KeyEncoder;
import cis5550.tools.Logger;

// webserver utils
import cis5550.webserver.Response;
import cis5550.webserver.Server;

// file handling

// networking and encoding
import java.net.URLEncoder;
import java.nio.file.FileSystems;
import java.nio.file.Files;

class Worker extends cis5550.generic.Worker {

	// logger instance for worker class
	private static final Logger log = Logger.getLogger(Worker.class);

	// version identifier for this worker
	private static final String workerVersion = "v1.10a Oct 26 2023";

	// in-memory table storage
	protected static Map<String, Map<String, Row>> tableData;

	// flag for using subdirectories in storage
	static final boolean useSubdirs = false;

	// base directory for storage
	static String baseStorageDir;

	// replication flags/settings (not enabled here)
	static final boolean replicationEnabled = false;
	static final int replicaCount = 3; // default replica count
	static String[] replicaServers = new String[2]; // stores replica server addresses

	// local range end for key space management
	static String localRangeEnd;

	// constructor (not doing anything special here)
	Worker() {
		log.info("worker instance created.");
	}

	// custom exception class for KVS errors
	static final class KVSException extends Exception {
		final String errorMessage; // error message
		final int statusCode; // HTTP status code

		// constructor for exception with message and code
		public KVSException(String message, int code) {
			super(message);
			this.errorMessage = message;
			this.statusCode = code;
			log.error("KVSException: " + message + " (HTTP " + code + ")");
		}
	}

	// create table if doesn't already exist
	static synchronized void createTableIfAbsent(String tableName) throws FileNotFoundException {
		// check if table already exists
		if (tableData.get(tableName) == null) {
			log.info("creating new table: " + tableName);
			tableData.put(tableName, new HashMap<>());

			// handle persistent table storage
			if (tableName.startsWith("pt-")) {
				String tablePath = baseStorageDir;
				// make directory for the table
				new File(tablePath + File.separator + KeyEncoder.encode(tableName)).mkdirs();
				log.info("persistent table storage initialized for: " + tableName);
			}
		} else {
			log.info("table '" + tableName + "' already exists.");
		}
	}

	// fetch a row from the table
	static synchronized Row fetchRow(String tableName, String rowKey, boolean throwIfMissing) throws Exception {
		log.info("fetching row '" + rowKey + "' from table '" + tableName + "'");

		// if table uses persistent storage
		if (tableName.startsWith("pt-")) {
			String filePath = baseStorageDir;
			File rowFile = new File(filePath + File.separator + KeyEncoder.encode(tableName) + File.separator + KeyEncoder.encode(rowKey));

			// check if the row file exists
			if (!rowFile.exists()) {
				log.warn("row file not found for: " + rowKey + " in table: " + tableName);
				return null; // row doesn't exist
			} else {
				log.info("row file found, reading data...");
				FileInputStream rowInputStream = new FileInputStream(rowFile);
				Row row = Row.readFrom(rowInputStream);
				rowInputStream.close();
				log.info("row successfully read for: " + rowKey);
				return row;
			}
		} else {
			// in-memory table handling
			Map<String, Row> tableRows = tableData.get(tableName);

			// check if table exists
			if (tableRows == null) {
				log.error("table '" + tableName + "' not found.");
				throw new KVSException("Table '" + tableName + "' not found", 404);
			} else {
				// check if the row exists in the table
				Row row = tableRows.get(rowKey);
				if (row == null) {
					log.warn("row '" + rowKey + "' not found in table: " + tableName);
					if (throwIfMissing) {
						throw new KVSException("Row '" + rowKey + "' not found in table '" + tableName + "'", 404);
					} else {
						return null; // row doesn't exist but no exception needed
					}
				} else {
					log.info("row found for: " + rowKey + " in table: " + tableName);
					return row;
				}
			}
		}
	}

	// save a row to the table
	static synchronized void saveRow(String tableName, Row rowData) throws IOException {
		log.info("Saving row '" + rowData.key() + "' to table '" + tableName + "'");

		// handle persistent storage
		if (tableName.startsWith("pt-")) {
			String filePath = baseStorageDir;
			String rowFilePath = filePath + File.separator + KeyEncoder.encode(tableName) + File.separator + KeyEncoder.encode(rowData.key());

			// write row data to file
			FileOutputStream rowOutputStream = new FileOutputStream(rowFilePath);
			rowOutputStream.write(rowData.toByteArray());
			rowOutputStream.close();
			log.info("row successfully saved to persistent storage: " + rowFilePath);
		} else {
			// in-memory table storage
			Map<String, Row> tableRows = tableData.get(tableName);
			tableRows.put(rowData.key(), rowData);
			log.info("row successfully saved in-memory for table: " + tableName);
		}
	}

	// recursively delete all files and subdirectories in a directory
	static void removeFileRecursively(File directory) {
		// check if the current file is a directory
		if (directory.isDirectory()) {
			// get list of all files in the directory
			File[] fileList = directory.listFiles();

			// iterate over each file
			for (File file : fileList) {
				// recursive call for subdirectories or files
				removeFileRecursively(file);
			}
		}
		// delete the current file or directory
		directory.delete();
		System.out.println("deleted: " + directory.getPath());
	}

	// recursively write rows to the response based on range conditions
	static void writeRowsRecursively(String tableName, File directory, String startRow, String endRowExclusive, Response response) throws Exception {
		// get list of all files in the directory
		File[] fileList = directory.listFiles();

		// iterate through each file
		for (File file : fileList) {
			// if it's a directory, dive deeper recursively
			if (file.isDirectory()) {
				writeRowsRecursively(tableName, file, startRow, endRowExclusive, response);
			}
			// check if the file matches the row range conditions
			else if ((startRow == null || startRow.compareTo(file.getName()) <= 0) &&
					(endRowExclusive == null || endRowExclusive.compareTo(file.getName()) > 0)) {
				// fetch the row and write to the response
				response.write(fetchRow(tableName, file.getName(), true).toByteArray());
				// append a newline byte for separation
				response.write(new byte[]{10});
				log.debug("row written: " + file.getName());
			}
		}
	}

	// recursively count the total number of files in a directory
	static int calculateRecursiveCount(File directory) throws Exception {
		int count = 0;
		// get all files in the directory
		File[] fileList = directory.listFiles();

		// iterate over files
		for (File file : fileList) {
			// if directory, recurse and add to count
			count += file.isDirectory() ? calculateRecursiveCount(file) : 1;
		}
		log.debug("count for directory " + directory.getPath() + ": " + count);
		return count;
	}

	// collect all keys from files in a directory (including subdirectories)
	static void collectAllKeys(File directory, TreeSet<String> keySet) throws Exception {
		// get all files in the directory
		File[] fileList = directory.listFiles();

		// iterate through files
		for (File file : fileList) {
			// recurse for subdirectories
			if (file.isDirectory()) {
				collectAllKeys(file, keySet);
			}
			// decode and add file name to the key set
			else {
				keySet.add(KeyEncoder.decode(file.getName()));
				log.debug("key collected: " + file.getName());
			}
		}
	}

	// compute the size of a table, either in-memory or persistent
	static int computeTableSize(String tableName) throws Exception {
		// for in-memory tables
		if (!tableName.startsWith("pt-")) {
			Map<String, Row> tableRows = tableData.get(tableName);
			int size = tableRows == null ? 0 : tableRows.size();
			System.out.println("in-memory table '" + tableName + "' size: " + size);
			return size;
		}
		// for persistent tables
		else {
			String tablePath = baseStorageDir;
			File directory = new File(tablePath + File.separator + KeyEncoder.encode(tableName));

			// check if the directory exists, and calculate count recursively
			int size = directory.exists() ? calculateRecursiveCount(directory) : 0;
			System.out.println("Persistent table '" + tableName + "' size: " + size);
			return size;
		}
	}

	public static void main(String[] args) {

		// check correct num of arguments
		if (args.length != 3) {
			// log error if syntax  wrong
			System.err.println("Syntax: Worker <port> <storageDir> <coordinatorIP:port>");
			System.exit(1); // exit if args are invalid
		}

		// parse the worker port from args
		int workerPort = Integer.parseInt(args[0]);

		// store base directory for storage
		baseStorageDir = args[1];

		// store the coordinator's address
		String coordinatorAddress = args[2];

		log.info("KVS Worker (" + workerVersion + ") starting on port " + workerPort);

		// init in-memory table storage as a thread-safe map
		tableData = new ConcurrentHashMap<>();

		// variable for the worker's unique ID
		String workerID = null;

		try {
			// check if storage dir exists
			if (!(new File(baseStorageDir)).exists()) {

				// if not, create the dir
				(new File(baseStorageDir)).mkdir();
				log.debug("Storage directory created at: " + baseStorageDir);
			}

			// locate or create file with worker's unique ID
			File idFile = new File(baseStorageDir + File.separator + "id");

			// if ID file exists, read the worker ID from it
			if (idFile.exists()) {
				workerID = (new Scanner(idFile)).nextLine();
				log.debug("Using existing ID: " + workerID);
			}

			// if no ID found, generate a new random ID
			if (workerID == null) {
				workerID = new Random()

						//generate random lowercase ascii
						.ints(97, 123)
						//limit id len to 5 chars
						.limit(5)
						.collect(StringBuilder::new, StringBuilder::appendCodePoint, StringBuilder::append)
						.toString();

				log.debug("choosing new ID: " + workerID);

				// write new ID to the ID file
				BufferedWriter writer = new BufferedWriter(new FileWriter(idFile));
				writer.write(workerID);
				writer.close();
				log.debug("worker ID saved to file: " + idFile.getPath());
			}
		} catch (Exception e) {
			// log the stack trace for debugging any issues
			e.printStackTrace();
			log.error("error initializing worker: " + e.getMessage());
		}


		// start the thread to ping the coordinator regularly
		startPingThread(coordinatorAddress, workerID, workerPort);

		// set the server to listen on the specified worker port
		Server.port(workerPort);

		// handle PUT requests for specific table, row, and column
		Server.put("/data/:table/:row/:column", (request, response) -> {
			// extract table, row, and column keys from URL params
			String tableName = request.params("table");
			String rowKey = request.params("row");
			String columnKey = request.params("column");

			// check if keys provided
			if (tableName != null && rowKey != null && columnKey != null) {

				log.info("PUT('" + tableName + "','" + rowKey + "','" + columnKey + "',[" + request.bodyAsBytes().length + " bytes])");

				// ensure table exists or create if missing
				createTableIfAbsent(tableName);

				// sync access to table data for thread safety
				synchronized (tableData) {
					// fetch row, create new if not found
					Row row = fetchRow(tableName, rowKey, false);
					if (row == null) {
						row = new Row(rowKey);
					}

					// add or update column in the row with the req data
					row.put(columnKey, request.bodyAsBytes());

					// save updated row back to storage
					saveRow(tableName, row);

					// return success response
					return "OK";
				}
			} else {
				log.warn("Bad request: Missing row or column keys for PUT");
				response.status(401, "Bad request");
				return "Row and column keys must both be present";
			}
		});

		// handle streaming PUT req for entire tables
		Server.put("/data/:table/", (request, response) -> {
			// extract table name from URL params
			String tableName = request.params("table");
			log.info("Streaming PUT('" + tableName + "')");

			// ensure table exists, create if missing
			createTableIfAbsent(tableName);

			// read data from req body as a byte stream
			ByteArrayInputStream inputStream = new ByteArrayInputStream(request.bodyAsBytes());

			// process each row in  stream
			while (true) {

				//read from from stream
				Row row = Row.readFrom(inputStream);
				if (row == null) {

					// if no more rows, return success response
					log.info("finished streaming PUT for table: " + tableName);
					return "OK";
				}

				// save row to the table
				saveRow(tableName, row);
			}
		});

		// handle DELETE requests for tables
		Server.put("/delete/:table/", (request, response) -> {
			// extract table name from URL params
			String tableName = request.params("table");

			log.info("DELETE('" + tableName + "')");

			// check if table exists
			if (tableData.get(tableName) == null) {
				// if table not found, return 404 response
				log.warn("Delete failed: Table '" + tableName + "' not found");
				response.status(404, "Not found");
				return "Table '" + tableName + "' not found";
			} else {
				// r, the table from in-memory storage
				tableData.remove(tableName);

				// delete table files from storage
				String tablePath = baseStorageDir;
				removeFileRecursively(new File(tablePath + File.separator + KeyEncoder.encode(tableName)));

				// log successful deletion and return OK
				log.info("Successfully deleted table: " + tableName);
				return "OK";
			}
		});

		// handle table rename reqs
		Server.put("/rename/:table/", (request, response) -> {
			// get old and new table names
			String oldTableName = request.params("table");
			String newTableName = request.body();

			log.info("RENAME('" + oldTableName + "' -> '" + newTableName + "')");

			// disallow conversion from persistent to in-memory tables
			if (oldTableName.startsWith("pt-") && !newTableName.startsWith("pt-")) {
				log.warn("cannot convert persistent table to in-memory");
				response.status(400, "cannot convert a table from persistent to in-memory");
				return "Cannot convert a table from persistent to in-memory";
			}

			// handle in-memory table renaming
			if (!oldTableName.startsWith("pt-")) {

				// get  old table
				Map<String, Row> oldTable = tableData.get(oldTableName);
				if (oldTable == null) {
					// return 404 if old table doesn't exist
					log.warn("rename failed: table '" + oldTableName + "' not found");
					response.status(404, "Not found");
					return "Table '" + oldTableName + "' not found";
				}

				// return conflict if new table already exists
				if (tableData.get(newTableName) != null) {
					log.warn("renaming conflict: table name '" + newTableName + "' already exist");
					response.status(409, "Conflict");
					return "Table '" + newTableName + "' already exists!";
				}

				// rename the table in-memory
				tableData.put(newTableName, oldTable);
				tableData.remove(oldTableName);

				// if new table is persistent, save rows to disk
				if (newTableName.startsWith("pt-")) {
					String tablePath = baseStorageDir;
					new File(tablePath + File.separator + KeyEncoder.encode(newTableName)).mkdirs();
					for (Map.Entry<String, Row> entry : oldTable.entrySet()) {
						saveRow(newTableName, entry.getValue());
					}
				}

				//rename persistent table files
			} else {

				Files.move(
						FileSystems.getDefault().getPath(baseStorageDir, oldTableName),
						FileSystems.getDefault().getPath(baseStorageDir, newTableName)
				);
			}

			// return success response
			log.info("succesfully renamed table '" + oldTableName + "' to '" + newTableName + "'");
			return "OK";
		});

		// handle reqs to list all tables
		Server.get("/tables", (request, response) -> {
			// build string of table names
			StringBuilder tableList = new StringBuilder();
			for (String tableName : tableData.keySet()) {
				tableList.append(tableName).append("\n");
			}

			// return list of tables
			log.info("Listing all tables");
			return tableList.toString();
		});

		// handle reqs to view table content
		Server.get("/view/:table/", (request, response) -> {

			// get the table name and opt start row
			String tableName = request.params("table");
			String startRow = request.queryParams("startRow");

			// start building html response
			StringBuilder htmlResponse = new StringBuilder("<html><head><title>Table view</title></head><body>\n");
			htmlResponse.append("<h3>Table ").append(tableName).append("</h3>\n<table border=\"1\">\n");

			// get table rows and keys
			Map<String, Row> tableRows = tableData.get(tableName);
			TreeSet<String> rowKeys = new TreeSet<>();

			// collect all keys if table persistent
			if (tableName.startsWith("pt-")) {
				String tablePath = baseStorageDir;
				collectAllKeys(new File(tablePath + File.separator + KeyEncoder.encode(tableName)), rowKeys);
			} else {
				// add all keys from in-memory table
				rowKeys.addAll(tableRows.keySet());
			}

			// filter keys based on startRow
			if (startRow != null) {
				rowKeys = (TreeSet<String>) rowKeys.tailSet(startRow);
			}

			// determine column names from first 10 rows
			TreeSet<String> columnNames = new TreeSet<>();
			Iterator<String> rowIterator = rowKeys.iterator();
			for (int i = 0; i < 10 && rowIterator.hasNext(); ++i) {
				columnNames.addAll(fetchRow(tableName, rowIterator.next(), true).columns());
			}

			// add header row to  table
			htmlResponse.append("<tr><td><b>Key</b>");
			for (String columnName : columnNames) {
				htmlResponse.append("<td><b>").append(columnName).append("</b></td>");
			}
			htmlResponse.append("</tr>\n");

			// add up to 10 rows of data
			rowIterator = rowKeys.iterator();
			for (int i = 0; i < 10 && rowIterator.hasNext(); ++i) {
				String rowKey = rowIterator.next();
				Row row = fetchRow(tableName, rowKey, true);
				htmlResponse.append("<tr><td>").append(rowKey).append("</td>");

				for (String columnName : columnNames) {
					htmlResponse.append("<td>").append(row.get(columnName)).append("</td>");
				}

				htmlResponse.append("</tr>\n");
			}

			// add link to the next page if there are more rows
			htmlResponse.append("</table><p>");
			if (rowIterator.hasNext()) {
				htmlResponse.append("<a href=\"/view/").append(tableName)
						.append("?startRow=").append(URLEncoder.encode(rowIterator.next(), "UTF-8"))
						.append("\">Next</a>\n");
			}

			// close HTML document
			htmlResponse.append("</body></html>\n");

			// return the response
			log.info("viewing table: " + tableName);
			return htmlResponse.toString();
		});

		//handle requests to view a worker
		String finalWorkerID = workerID;
		Server.get("/", (req, res) -> {
			res.type("text/html");

			StringBuilder html = new StringBuilder();
			html.append("<!DOCTYPE html><html><head><title>KVS worker</title></head><body>\n");
			html.append("<h3>KVS worker '" + finalWorkerID + "' (port " + workerPort + ")</h3>\n<table border=\"1\">\n");

			for (String table : tableData.keySet()) {
				html.append("<tr><td><a href=\"/view/").append(table).append("\">").append(table).append("</a></td><td>").append(computeTableSize(table)).append("</td></tr>");
			}

			html.append("</table></body></html>");
			return html.toString();
		});

		// handle requests to count rows in a table
		Server.get("/count/:table/", (request, response) -> {
			// get table name from request params
			String tableName = request.params("table");

			// compute the size of the table
			int tableCount = computeTableSize(tableName);

			// return 0 if table not found
			if (tableCount == 0) {
				return String.valueOf(0);
			} else {
				// return the row count
				log.info("counted " + tableCount + " rows in table '" + tableName + "'");
				return String.valueOf(tableCount);
			}
		});


		// handle requests to fetch rows from a table
		Server.get("/data/:table/", (request, response) -> {
			// get table name and row range from request params
			String tableName = request.params("table");
			String startRow = request.queryParams("startRow");
			String endRowExclusive = request.queryParams("endRowExclusive");
			byte[] rowSeparator = new byte[]{10}; // newline separator for rows

			// check if table is in-memory or persistent
			if (!tableName.startsWith("pt-")) {
				// handle in-memory table
				Map<String, Row> tableRows = tableData.get(tableName);
				if (tableRows == null) {
					//log.warn("fetch failed: Table '" + tableName + "' not found");
					response.status(404, "Not found");
					return "Table '" + tableName + "' not found";
				}

				// iterate over rows and write matching rows to rsp object
				for (Map.Entry<String, Row> entry : tableRows.entrySet()) {
					String rowKey = entry.getKey();
					if ((startRow == null || startRow.compareTo(rowKey) <= 0) &&
							(endRowExclusive == null || endRowExclusive.compareTo(rowKey) > 0)) {
						response.write(entry.getValue().toByteArray());
						response.write(rowSeparator);
					}
				}
			} else {

				// handle persistent table
				String tablePath = baseStorageDir;
				File directory = new File(tablePath + File.separator + KeyEncoder.encode(tableName));
				if (!directory.exists()) {
					log.warn("fetch failed: persistent table '" + tableName + "' not found");
					response.status(404, "Not found");
					return "Table '" + tableName + "' not found";
				}

				// recurse to write matching rows from the dir
				writeRowsRecursively(tableName, directory, startRow, endRowExclusive, response);
			}

			// write final row separator
			response.write(rowSeparator);

			// return null (streaming response)
			log.info("rows fetched from table '" + tableName + "' with startRow=" + startRow + " and endRowExclusive=" + endRowExclusive);
			return null;
		});

		//GET route with column
		Server.get("/data/:t/:r/:c", (req, res) -> {
			res.type("text/plain");
			String table = req.params("t");
			String rowKey = req.params("r");
			String column = req.params("c");

			try {
				Row row = fetchRow(table, rowKey, true);
				byte[] value = row != null ? row.getBytes(column) : null;
				if (value == null) {
					throw new KVSException("Column " + column + " not found in row '" + rowKey + "' of table '" + table + "'", 404);
				} else {
					log.debug("Returning a " + value.length + "-byte value");
					res.bodyAsBytes(value);
					return null;
				}
			} catch (KVSException e) {
				log.debug(e.errorMessage);
				res.status(e.statusCode, e.errorMessage);
				return e.errorMessage;
			}
	});

		Server.get("/data/:table/:row", (req, res) -> {
			String table = req.params("table");
			String rowKey = req.params("row");

			try {
				Row row = fetchRow(table, rowKey, true);
				if (row != null) {
					res.bodyAsBytes(row.toByteArray());
				} else {
					res.status(404, "Not found");
				}

				return null;
			} catch (KVSException e) {
				log.debug(e.errorMessage);
				res.status(e.statusCode, e.errorMessage);
				return e.errorMessage;
			}
		});

		// return the version of the worker
		Server.get("/version", (request, response) -> {
			// return the worker version
			log.info("worker version requested: " + workerVersion);
			return workerVersion;
		});

	}

}
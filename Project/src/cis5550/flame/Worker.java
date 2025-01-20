package cis5550.flame;

import cis5550.flame.FlamePairRDD.PairToStringIterable;
import cis5550.flame.FlamePairRDD.TwoStringsToString;
// kvs imports
import cis5550.kvs.KVS;
import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;

// tools
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;
import cis5550.webserver.ConnectionHandler;
// webserver
import cis5550.webserver.Request;
import cis5550.webserver.Server;

import static cis5550.webserver.Server.post;

// std lib imports
import java.io.File;
import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.Iterator;
import java.util.Random;
import java.util.Vector;


class Worker extends cis5550.generic.Worker {
	private static final Logger log = Logger.getLogger(Worker.class);
    // default worker constructor
    Worker() {}

    // perform an op using an iterator
    static String execIteratorOp(String operationName, Request request, IteratorTwoStringsAndKVSToString operation) throws Exception {
        // get input/output table names and params from request
        String inputTable = request.queryParams("inputTable");
        String outputTable = request.queryParams("outputTable");
        String secondaryInputTable = request.queryParams("inputTable2");
        String kvsCoordinator = request.queryParams("kvsCoordinator");
        String fromKey = request.queryParams("fromKey");
        String toKeyExclusive = request.queryParams("toKeyExclusive");

        // log the received params
        log("Performing operation: " + operationName);
        log("Input table: " + inputTable);
        log("Output table: " + outputTable);
        log("Secondary input table: " + secondaryInputTable);
        log("KVS Coordinator: " + kvsCoordinator);
        log("Key range: fromKey=" + fromKey + ", toKeyExclusive=" + toKeyExclusive);

        // setup kvs client for the operation
        Coordinator.kvsClient = new KVSClient(kvsCoordinator);

        // invoke op with the iterator
        String result = operation.op(Coordinator.kvsClient.scan(inputTable, fromKey, toKeyExclusive), outputTable, secondaryInputTable, Coordinator.kvsClient);

        // log the result
        log("Operation result: " + result);

        // return the result string
        return result;
    }

    // perform an op by iterating rows and applying func
    static String perform(String operationName, Request request, RowTwoStringsAndKVSToInt operation) throws Exception {
        // use performWithIterator w/ a lambda func to process rows
        return execIteratorOp(operationName, request, (rowIterator, outputTable, inputTable2, kvs) -> {
            // init counters for input/output keys
            int inputKeys = 0;
            int outputKeys = 0;

            // process each row in iterator
            while (rowIterator.hasNext()) {
                // get current row
                Row currentRow = rowIterator.next();

                // apply operation and update counters
                outputKeys += operation.op(currentRow, outputTable, inputTable2, kvs);
                inputKeys++;

                // log progress for debugging
//                log("Processed key #" + inputKeys + ", outputKeys=" + outputKeys);
            }

            // log summary of operation
//            log("Finished processing: " + inputKeys + " keys in, " + outputKeys + " keys out");

            // return summary result
            return "OK - " + inputKeys + " keys in, " + outputKeys + " keys out";
        });
    }


    // interface 4 defining op that processes rows using iterator
    public interface IteratorTwoStringsAndKVSToString extends Serializable {
        // perform op w/ row iterator, output table, secondary table, kvs client
        String op(Iterator<Row> rows, String outputTable, String secondaryInputTable, KVS kvs) throws Exception;
    }

    // interface 4 defining op that processes single row
    public interface RowTwoStringsAndKVSToInt extends Serializable {
        // perform op w/ row, output table, secondary table, kvs client
        int op(Row row, String outputTable, String secondaryInputTable, KVS kvs) throws Exception;
    }



    public static void main(String[] args) {
        if (args.length != 2) {
            System.err.println("Syntax: Worker <port> <coordinatorIP:port>");
            System.exit(1);
        }

        // parse the server port from the command-line args
        int serverPort = Integer.parseInt(args[0]);

        // get the coordinator address from args
        String coordinatorAddress = args[1];

        // start a background thread to ping the coordinator regularly
        System.out.println("Starting ping thread for coordinator: " + coordinatorAddress + " on port: " + serverPort);
        startPingThread(coordinatorAddress, "" + serverPort, serverPort);

        // setup a file to store the current worker jar
        File jarFile = new File("__worker" + serverPort + "-current.jar");
        System.out.println("worker JAR file initialized: " + jarFile.getAbsolutePath());

        // set the server to listen on the specified port
        System.out.println("setting up server on port: " + serverPort);
        Server.port(serverPort);

        // endpoint to upload a new JAR file for the worker
        Server.post("/useJAR", (request, response) -> {
            System.out.println("Uploading new JAR...");
            FileOutputStream fileOutputStream = new FileOutputStream(jarFile);

            // write the uploaded JAR content to the file
            fileOutputStream.write(request.bodyAsBytes());
            fileOutputStream.close();
            System.out.println("JAR upload complete.");
            return "OK";
        });

        // endpoint to handle flatMap operation
        Server.post("/flatMap", (request, response) -> {
            System.out.println("processing flatMap request...");

            // deserialize the mapper function from the request body
            FlameRDD.StringToIterable mapper = (FlameRDD.StringToIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // execute the flatMap operation
            return perform("flatMap", request, (row, outputTable, inputTable2, kvs) -> {
                // apply mapper to row value and iterate over results
                Iterator<String> results = mapper.op(row.get("value")).iterator();
                int count = 0;

                // store each result in the KVS
                while (results.hasNext()) {
                    String value = results.next();
                    kvs.put(outputTable, Hasher.hash(row.key() + count++), "value", value.getBytes());
                }

                log.debug("flatMap completed: " + count + " keys written.");
                return count;
            });
        });

        // endpoint to handle mapToPair operation
        Server.post("/mapToPair", (request, response) -> {
            System.out.println("Processing mapToPair request...");

            // deserialize the pair mapper function from the request body
            FlameRDD.StringToPair pairMapper = (FlameRDD.StringToPair) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // execute the mapToPair operation
            return perform("mapToPair", request, (row, outputTable, inputTable2, kvs) -> {
                // apply pair mapper to row value
                FlamePair pair = pairMapper.op(row.get("value"));
                if (pair != null) {
                    // store the resulting pair in the KVS
                    kvs.put(outputTable, pair._1(), row.key(), pair._2().getBytes());
//                    System.out.println("mapToPair result: key=" + pair._1() + ", value=" + pair._2());
                }
                return 1;
            });
        });


        // endpoint 4 foldByKey operation
        Server.post("/foldByKey", (request, response) -> {
            System.out.println("processing foldByKey request...");

            // deserialize the folder function from request body
            FlamePairRDD.TwoStringsToString folder = (FlamePairRDD.TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // execute foldByKey operation
            return perform("foldByKey", request, (row, outputTable, inputTable2, kvs) -> {
                // get the initial accumulator value from request params
                String accumulator = request.queryParams("zero");
                int columnCount = 0;

                // iterate over columns of the row
                for (Iterator<String> columns = row.columns().iterator(); columns.hasNext(); columnCount++) {
                    String column = columns.next();
                    // apply folder function to combine values
                    accumulator = folder.op(accumulator, row.get(column));
                }

                // store the final accumulated value in KVS
                kvs.put(outputTable, row.key(), "acc", accumulator.getBytes());

                log.debug("foldByKey completed: " + columnCount + " columns processed for key: " + row.key());
                return columnCount;
            });
        });

        // endpoint 4 filter operation
        Server.post("/filter", (request, response) -> {
        	log.debug("Processing filter request...");

            // deserialize the filter function from request body
            FlameRDD.StringToBoolean filter = (FlameRDD.StringToBoolean) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // execute filter operation
            return perform("filter", request, (row, outputTable, inputTable2, kvs) -> {
                // apply filter function to decide if row should be kept
                boolean keep = filter.op(row.get("value"));
                if (keep) {
                    // if kept, store the row value in KVS
                    kvs.put(outputTable, row.key(), "value", row.get("value").getBytes());
                }
                log.debug("Filter result for key: " + row.key() + " - " + (keep ? "kept" : "discarded"));
                return keep ? 1 : 0;
            });
        });

        // endpoint 4 mapPartitions operation
        Server.post("/mapPartitions", (request, response) -> {
            System.out.println("Processing mapPartitions request...");

            // deserialize the mapper function from request body
            FlameRDD.IteratorToIterator mapper = (FlameRDD.IteratorToIterator) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // execute mapPartitions operation
            return execIteratorOp("mapPartitions", request, (rowIterator, outputTable, inputTable2, kvs) -> {
                // get partition ID from request params
                String partitionId = request.queryParams("part");

                // apply mapper function to the entire partition
                Iterator<String> mappedValues = mapper.op(new Iterator<String>() {
                    public String next() {
                        // get next row value
                        return rowIterator.next().get("value");
                    }

                    public boolean hasNext() {
                        // check if more rows exist
                        return rowIterator.hasNext();
                    }
                });

                int keyCount = 0;
                // iterate over mapped values and store them in KVS
                while (mappedValues.hasNext()) {
                    String value = mappedValues.next();
                    kvs.put(outputTable, Hasher.hash(partitionId + keyCount++), "value", value.getBytes());
                }

                System.out.println("mapPartitions completed: " + keyCount + " keys written for partition: " + partitionId);
                return "OK - " + keyCount + " keys written";
            });
        });


        // endpoint 4 cogroup operation
        Server.post("/cogroup", (request, response) -> {
            // get input/output table names and kvs coordinator from request params
            String inputTable1 = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String inputTable2 = request.queryParams("inputTable2");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");

            System.out.println("Processing cogroup with inputTable1: " + inputTable1 + ", inputTable2: " + inputTable2 + ", outputTable: " + outputTable);

            // create kvs client to access tables
            KVSClient kvs = new KVSClient(kvsCoordinator);

            // scan rows from the first input table
            Iterator<Row> rows1 = kvs.scan(inputTable1, fromKey, toKeyExclusive);
            int writtenKeys = 0;

            // loop thru rows in inputTable1
            while (rows1.hasNext()) {
                Row row = rows1.next();
                String values1 = "[";

                // collect all column values from the row
                for (String column : row.columns()) {
                    values1 += (values1.equals("[") ? "" : ",") + row.get(column);
                }
                values1 += "]";

                // get matching row from inputTable2
                Row row2 = kvs.getRow(inputTable2, row.key());
                String values2 = "[";

                if (row2 != null) {
                    // collect all column values from the matching row
                    for (String column : row2.columns()) {
                        values2 += (values2.equals("[") ? "" : ",") + row2.get(column);
                    }
                }
                values2 += "]";

                // store combined values in the output table
                kvs.put(outputTable, row.key(), "value", (values1 + "," + values2).getBytes());
                writtenKeys++;
                System.out.println("cogroup: wrote key: " + row.key());
            }

            // scan rows from inputTable2 for rows missing in inputTable1
            Iterator<Row> rows2 = kvs.scan(inputTable2, fromKey, toKeyExclusive);

            while (rows2.hasNext()) {
                Row row = rows2.next();
                if (kvs.getRow(inputTable1, row.key()) == null) {
                    String values2 = "[";

                    // collect all column values from the row
                    for (String column : row.columns()) {
                        values2 += (values2.equals("[") ? "" : ",") + row.get(column);
                    }
                    values2 += "]";

                    // store combined values in the output table
                    kvs.put(outputTable, row.key(), "value", "[]," + values2);
                    writtenKeys++;
                    System.out.println("cogroup: wrote key (missing in inputTable1): " + row.key());
                }
            }

            System.out.println("cogroup completed: " + writtenKeys + " keys written.");
            return "OK - " + writtenKeys + " keys written";
        });

        // endpoint 4 intersection operation
        Server.post("/intersection", (request, response) -> {
            // get input/output table names and kvs coordinator from request params
            String inputTable1 = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String inputTable2 = request.queryParams("inputTable2");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String tempTable = request.queryParams("tempTable");

            log.debug("Processing intersection with inputTable1: " + inputTable1 + ", inputTable2: " + inputTable2 + ", outputTable: " + outputTable);

            // create kvs client to access tables
            KVSClient kvs = new KVSClient(kvsCoordinator);

            // scan rows from the first input table
            Iterator<Row> rows1 = kvs.scan(inputTable1, fromKey, toKeyExclusive);

            // mark rows from inputTable1 in tempTable
            while (rows1.hasNext()) {
                Row row = rows1.next();
                kvs.put(tempTable, row.get("value"), "in1", "1");
                log.debug("intersection: marked value from inputTable1: " + row.get("value"));
            }

            // scan rows from the second input table
            Iterator<Row> rows2 = kvs.scan(inputTable2, fromKey, toKeyExclusive);

            // mark rows from inputTable2 in tempTable
            while (rows2.hasNext()) {
                Row row = rows2.next();
                kvs.put(tempTable, row.get("value"), "in2", "1");
                log.debug("intersection: marked value from inputTable2: " + row.get("value"));
            }

            // scan rows from tempTable to find common keys
            Iterator<Row> rowsTemp = kvs.scan(tempTable, fromKey, toKeyExclusive);

            while (rowsTemp.hasNext()) {
                Row row = rowsTemp.next();
                if (row.columns().size() > 1) {
                    // if marked in both tables, store in output table
                    kvs.put(outputTable, row.key(), "value", row.key());
                    log.debug("intersection: wrote intersected key: " + row.key());
                }
            }

            // log completion details
            log.debug("intersection completed.");
            return "OK";
        });
        
        //adding in join code
        Server.post("/join", (request, response) -> {
        	try {
        		//get query parameter values
//        		Vector<String> qParams = decodeReqParams(request);
        		
        		// get input/output table names and kvs coordinator from request params
                String inputTable1 = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
                String inputTable2 = request.queryParams("inputTable2");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKeyExclusive");
//                String tempTable = request.queryParams("tempTable");

//        		String outputTable = qParams.elementAt(1);
        		String otherTable = inputTable2;
        		
        		KVSClient kvs = new KVSClient(kvsCoordinator);
        		
        		//scan table 1
        		Iterator<Row> table1 = kvs.scan(inputTable1, fromKey, toKeyExclusive);
        		
        		while(table1.hasNext()) {
        			Row a = table1.next();
        			String rowKey = a.key();
        			Row b = kvs.getRow(otherTable, rowKey);
        			
        			if(b != null) {//check of row with matching key was returned
        				for(String aCol : a.columns()) {
        					for(String bCol : b.columns()) {
        						FlamePair fp = new FlamePair(rowKey, a.get(aCol) + "," + b.get(bCol));
        						String newColName = Hasher.hash(aCol) + Hasher.hash(bCol);
        						kvs.put(outputTable, fp._1(), newColName, fp._2());
        					}
        				}
        			}	
        		}
        	}catch(Exception e) {
        		response.status(500, "Error in pairRDD/join on Worker");
        		return e.getStackTrace();
        	}
        	
        	return "OK";
        });
        
        //adding in pairRDD/FlatMap code
        post("/pairFlatMap", (request, response) -> {
        	try {
//        		Vector<String> qParams = decodeReqParams(request);
        		
        		// get input/output table names and kvs coordinator from request params
                String inputTable1 = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
//                String inputTable2 = request.queryParams("inputTable2");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKeyExclusive");

//        		String outputTable = qParams.elementAt(1);

        		//deserialize the lambda using Serializer - provide name of job's jar file
        		FlamePairRDD.PairToStringIterable lambda =  (PairToStringIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);


        		KVSClient kvs = new KVSClient(kvsCoordinator);
        		Iterator<Row> rows = kvs.scan(inputTable1, fromKey, toKeyExclusive);
        		int count = 1;
        		Random rand = new Random();
        		
        		while(rows.hasNext()) {
        			Row r = rows.next();
        			String rowKey = r.key();
        			
        			//iterate through columns to create FlamePairs to send to the lambda
        			for(String col : r.columns()) {
        				FlamePair fp = new FlamePair(rowKey, r.get(col));
        				
        				//lambda returns Iterable<String>
        				Iterable<String> result = lambda.op(fp);
        				
        				//iterate through iterable 
        				for(String s : result) {
        					//create unique row name
        					String rowName = Hasher.hash(rand.nextInt(6045) + count + String.valueOf(System.currentTimeMillis() / rand.nextInt(3284)));
        				
        					//add to output table
        					kvs.put(outputTable, rowName, "value", s);
        					count++;
        				}    				
        			}    		
        		}    		
        	}catch(Exception e) {
        		response.status(500, "Error in pairRDD/flatMap on Worker");
        		return e.getStackTrace();
        	}
        	    	
        	return "OK";
        });
        
        //adding in rdd/fold code
        post("/fold", (request, response) -> {
        	try {
//        		Vector<String> qParams = decodeReqParams(request);
        		
        		// get input/output table names and kvs coordinator from request params
                String inputTable1 = request.queryParams("inputTable");
                String outputTable = request.queryParams("outputTable");
//                String inputTable2 = request.queryParams("inputTable2");
                String kvsCoordinator = request.queryParams("kvsCoordinator");
                String fromKey = request.queryParams("fromKey");
                String toKeyExclusive = request.queryParams("toKeyExclusive");
        		
//        		String outputTable = qParams.elementAt(1);
//                String accumulator = request.queryParams("zero");
        		String zeroElement = request.queryParams("zero");

        		//deserialize the lambda using Serializer - provide name of job's jar file
        		FlamePairRDD.TwoStringsToString lambda = (TwoStringsToString) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

        		KVSClient kvs = new KVSClient(kvsCoordinator);
        		Iterator<Row> rows = kvs.scan(inputTable1, fromKey, toKeyExclusive);
        		
        		String accumulator = zeroElement;
        		
        		while(rows.hasNext()) {
        			//get row
        			Row r = rows.next();
        			//System.out.println("foldByKey r.key(): " + r.key());

        			//iterate over all columns and call lambda for each value(with current accumulator and value as args) = new accumulator
        			for(String col : r.columns()) {
        				//System.out.println("foldByKey r.get(col): " + r.get(col));
        				accumulator = lambda.op(accumulator, r.get(col));
        			}
        		}
        		response.body(accumulator);
        		return accumulator;
        	}catch(Exception e) {
        		response.status(500, "Error in rdd/fold on Worker");
        		return e.getStackTrace();
        	}
        	
        	//return accumulator value or zeroElement if nothing in the rows for this worker;
        	    	
        });
        

        // endpoint 4 sampling rows
        Server.post("/sample", (request, response) -> {
            // get params for input/output tables, range, and fraction
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            double fraction = Double.parseDouble(request.queryParams("fraction"));

            // log request details
            System.out.println("Processing sample request with fraction: " + fraction);

            // setup kvs client and random gen
            KVSClient kvs = new KVSClient(kvsCoordinator);
            Random random = new Random();

            // scan rows from the input table
            Iterator<Row> rows = kvs.scan(inputTable, fromKey, toKeyExclusive);

            // iterate thru rows and randomly sample
            while (rows.hasNext()) {
                Row row = rows.next();
                if (random.nextDouble() < fraction) {
                    kvs.put(outputTable, row.key(), "value", row.get("value"));
                    System.out.println("sample: wrote key: " + row.key());
                }
            }

            // log completion
            System.out.println("sample completed for table: " + inputTable);
            return "OK";
        });

        // endpoint 4 grouping rows by a key
        Server.post("/groupBy", (request, response) -> {
            // get params for input/output tables and range
            String inputTable = request.queryParams("inputTable");
            String outputTable = request.queryParams("outputTable");
            String kvsCoordinator = request.queryParams("kvsCoordinator");
            String fromKey = request.queryParams("fromKey");
            String toKeyExclusive = request.queryParams("toKeyExclusive");
            String tempTable = request.queryParams("tempTable");

            System.out.println("Processing groupBy request...");

            // deserialize grouping function
            FlameRDD.StringToString grouper = (FlameRDD.StringToString) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // setup kvs client and scan rows
            KVSClient kvs = new KVSClient(kvsCoordinator);
            Iterator<Row> rows = kvs.scan(inputTable, fromKey, toKeyExclusive);

            // group rows by computed groupKey
            while (rows.hasNext()) {
                Row row = rows.next();
                String groupKey = grouper.op(row.get("value"));
                kvs.put(tempTable, groupKey, row.get("value"), "1");
                System.out.println("groupBy: grouped key: " + row.key() + " under groupKey: " + groupKey);
            }

            // consolidate grouped rows
            Iterator<Row> groupedRows = kvs.scan(tempTable, fromKey, toKeyExclusive);

            while (groupedRows.hasNext()) {
                Row row = groupedRows.next();
                String groupedValues = "";

                for (String column : row.columns()) {
                    groupedValues += (groupedValues.isEmpty() ? "" : ",") + column;
                }

                kvs.put(outputTable, row.key(), "value", groupedValues);
                System.out.println("groupBy: wrote grouped values for key: " + row.key());
            }

            System.out.println("groupBy completed.");
            return "OK";
        });

        // endpoint 4 transforming rows from table
        Server.post("/fromTable", (request, response) -> {
            // deserialize rowMapper func from request
            FlameContext.RowToString rowMapper = (FlameContext.RowToString) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // log request handling
            System.out.println("Processing fromTable request...");

            // execute fromTable transformation
            return perform("fromTable", request, (row, outputTable, inputTable2, kvs) -> {
                String mappedValue = rowMapper.op(row);

                if (mappedValue != null) {
                    kvs.put(outputTable, row.key(), "value", mappedValue.getBytes());
                    log.debug("fromTable: transformed key: " + row.key() + " to value: " + mappedValue);
                }

                return 1;
            });
        });

    // endpoint  flatMap to key-value pairs
        Server.post("/flatMapToPair", (request, response) -> {
            // deserialize pairMapper func
            FlameRDD.StringToPairIterable pairMapper = (FlameRDD.StringToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // log request handling
            System.out.println("Processing flatMapToPair request...");

            // execute flatMapToPair transformation
            return perform("flatMapToPair", request, (row, outputTable, inputTable2, kvs) -> {
                Iterator<FlamePair> pairs = pairMapper.op(row.get("value")).iterator();
                int count = 0;

                // iterate thru pairs and store in kvs
                while (pairs.hasNext()) {
                    FlamePair pair = pairs.next();
                    kvs.put(outputTable, pair._1(), row.key() + "-" + count++, pair._2().getBytes());
//                    System.out.println("flatMapToPair: wrote pair: (" + pair._1() + ", " + pair._2() + ")");
                }

                return count;
            });
        });

        // endpoint mapping key-value pairs
        Server.post("/pairFlatMapToPair", (request, response) -> {
            // deserialize pairMapper func
            FlamePairRDD.PairToPairIterable pairMapper = (FlamePairRDD.PairToPairIterable) Serializer.byteArrayToObject(request.bodyAsBytes(), jarFile);

            // log request handling
            System.out.println("Processing pairFlatMapToPair request...");

            // execute pairFlatMapToPair transformation
            return perform("pairFlatMapToPair", request, (row, outputTable, inputTable2, kvs) -> {
                int count = 0;

                // iterate over row columns
                for (String column : row.columns()) {
                    Iterator<FlamePair> pairs = pairMapper.op(new FlamePair(row.key(), row.get(column))).iterator();

                    // write each pair to kvs
                    while (pairs.hasNext()) {
                        FlamePair pair = pairs.next();
                        log.debug("pairFlatMapToPair rowkey: " + row.key() +  " /n pair: " + pair._1());
                        kvs.put(outputTable, pair._1(), row.key() + "-" + count++, pair._2().getBytes());
//                        System.out.println("pairFlatMapToPair: wrote pair: (" + pair._1() + ", " + pair._2() + ")");
                    }
                }

                return count;
            });
        });

        // endpoint version info
        Server.get("/version", (request, response) -> {
            System.out.println("Returning version info.");
            return "v1.3a Nov 10 2022";
        });


    }
    // log msg to stdout
    static void log(String message) {
        // print log msg
//        System.out.println(message);
    }
}

package cis5550.flame;

//package imports
import cis5550.kvs.KVSClient;

//tools
import cis5550.tools.HTTP;
import cis5550.tools.Hasher;
import cis5550.tools.Logger;
import cis5550.tools.Partitioner;
import cis5550.tools.Serializer;

//i/o management
import java.io.IOException;
import java.io.Serializable;

//data processing and structures
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

// class for flame context funcs
public class FlameContextImpl implements FlameContext, Serializable {

    private static final Logger logger = Logger.getLogger(FlameContextImpl.class);

    // next rdd id counter
    int nextRddId = 1;

    // job identifiers
    String jobId;
    String jobName;
    String jobOutput;

    // num of partitions 4 parallel ops
    int partitionCount;

    // invoke specific flame op (e.g., map/filter)
    int invokeOperation(String operationName, final byte[] payload) throws Exception {
        // gen new rdd id 4 result
        int rddId = this.nextRddId++;

        // init partitioner 4 data splits
        Partitioner partitioner = new Partitioner();

        // set num of partitions per worker
        partitioner.setKeyRangesPerWorker(this.partitionCount);

        // add kvs workers to partitioner
        partitioner.addKVSWorker(this.getKVS().getWorkerAddress(this.getKVS().numWorkers() - 1), null, this.getKVS().getWorkerID(0));

        // loop thru workers, set worker ranges
        for (int workerIndex = 0; workerIndex < this.getKVS().numWorkers() - 1; ++workerIndex) {
            partitioner.addKVSWorker(
                    this.getKVS().getWorkerAddress(workerIndex),
                    this.getKVS().getWorkerID(workerIndex),
                    this.getKVS().getWorkerID(workerIndex + 1)
            );
        }

        // add last kvs worker
        partitioner.addKVSWorker(this.getKVS().getWorkerAddress(this.getKVS().numWorkers() - 1), this.getKVS().getWorkerID(this.getKVS().numWorkers() - 1), null);

        // get flame workers from coordinator
        Iterator<String> workersIterator = Coordinator.getWorkers().iterator();
        while (workersIterator.hasNext()) {
            // add flame worker 2 partitioner
            String workerAddress = workersIterator.next();
            partitioner.addFlameWorker(workerAddress);
        }

        // assign partitions 2 workers
        Vector<Partitioner.Partition> partitions = partitioner.assignPartitions();

        // setup threads 4 parallel requests
        Thread[] requestThreads = new Thread[partitions.size()];
        final String[] responses = new String[partitions.size()];

        // create threads 4 each partition
        for (int partitionIndex = 0; partitionIndex < partitions.size(); ++partitionIndex) {
            // gen worker url w/ params
            String workerUrl = "http://" + partitions.elementAt(partitionIndex).assignedFlameWorker + "/" + operationName;
            workerUrl += workerUrl.contains("?") ? "&" : "?";
            workerUrl += "kvsCoordinator=" + this.getKVS().getCoordinator();
            workerUrl += "&outputTable=" + this.tableName(rddId);
            workerUrl += "&part=" + partitionIndex;

            // add optional keys 2 url
            if (partitions.elementAt(partitionIndex).fromKey != null) {
                workerUrl += "&fromKey=" + partitions.elementAt(partitionIndex).fromKey;
            }
            if (partitions.elementAt(partitionIndex).toKeyExclusive != null) {
                workerUrl += "&toKeyExclusive=" + partitions.elementAt(partitionIndex).toKeyExclusive;
            }

            // log worker url
            System.out.println("Sending request to worker: " + workerUrl);

            // store url & index 4 thread run
            final String requestUrl = workerUrl;
            final int partitionIdx = partitionIndex;

            // create thread 4 request
            requestThreads[partitionIndex] = new Thread("Request #" + (partitionIndex + 1)) {
                public void run() {
                    try {
                        // send req, save response
                        responses[partitionIdx] = new String(HTTP.doRequest("POST", requestUrl, payload).body());
                    } catch (Exception ex) {
                        // catch errors in thread
                        ex.printStackTrace();
                        responses[partitionIdx] = "Exception: " + ex.getMessage();
                    }
                }
            };

            // start thread
            requestThreads[partitionIndex].start();
        }

        // wait 4 threads 2 finish
        for (Thread thread : requestThreads) {
            try {
                thread.join();
            } catch (InterruptedException ignored) {}
        }

        // count failed jobs
        int failedJobs = 0;

        // check responses 4 errors
        for (int responseIndex = 0; responseIndex < partitions.size(); ++responseIndex) {
            if (!responses[responseIndex].startsWith("OK") && !(operationName.contains("fold"))) {
                // log error 4 failed job
                logger.error(
                        "Job #" + (responseIndex + 1) + "/" + partitions.size() + " (" + operationName + ") failed with a " + responses[responseIndex]
                );
                ++failedJobs;
            }
        }

        // throw err if any jobs failed
        if (failedJobs > 0) {
            throw new Exception(failedJobs + " job(s) failed");
        }

        // log success
        logger.debug("Operation completed and returned as RDD #" + rddId);
        return rddId;
    }

    // get table name 4 rdd
    public String tableName(int tableId) {
        return this.jobId + "." + tableId;
    }

    // get kvs client instance
    public KVSClient getKVS() {
        return Coordinator.kvsClient;
    }

    // ctor 4 flame context impl
    public FlameContextImpl(String jobName) {
        // init job name/id/output
        this.jobName = jobName;
        this.jobId = jobName + "-" + System.currentTimeMillis();
        this.jobOutput = "";

        // default partition count
        this.partitionCount = 1;
    }

    // append output 2 job result
    public void output(String outputData) {
        this.jobOutput += outputData;
        Coordinator.jobOutputs.put(this.jobName, this.jobOutput);
    }

    // create rdd from list of strings
    public FlameRDD parallelize(List<String> data) throws IOException {
        // create new rdd impl
        FlameRDDImpl newRDD = new FlameRDDImpl(this, this.nextRddId++);
        int dataIndex = 0;

        // put data into kvs
        for (String record : data) {
            this.getKVS().put(newRDD.rddTableName, Hasher.hash(String.valueOf(dataIndex++)), "value", record.getBytes());
        }

        // return new rdd
        return newRDD;
    }

    // create rdd from kvs table rows
    public FlameRDD fromTable(String tableName, FlameContext.RowToString rowMapper) throws Exception {
        // invoke op w/ table name + mapper
        return new FlameRDDImpl(this, this.invokeOperation("fromTable?inputTable=" + tableName, Serializer.objectToByteArray(rowMapper)));
    }

    // set num of concurrent tasks
    public void setConcurrencyLevel(int concurrencyLevel) {
        this.partitionCount = concurrencyLevel;
    }
}

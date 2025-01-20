package cis5550.flame;

import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

// impl of flame pair rdd, handles rdd ops 4 (k, v) pairs
public class FlamePairRDDImpl implements FlamePairRDD {
    // flame context reference
    FlameContextImpl context;

    // table name for rdd data
    String tableName;

    // sequence id for rdd
    int sequenceId;

    private static final Logger logger = Logger.getLogger(FlamePairRDDImpl.class);

    FlamePairRDDImpl(FlameContextImpl context, int sequenceId) {
        // set context ref
        this.context = context;

        // set sequence id
        this.sequenceId = sequenceId;

        // gen table name based on sequence id
        this.tableName = this.context.tableName(sequenceId);
        logger.info("created FlamePairRDD: " + this.tableName);
    }

    // collect all elems in rdd as list
    public List<FlamePair> collect() throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD was destroyed and can't be used");
        } else {
            // init result list
            LinkedList<FlamePair> resultList = new LinkedList<>();

            // get row iterator from kvs
            Iterator<Row> rowIterator = this.context.getKVS().scan(this.tableName, null, null);

            // loop through rows
            while (rowIterator.hasNext()) {
                Row currentRow = rowIterator.next();
                Iterator<String> columnIterator = currentRow.columns().iterator();

                // loop thru columns in row
                while (columnIterator.hasNext()) {
                    String columnKey = columnIterator.next();

                    // add pair 2 result list
                    resultList.add(new FlamePair(currentRow.key(), currentRow.get(columnKey)));
                }
            }
            logger.info("collected pairs from RDD: " + this.tableName);
            return resultList;
        }
    }

    // fold rdd by key w/ func
    public FlamePairRDD foldByKey(String zeroValue, FlamePairRDD.TwoStringsToString function) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // build operation url
            String operationUrl = "foldByKey?inputTable=" + this.tableName + "&zero=" + URLEncoder.encode(zeroValue, "UTF-8");

            // serialize fold func
            byte[] serializedFunction = Serializer.objectToByteArray(function);

            logger.info("performing foldByKey on RDD: " + this.tableName);
            return new FlamePairRDDImpl(this.context, this.context.invokeOperation(operationUrl, serializedFunction));
        }
    }

    // save rdd as new table
    public void saveAsTable(String newTableName) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // rename table in kvs
            this.context.getKVS().rename(this.tableName, newTableName);

            // update rdd table name
            this.tableName = newTableName;
            logger.info("RDD saved as table: " + newTableName);
        }
    }

    // destroy rdd, delete table
    public void destroy() throws Exception {
        // delete table from kvs
        this.context.getKVS().delete(this.tableName);

        // nullify table name
        this.tableName = null;
        logger.info("RDD destroyed");
    }

    // map each (k, v) pair 2 strings
    public FlameRDD flatMap(FlamePairRDD.PairToStringIterable function) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("This RDD was destroyed and can no longer be used");
        } else {
            // build operation url
            String operationUrl = "pairFlatMap?inputTable=" + this.tableName;

            // serialize map func
            byte[] serializedFunction = Serializer.objectToByteArray(function);
            logger.info("performing flatMap on RDD: " + this.tableName);
            return new FlameRDDImpl(this.context, this.context.invokeOperation(operationUrl, serializedFunction));
        }
    }

    // map each (k, v) pair 2 (k', v') pairs
    public FlamePairRDD flatMapToPair(FlamePairRDD.PairToPairIterable function) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // build operation url
            String operationUrl = "pairFlatMapToPair?inputTable=" + this.tableName;

            // serialize map func
            byte[] serializedFunction = Serializer.objectToByteArray(function);
            logger.info("performing flatMapToPair on RDD: " + this.tableName);
            return new FlamePairRDDImpl(this.context, this.context.invokeOperation(operationUrl, serializedFunction));
        }
    }

    // join 2 rdds on keys
    public FlamePairRDD join(FlamePairRDD otherRDD) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // build join url
            String operationUrl = "join?inputTable=" + this.tableName + "&inputTable2=" + ((FlamePairRDDImpl) otherRDD).tableName;
            logger.info("koining RDDs: " + this.tableName + " and " + ((FlamePairRDDImpl) otherRDD).tableName);
            logger.debug("join url: " + operationUrl);
            return new FlamePairRDDImpl(this.context, this.context.invokeOperation(operationUrl, null));
        }
    }

    // cogroup 2 rdds (group vals w/ same key)
    public FlamePairRDD cogroup(FlamePairRDD otherRDD) throws Exception {
        // chk if rdd destroyed
        if (this.tableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // build cogroup url
            String operationUrl = "cogroup?inputTable=" + this.tableName + "&inputTable2=" + ((FlamePairRDDImpl) otherRDD).tableName;
            logger.info("cogrouping RDDs: " + this.tableName + " and " + ((FlamePairRDDImpl) otherRDD).tableName);
            return new FlamePairRDDImpl(this.context, this.context.invokeOperation(operationUrl, null));
        }
    }
}
package cis5550.flame;

import cis5550.kvs.Row;
import cis5550.tools.Logger;
import cis5550.tools.Serializer;

import java.io.Serializable;
import java.net.URLEncoder;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Vector;

// impl of flame rdd, supports distributed dataset ops
public class FlameRDDImpl implements FlameRDD {
    // context for flame ops
    FlameContextImpl flameContext;

    // table name for rdd storage
    String rddTableName;

    // seq num for unique rdd id
    int sequenceNumber;

    private static final Logger log = Logger.getLogger(FlameRDDImpl.class);

    // ctor for rdd impl
    FlameRDDImpl(FlameContextImpl context, int sequenceNum) {
        // init context and seq num
        this.flameContext = context;
        this.sequenceNumber = sequenceNum;

        // gen table name for this rdd
        this.rddTableName = this.flameContext.tableName(sequenceNum);
        log.info("created FlameRDD: " + this.rddTableName);
    }

    // flatmap each rdd elem to multiple strings
    public FlameRDD flatMap(FlameRDD.StringToIterable mapperFunction) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("executing flatMap on RDD: " + this.rddTableName);
            // invoke flatmap op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("flatMap?inputTable=" + this.rddTableName, Serializer.objectToByteArray(mapperFunction)));
        }
    }

    // map each elem to a (k, v) pair
    public FlamePairRDD mapToPair(FlameRDD.StringToPair pairMapper) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // log mapToPair op
            log.debug("performing mapToPair on RDD: " + this.rddTableName);
            // invoke mapToPair op via flame
            return new FlamePairRDDImpl(this.flameContext, this.flameContext.invokeOperation("mapToPair?inputTable=" + this.rddTableName, Serializer.objectToByteArray(pairMapper)));
        }
    }

    // collect all rdd elems as a list
    public List<String> collect() throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // init list to store collected elems
            LinkedList<String> collectedValues = new LinkedList<>();

            // scan rdd table rows
            Iterator<Row> rowIterator = this.flameContext.getKVS().scan(this.rddTableName, null, null);

            // loop thru rows, add values to list
            while (rowIterator.hasNext()) {
                collectedValues.add(rowIterator.next().get("value"));
            }

            // log collect op
            log.info("collected elements from RDD: " + this.rddTableName);
            return collectedValues;
        }
    }

    // count num of elems in rdd
    public int count() throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("counting elements in RDD: " + this.rddTableName);
            // return count from kvs
            return this.flameContext.getKVS().count(this.rddTableName);
        }
    }

    // destroy rdd, delete its table
    public void destroy() throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // delete table from kvs
            this.flameContext.getKVS().delete(this.rddTableName);

            // nullify table name
            this.rddTableName = null;
            log.info("destroyed RDD");
        }
    }

    // save rdd to a new table
    public void saveAsTable(String targetTableName) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // rename table in kvs
            this.flameContext.getKVS().rename(this.rddTableName, targetTableName);

            // update table name
            this.rddTableName = targetTableName;
            log.info("saved RDD as table: " + targetTableName);
        }
    }

    // take first n elems from rdd
    public Vector<String> take(int numElements) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // init vector for result
            Vector<String> result = new Vector<>();

            // scan rdd table rows
            Iterator<Row> rowIterator = this.flameContext.getKVS().scan(this.rddTableName, null, null);

            // loop until desired num elems taken
            while (rowIterator.hasNext() && result.size() < numElements) {
                result.add(rowIterator.next().get("value"));
            }

            log.info("took " + result.size() + " elements from RDD: " + this.rddTableName);
            return result;
        }
    }

    // get distinct elems in rdd
    public FlameRDD distinct() throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing distinct on RDD: " + this.rddTableName);
            // invoke distinct op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("distinct?inputTable=" + this.rddTableName, null));
        }
    }

    // fold rdd elems into single val
    public String fold(String initialValue, FlamePairRDD.TwoStringsToString foldingFunction) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            // invoke fold op via flame
            int intermediateTableId = this.flameContext.invokeOperation("fold?inputTable=" + this.rddTableName + "&zero=" + URLEncoder.encode(initialValue, "UTF-8"), Serializer.objectToByteArray(foldingFunction));

            // scan intermediate table for result
            Iterator<Row> rowIterator = this.flameContext.getKVS().scan(this.flameContext.tableName(intermediateTableId), null, null);

            // combine values with fold func
            String foldedResult = initialValue;
            while (rowIterator.hasNext()) {
                foldedResult = foldingFunction.op(foldedResult, rowIterator.next().get("value"));
            }

            log.info("fold result: " + foldedResult);
            return foldedResult;
        }
    }


    // map each elem 2 multiple (k, v) pairs
    public FlamePairRDD flatMapToPair(FlameRDD.StringToPairIterable mapperFunction) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {

            log.debug("performing flatMapToPair on RDD: " + this.rddTableName);
            // invoke flatMapToPair op via flame
            return new FlamePairRDDImpl(this.flameContext, this.flameContext.invokeOperation("flatMapToPair?inputTable=" + this.rddTableName, Serializer.objectToByteArray(mapperFunction)));
        }
    }

    // get intersection of this rdd w/ another
    public FlameRDD intersection(FlameRDD otherRDD) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing intersection between RDDs: " + this.rddTableName + " and " +
                    ((FlameRDDImpl) otherRDD).rddTableName);

            // invoke intersection op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("intersection?tempTable=temp-" + System.currentTimeMillis() + "&inputTable1=" + this.rddTableName + "&inputTable2=" + ((FlameRDDImpl) otherRDD).rddTableName, null));
        }
    }

    // sample rdd w/ given fraction
    public FlameRDD sample(double fraction) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing sampling on RDD: " + this.rddTableName + " with frac: " + fraction);
            // invoke sample op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("sample?inputTable=" + this.rddTableName + "&fraction=" + fraction, null));
        }
    }

    // group elems in rdd by key
    public FlamePairRDD groupBy(FlameRDD.StringToString groupingFunction) throws Exception {
        // chk if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing groupBy on RDD: " + this.rddTableName);
            // invoke groupBy op via flame
            return new FlamePairRDDImpl(this.flameContext, this.flameContext.invokeOperation("groupBy?tempTable=temp-" + System.currentTimeMillis() + "&inputTable=" + this.rddTableName, Serializer.objectToByteArray(groupingFunction)));
        }
    }

    // filter rdd elems by predicate func
    public FlameRDD filter(FlameRDD.StringToBoolean filterFunction) throws Exception {
        // check if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing filter on RDD: " + this.rddTableName);
            // invoke filter op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("filter?inputTable=" + this.rddTableName, Serializer.objectToByteArray(filterFunction)));
        }
    }

    // apply func to rdd partitions
    public FlameRDD mapPartitions(FlameRDD.IteratorToIterator partitionMapper) throws Exception {
        // check if rdd destroyed
        if (this.rddTableName == null) {
            throw new Exception("RDD DESTROYED");
        } else {
            log.debug("performing mapPartitions on RDD: " + this.rddTableName);
            // invoke mapPartitions op via flame
            return new FlameRDDImpl(this.flameContext, this.flameContext.invokeOperation("mapPartitions?inputTable=" + this.rddTableName, Serializer.objectToByteArray(partitionMapper)));
        }
    }

    // override toString for debug info
    public String toString() {
        log.debug("converting RDD to string: " + this.rddTableName);
        return "FlameRDD<" + this.sequenceNumber + "/" + this.rddTableName + ">";
    }

    // interface for partition to iterable mapping
    public interface IteratorToIterable extends Serializable {
        // apply func to input iterator
        Iterable<String> op(Iterator<String> inputIterator) throws Exception;
    }
}

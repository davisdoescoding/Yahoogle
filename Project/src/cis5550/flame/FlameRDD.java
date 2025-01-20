package cis5550.flame;

import java.io.Serializable;
import java.util.Iterator;
import java.util.List;
import java.util.Vector;

// interface  flame rdd, base abstraction  distributed datasets
public interface FlameRDD {

  // count num of elems in rdd
  int count() throws Exception;

  // save rdd data as kvs table
  void saveAsTable(String var1) throws Exception;

  // get distinct elems from rdd
  FlameRDD distinct() throws Exception;

  // destroy rdd, free resources
  void destroy() throws Exception;

  // take first n elems from rdd
  Vector<String> take(int var1) throws Exception;

  // combine elems w/ fold func
  String fold(String var1, FlamePairRDD.TwoStringsToString var2) throws Exception;

  // collect all elems from rdd
  List<String> collect() throws Exception;

  // map each elem 2 multiple strings
  FlameRDD flatMap(StringToIterable var1) throws Exception;

  // map each elem 2 multiple pairs (k, v)
  FlamePairRDD flatMapToPair(StringToPairIterable var1) throws Exception;

  // map each elem 2 a single pair (k, v)
  FlamePairRDD mapToPair(StringToPair var1) throws Exception;

  // get intersection of 2 rdds
  FlameRDD intersection(FlameRDD var1) throws Exception;

  // sample rdd w/ given probability
  FlameRDD sample(double var1) throws Exception;

  // group rdd elems by key
  FlamePairRDD groupBy(StringToString var1) throws Exception;

  // filter elems by predicate func
  FlameRDD filter(StringToBoolean var1) throws Exception;

  // apply func 2 partitions of rdd
  FlameRDD mapPartitions(IteratorToIterator var1) throws Exception;

  // func  mapping partition 2 another partition
  public interface IteratorToIterator extends Serializable {
    // apply func 2 partition iter
    Iterator<String> op(Iterator<String> var1) throws Exception;
  }

  // func  filtering elems
  public interface StringToBoolean extends Serializable {
    // apply predicate func 2 elem
    boolean op(String var1) throws Exception;
  }

  // func  mapping string 2 another string
  public interface StringToString extends Serializable {
    // apply mapping func 2 elem
    String op(String var1) throws Exception;
  }

  // func  mapping string 2 iterable pairs
  public interface StringToPairIterable extends Serializable {
    // apply mapping func 2 elem
    Iterable<FlamePair> op(String var1) throws Exception;
  }

  // func  mapping string 2 a single pair
  public interface StringToPair extends Serializable {
    // apply mapping func 2 elem
    FlamePair op(String var1) throws Exception;
  }

  // func  mapping string 2 iterable strings
  public interface StringToIterable extends Serializable {
    // apply mapping func 2 elem
    Iterable<String> op(String var1) throws Exception;
  }
}

package cis5550.flame;

import java.io.Serializable;
import java.util.List;

// interface 4 flame pair rdd, extends rdd ops 4 (k, v) pairs
public interface FlamePairRDD {

  // collect all elems from rdd
  List<FlamePair> collect() throws Exception;

  // combine vals by key w/ fold func
  FlamePairRDD foldByKey(String var1, TwoStringsToString var2) throws Exception;

  // save rdd as table in kvs
  void saveAsTable(String var1) throws Exception;

  // map each (k, v) pair 2 multiple strings
  FlameRDD flatMap(PairToStringIterable var1) throws Exception;

  // destroy rdd, free resources
  void destroy() throws Exception;

  // map each (k, v) pair 2 multiple (k', v') pairs
  FlamePairRDD flatMapToPair(PairToPairIterable var1) throws Exception;

  // join two pair rdds on keys
  FlamePairRDD join(FlamePairRDD var1) throws Exception;

  // cogroup two pair rdds (group vals w/ same key)
  FlamePairRDD cogroup(FlamePairRDD var1) throws Exception;

  // func pair 2 iterable<string> conv
  public interface PairToStringIterable extends Serializable {
    // define pair-to-strings op
    Iterable<String> op(FlamePair var1) throws Exception;
  }

  // func pair 2 iterable<pair> conv
  public interface PairToPairIterable extends Serializable {
    // define pair-to-pairs op
    Iterable<FlamePair> op(FlamePair var1) throws Exception;
  }

  // func combining 2 strings into 1
  public interface TwoStringsToString extends Serializable {
    // define string-fold op
    String op(String var1, String var2);
  }
}

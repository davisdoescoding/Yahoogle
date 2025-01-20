package cis5550.flame;

import cis5550.kvs.KVSClient;
import cis5550.kvs.Row;
import java.io.Serializable;
import java.util.List;

// interface for flame context
public interface FlameContext {

  // get kvs client instance
  KVSClient getKVS();

  // write output to some target (stdout/file/etc.)
  void output(String target);

  // create rdd from list of strings
  FlameRDD parallelize(List<String> stringsList) throws Exception;

  // create rdd from table rows, conv rows 2 strings
  FlameRDD fromTable(String tableName, RowToString rowConverter) throws Exception;

  // set concurrency level 4 flame jobs
  void setConcurrencyLevel(int level);

  // func interface 4 row->string conv
  public interface RowToString extends Serializable {
    // conv single row to string
    String op(Row var1);
  }
}

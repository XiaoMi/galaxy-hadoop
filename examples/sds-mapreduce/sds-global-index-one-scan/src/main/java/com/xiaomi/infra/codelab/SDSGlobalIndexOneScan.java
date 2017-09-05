package com.xiaomi.infra.codelab;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSConfiguration;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapper;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSReducer;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutput;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableProperty;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableScan;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/**
 * Created by niuyulin on 09/05/17.
 * This example indicate how to use mapreduce on Galaxy SDS, with one scan on global index table.
 * Input: a Galaxy SDS table with sales quantities of various products :)
 *      [entity group (hash salt enabled)]
 *        key : productId
 *      [primary key]
 *        key : timestamp
 *      [attribute(s)]
 *        key : counter (sales quantity)
 *
 *      [global index]
 *          [index entity group (hash salt enabled)]
 *             key : timestamp
 *          [index primary key]
 *             key : productId
 *          [index projections]
 *             key : counter
 *
 * Output: a Galaxy SDS table with total sales quantities of various products
 *      [primary key]
 *        key : productId
 *      [attribute(s)]
 *        key : counter (total sales quantity)
 */
public class SDSGlobalIndexOneScan extends Configured implements Tool {
  public static class SalesCountMapper
      extends SDSMapper<Text, IntWritable> {
    @Override
    protected void map(Object key, SDSRecordWritable record, Context context)
        throws IOException, InterruptedException {
      Map<String, Object> objectMap = DatumUtil.fromDatum(record.getRecord());
      String productId = (String) objectMap.get("productId");
      Integer counter = (Integer) objectMap.get("counter");
      context.write(new Text(productId), new IntWritable(counter));
    }
  }

  public static class SalesCountReducer
      extends SDSReducer<Text, IntWritable> {
    @Override
    protected void reduce(Text key, Iterable<IntWritable> values, Context context)
        throws IOException, InterruptedException {
      int sum = 0;
      for (IntWritable val : values) {
        sum += val.get();
      }

      Map<String, Datum> record = new HashMap<String, Datum>();
      record.put("productId", DatumUtil.toDatum(key.toString()));
      record.put("counter", DatumUtil.toDatum(sum));
      context.write(NullWritable.get(), new SDSRecordWritable(record));
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = SDSConfiguration.create(getConf());

    if (args.length != 3) {
      System.err.println("Usage: program [-Dsds.mapreduce.rest.endpoint=endpoint] " +
          "[-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] <input table> <index-name> <output table>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "open-computing-codelab-sds-global-index-one-scan");

    String inputTable = args[0];
    String indexTable = args[1];
    String outputTable = args[2];

    ScanRequest scanRequest = new ScanRequest()
        .setTableName(inputTable)
        .setIndexName(indexTable)
        .setLimit(1000)
        .setCacheResult(false);
    SDSTableProperty sdsProperty = new SDSTableProperty();
    SDSTableScan scan = new SDSTableScan(scanRequest, sdsProperty);

    SDSMapReduceUtil.initSDSTableMapperJob(scan, SalesCountMapper.class,
        Text.class, IntWritable.class, job);

    SDSTableOutput tableOutput = new SDSTableOutput(outputTable);
    SDSMapReduceUtil.initSDSTableReducerJob(tableOutput, SalesCountReducer.class,
        job);

    job.setNumReduceTasks(5);
    job.setJarByClass(SDSGlobalIndexOneScan.class);
    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SDSGlobalIndexOneScan(), args);
    System.exit(exitCode);
  }
}

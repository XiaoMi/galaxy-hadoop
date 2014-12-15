package com.xiaomi.infra.galaxy.hadoop.example;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xiaomi.infra.galaxy.hadoop.SDSConfiguration;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.SDSProperty;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.SDSRecord;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.TableMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.TableMapper;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.TableOutput;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.TableReducer;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.TableScan;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;

public class SDSTest extends Configured implements Tool {
  public static class CountMapper
      extends TableMapper<Text, IntWritable> {

    @Override
    protected void map(Object key, SDSRecord record, Context context)
        throws IOException, InterruptedException {
      Map<String, Object> objectMap = DatumUtil.fromDatum(record.getValues());
      String mapKey = (String) objectMap.get("productId");
      Integer quantity = (Integer) objectMap.get("counter");
      context.write(new Text(mapKey), new IntWritable(quantity));
    }
  }

  public static class CountReducer
      extends TableReducer<Text, IntWritable> {

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
      context.write(NullWritable.get(), record);
    }
  }

  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = SDSConfiguration.create(getConf());

    if (args.length != 2) {
      System.err.println("Usage: program [-D=sds.mapreduce.rest.endpoint=endpoint] " +
          "[-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] " +
          "[-Dsds.mapreduce.client.max.retry] <input table> <output table>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "codelab-SDS-to-SDS");

    String inputTable = args[0];
    String output = args[1];

    List <TableScan> scans = new ArrayList<TableScan>();

    SDSProperty sdsProperty = new SDSProperty();
//    sdsProperty.setEndpoint("endpoint")
//        .setSecretID("id")
//        .setSecretKey("key")
//        .setClientMaxRetry(maxRetry);

    Map<String, Datum> range1Key = new HashMap<String, Datum>();
    range1Key.put("productId", DatumUtil.toDatum("小米路由器"));
    ScanRequest scanRequest =
        new com.xiaomi.infra.galaxy.sds.thrift.ScanRequest()
            .setTableName(inputTable)
            .setStartKey(range1Key)
            .setStopKey(range1Key)
            .setLimit(1000)
            .setCacheResult(false);
    TableScan scan = new TableScan(scanRequest, sdsProperty);
    scans.add(scan);

    Map<String, Datum> range2StartKey = new HashMap<String, Datum>();
    range2StartKey.put("productId", DatumUtil.toDatum("mi_pad"));
    Map<String, Datum> range2StopKey = new HashMap<String, Datum>();
    range2StopKey.put("productId", DatumUtil.toDatum("小米路由器"));
    scanRequest =
        new com.xiaomi.infra.galaxy.sds.thrift.ScanRequest()
            .setTableName(inputTable)
            .setStartKey(range2StartKey)
            .setStopKey(range2StopKey)
            .setLimit(1000)
            .setCacheResult(false);
    scan = new TableScan(scanRequest);
    // Not set SDSProperty explicitly, will get from configuration
    scans.add(scan);

    TableMapReduceUtil.initTableMapperJob(scans, CountMapper.class,
        Text.class, IntWritable.class, job);

    TableOutput tableOutput = new TableOutput(output);
    TableMapReduceUtil.initTableReducerJob(tableOutput, CountReducer.class, job);
    job.setNumReduceTasks(2);

    job.setJarByClass(SDSTest.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SDSTest(), args);
    System.exit(exitCode);
  }
}

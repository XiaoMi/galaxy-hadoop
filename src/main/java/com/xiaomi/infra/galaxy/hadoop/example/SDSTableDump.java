package com.xiaomi.infra.galaxy.hadoop.example;

import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSConfiguration;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file.SDSFileOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableProperty;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableScan;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Dump SDS table data as SDS Log File (SLFile) to the specified path (can be any supported Hadoop
 * FileSystem, including HDFS, FDS or local filesystem)
 */
public class SDSTableDump extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = SDSConfiguration.create(getConf());

    if (args.length != 1) {
      System.err.println("Utility to dump SDS table as SDS Log File (SLFile)");
      System.err.println("Usage: program [-D=sds.mapreduce.rest.endpoint=endpoint] " +
                             "[-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] " +
                             "[-Dsds.mapreduce.client.max.retry] [-Dmapreduce.output.fileoutputformat.outputdir] "
                             +
                             "<input table>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "codelab-SDS-Dump");

    String inputTable = args[0];

    List<SDSTableScan> scans = new ArrayList<SDSTableScan>();

    SDSTableProperty sdsProperty = new SDSTableProperty();
    //    sdsProperty.setEndpoint("endpoint")
    //        .setSecretID("id")
    //        .setSecretKey("key")
    //        .setClientMaxRetry(maxRetry);

    ScanRequest scanRequest =
        new ScanRequest()
            .setTableName(inputTable)
            .setStartKey(null) // full table scan
            .setStopKey(null)
            .setLimit(1000)
            .setCacheResult(false);
    SDSTableScan scan = new SDSTableScan(scanRequest, sdsProperty);
    scans.add(scan);

    SDSMapReduceUtil.initSDSTableMapperJob(scans, RecordPipeMapper.class,
                                           NullWritable.class, SDSRecordWritable.class, job);
    job.setOutputFormatClass(SDSFileOutputFormat.class);

    job.setNumReduceTasks(0);

    job.setJarByClass(SDSTableDump.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SDSTableDump(), args);
    System.exit(exitCode);
  }
}

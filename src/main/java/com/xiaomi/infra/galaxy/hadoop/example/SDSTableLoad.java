package com.xiaomi.infra.galaxy.hadoop.example;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSConfiguration;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutput;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableProperty;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * Load SDS table data from SDS Log File (SLFile) in the specified path (can be any supported Hadoop
 * FileSystem, including HDFS, FDS or local filesystem)
 */
public class SDSTableLoad extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = SDSConfiguration.create(getConf());

    if (args.length != 1) {
      System.err.println("Utility to load SDS table from SDS Log File (SLFile)");
      System.err.println("Usage: program [-D=sds.mapreduce.rest.endpoint=endpoint] " +
                             "[-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] " +
                             "[-Dsds.mapreduce.client.max.retry] [-Dmapreduce.input.fileinputformat.inputdir] "
                             +
                             "<output table>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "codelab-SDS-Load");

    String outputTable = args[0];
    SDSTableProperty sdsProperty = new SDSTableProperty();
    //    sdsProperty.setEndpoint("endpoint")
    //        .setSecretID("id")
    //        .setSecretKey("key")
    //        .setClientMaxRetry(maxRetry);
    conf = job.getConfiguration();
    SDSTableOutput tableOutput = new SDSTableOutput(outputTable, sdsProperty);
    tableOutput.getSDSProperty().checkSanityAndSet(conf);
    conf.set(SDSTableOutputFormat.OUTPUT_TABLE,
             SDSMapReduceUtil.convertTableOuputToString(tableOutput));

    SDSMapReduceUtil.initSDSFileMapperJob(RecordPipeMapper.class,
                                          NullWritable.class, SDSRecordWritable.class, job);
    job.setOutputFormatClass(SDSTableOutputFormat.class);

    job.setNumReduceTasks(0);

    job.setJarByClass(SDSTableLoad.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SDSTableLoad(), args);
    System.exit(exitCode);
  }
}

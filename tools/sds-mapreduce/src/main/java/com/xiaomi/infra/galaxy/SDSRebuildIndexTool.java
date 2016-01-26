package com.xiaomi.infra.galaxy;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xiaomi.infra.galaxy.hadoop.example.RecordPipeMapper;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSConfiguration;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.index.SDSTableRebuildIndexOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutput;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableProperty;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableScan;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class SDSRebuildIndexTool extends Configured implements Tool {
  @Override
  public int run(String[] args) throws Exception {
    Configuration conf = SDSConfiguration.create(getConf());

    if (args.length != 1) {
      System.err.println("Utility to Rebuild SDS table index");
      System.err.println("Usage: program [-D=sds.mapreduce.rest.endpoint=endpoint] " +
          "[-Dsds.mapreduce.secret.id=id] [-Dsds.mapreduce.secret.key=key] " +
          "[-Dsds.mapreduce.client.max.retry]" +
          "<rebuilt table>");
      System.exit(2);
    }

    Job job = Job.getInstance(conf, "codelab-SDS-RebuildIndex");

    String table = args[0];
    SDSTableProperty sdsProperty = new SDSTableProperty();

    conf = job.getConfiguration();
    SDSTableOutput tableOutput = new SDSTableOutput(table, sdsProperty);
    tableOutput.getSDSProperty().checkSanityAndSet(conf);
    conf.set(SDSTableOutputFormat.OUTPUT_TABLE,
        SDSMapReduceUtil.convertTableOuputToString(tableOutput));

    List<SDSTableScan> scans = new ArrayList<SDSTableScan>();
    ScanRequest scanRequest =
        new ScanRequest()
            .setTableName(table)
            .setStartKey(null) // full table scan
            .setStopKey(null)
            .setLimit(10)
            .setCacheResult(false);
    SDSTableScan scan = new SDSTableScan(scanRequest, sdsProperty);
    scans.add(scan);

    SDSMapReduceUtil.initSDSTableMapperJob(scans, RecordPipeMapper.class, NullWritable.class,
        SDSRecordWritable.class, job);
    job.setOutputFormatClass(SDSTableRebuildIndexOutputFormat.class);

    job.setNumReduceTasks(0);

    job.setJarByClass(SDSRebuildIndexTool.class);

    return job.waitForCompletion(true) ? 0 : 1;
  }

  public static void main(String[] args) throws Exception {
    int exitCode = ToolRunner.run(new SDSRebuildIndexTool(), args);
    System.exit(exitCode);
  }
}

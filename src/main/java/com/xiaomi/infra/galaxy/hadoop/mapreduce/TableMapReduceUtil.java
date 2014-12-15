package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.hadoop.SDSConfiguration;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

public class TableMapReduceUtil {
  public static void initTableMapperJob(TableScan scan,
                                        Class<? extends TableMapper> mapper,
                                        Class<? extends WritableComparable> outputKeyClass,
                                        Class<? extends Writable> outputValueClass,
                                        Job job) throws IOException {
    List<TableScan> scans = new ArrayList<TableScan>();
    scans.add(scan);
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, true, job);
  }

  public static void initTableMapperJob(List<TableScan> scans,
                                        Class<? extends TableMapper> mapper,
                                        Class<? extends WritableComparable> outputKeyClass,
                                        Class<? extends Writable> outputValueClass,
                                        Job job) throws IOException {
    initTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, true, job);
  }

  public static void initTableMapperJob(List<TableScan> scans,
                                        Class<? extends TableMapper> mapper,
                                        Class<? extends WritableComparable> outputKeyClass,
                                        Class<? extends Writable> outputValueClass,
                                        boolean addDependencyJars, Job job) throws IOException {
    Configuration conf = job.getConfiguration();

    job.setInputFormatClass(GalaxySDSInputFormat.class);
    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);

    List<String> scanStrings = new ArrayList<String>();
    for (TableScan scan : scans) {
      // use default scan limit if not set
      if (!scan.getScanRequest().isSetLimit()) {
        scan.getScanRequest().setLimit(SDSConfiguration.DEFAULT_SDS_MAPREDUCE_SCAN_LIMIT);
      }

      scan.getSDSProperty().checkSanity(conf);
      scanStrings.add(convertTableScanToString(scan));
    }

    job.getConfiguration().setStrings(GalaxySDSInputFormat.SCANS,
        scanStrings.toArray(new String[scanStrings.size()]));

    if (addDependencyJars) {
      addDependencyJars(job);
    }
    // TODO: dump galaxy sds server configuration to support cross cluster
  }

  public static String convertTableScanToString(TableScan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      scan.write(dataOut);
    } finally {
      dataOut.close();
    }

    return Base64.encodeBase64String(out.toByteArray());
  }

  public static TableScan convertStringToTableScan(String scanStr) throws IOException {
    byte[] bytes = Base64.decodeBase64(scanStr);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    TableScan tableScan = new TableScan();
    try {
      tableScan.readFields(dataIn);
    } finally {
      dataIn.close();
    }

    return tableScan;
  }

  public static void initTableReducerJob(TableOutput tableOutput,
                                         Class<? extends TableReducer> reducer,
                                         Job job) throws IOException {
    initTableReducerJob(tableOutput, reducer, true, job);
  }

  public static void initTableReducerJob(TableOutput tableOutput,
                                         Class<? extends TableReducer> reducer,
                                         boolean addDependencyJars, Job job) throws IOException {
    Configuration conf = job.getConfiguration();
    tableOutput.getSDSProperty().checkSanity(conf);
    conf.set(GalaxySDSOutputFormat.OUTPUT_TABLE,
        convertTableOuputToString(tableOutput));

    job.setOutputFormatClass(GalaxySDSOutputFormat.class);
    job.setReducerClass(reducer);

    if (addDependencyJars) {
      addDependencyJars(job);
    }
  }

  public static String convertTableOuputToString(TableOutput tableOutput) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      tableOutput.write(dataOut);
    } finally {
      dataOut.close();
    }

    return Base64.encodeBase64String(out.toByteArray());
  }

  public static TableOutput convertStringToTableOutput(String scanStr) throws IOException {
    byte[] bytes = Base64.decodeBase64(scanStr);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    TableOutput tableOutput= new TableOutput();
    try {
      tableOutput.readFields(dataIn);
    } finally {
      dataIn.close();
    }

    return tableOutput;
  }

  private static void addDependencyJars(Job job) {
    // TODO: add dependecy jar automatically
  }
}

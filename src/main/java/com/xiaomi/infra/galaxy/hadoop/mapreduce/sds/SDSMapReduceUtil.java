package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file.SDSFileInputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableInputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutput;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableScan;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapreduce.Job;

public class SDSMapReduceUtil {
  public static void initSDSTableMapperJob(SDSTableScan scan,
                                           Class<? extends SDSMapper> mapper,
                                           Class<? extends WritableComparable> outputKeyClass,
                                           Class<? extends Writable> outputValueClass,
                                           Job job) throws IOException {
    List<SDSTableScan> scans = new ArrayList<SDSTableScan>();
    scans.add(scan);
    initSDSTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, true, job);
  }

  public static void initSDSTableMapperJob(List<SDSTableScan> scans,
                                           Class<? extends SDSMapper> mapper,
                                           Class<? extends WritableComparable> outputKeyClass,
                                           Class<? extends Writable> outputValueClass,
                                           Job job) throws IOException {
    initSDSTableMapperJob(scans, mapper, outputKeyClass, outputValueClass, true, job);
  }

  public static void setSDSTableInputConf(Configuration conf, SDSTableScan scan) throws IOException {
    List<SDSTableScan> scans = new ArrayList<SDSTableScan>();
    scans.add(scan);
    setSDSTableInputConf(conf, scans);
  }

  public static void setSDSTableInputConf(Configuration conf, List<SDSTableScan> scans) throws IOException {
    List<String> scanStrings = new ArrayList<String>();
    for (SDSTableScan scan : scans) {
      // use default scan limit if not set
      if (!scan.getScanRequest().isSetLimit()) {
        scan.getScanRequest().setLimit(SDSConfiguration.DEFAULT_SDS_MAPREDUCE_SCAN_LIMIT);
      }

      scan.getSDSProperty().checkSanityAndSet(conf);
      scanStrings.add(convertTableScanToString(scan));
    }

    conf.setStrings(SDSTableInputFormat.SCANS,
        scanStrings.toArray(new String[scanStrings.size()]));
  }

  public static void setSDSTableOutputConf(Configuration conf, SDSTableOutput tableOutput) throws IOException {
    tableOutput.getSDSProperty().checkSanityAndSet(conf);
    conf.set(SDSTableOutputFormat.OUTPUT_TABLE, SDSMapReduceUtil.convertTableOuputToString(tableOutput));
  }

  public static void initSDSTableMapperJob(List<SDSTableScan> scans,
                                           Class<? extends SDSMapper> mapper,
                                           Class<? extends WritableComparable> outputKeyClass,
                                           Class<? extends Writable> outputValueClass,
                                           boolean addDependencyJars, Job job) throws IOException {
    initMapperJob(mapper, outputKeyClass, outputValueClass, addDependencyJars, job);
    job.setInputFormatClass(SDSTableInputFormat.class);

    setSDSTableInputConf(job.getConfiguration(), scans);
    // TODO: dump galaxy sds server configuration to support cross cluster
  }

  public static void initSDSFileMapperJob(Class<? extends SDSMapper> mapper,
                                          Class<? extends WritableComparable> outputKeyClass,
                                          Class<? extends Writable> outputValueClass,
                                          Job job) throws IOException {
    initMapperJob(mapper, outputKeyClass, outputValueClass, true, job);
    job.setInputFormatClass(SDSFileInputFormat.class);
  }

  private static void initMapperJob(Class<? extends SDSMapper> mapper,
                                    Class<? extends WritableComparable> outputKeyClass,
                                    Class<? extends Writable> outputValueClass,
                                    boolean addDependencyJars, Job job) throws IOException {

    if (outputValueClass != null) {
      job.setMapOutputValueClass(outputValueClass);
    }
    if (outputKeyClass != null) {
      job.setMapOutputKeyClass(outputKeyClass);
    }
    job.setMapperClass(mapper);

    if (addDependencyJars) {
      addDependencyJars(job);
    }
  }

  public static String convertTableScanToString(SDSTableScan scan) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      scan.write(dataOut);
    } finally {
      dataOut.close();
    }

    return Base64.encodeBase64String(out.toByteArray());
  }

  public static SDSTableScan convertStringToTableScan(String scanStr) throws IOException {
    byte[] bytes = Base64.decodeBase64(scanStr);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    SDSTableScan tableScan = new SDSTableScan();
    try {
      tableScan.readFields(dataIn);
    } finally {
      dataIn.close();
    }

    return tableScan;
  }

  public static void initSDSTableReducerJob(SDSTableOutput tableOutput,
                                            Class<? extends SDSReducer> reducer,
                                            Job job) throws IOException {
    initSDSTableReducerJob(tableOutput, reducer, true, job);
  }

  public static void initSDSTableReducerJob(SDSTableOutput tableOutput,
                                            Class<? extends SDSReducer> reducer,
                                            boolean addDependencyJars, Job job) throws IOException {
    Configuration conf = job.getConfiguration();
    tableOutput.getSDSProperty().checkSanityAndSet(conf);
    conf.set(SDSTableOutputFormat.OUTPUT_TABLE,
             convertTableOuputToString(tableOutput));

    job.setOutputFormatClass(SDSTableOutputFormat.class);
    job.setReducerClass(reducer);

    if (addDependencyJars) {
      addDependencyJars(job);
    }
  }

  public static String convertTableOuputToString(SDSTableOutput tableOutput) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    DataOutputStream dataOut = new DataOutputStream(out);
    try {
      tableOutput.write(dataOut);
    } finally {
      dataOut.close();
    }

    return Base64.encodeBase64String(out.toByteArray());
  }

  public static SDSTableOutput convertStringToTableOutput(String scanStr) throws IOException {
    byte[] bytes = Base64.decodeBase64(scanStr);

    ByteArrayInputStream in = new ByteArrayInputStream(bytes);
    DataInputStream dataIn = new DataInputStream(in);
    SDSTableOutput tableOutput = new SDSTableOutput();
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

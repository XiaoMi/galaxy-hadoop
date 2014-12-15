package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import libthrift091.TException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.TableSplit;

public class GalaxySDSInputFormat extends InputFormat<NullWritable, SDSRecord>
    implements Configurable {
  public static String SCANS = "sds.mapreduce.input.scans";

  Configuration conf;
  List<TableScan> scans;

  public void setConf(Configuration conf) {
    this.conf = conf;

    String[] scanStrings = conf.getStrings(SCANS);
    if (scanStrings.length <= 0) {
      throw new IllegalArgumentException("There must be at least 1 scan configuration set to : " + SCANS);
    }

    scans = new ArrayList<TableScan>();
    for (String scanString : scanStrings) {
      try {
        TableScan scan = TableMapReduceUtil.convertStringToTableScan(scanString);
        scans.add(scan);
      } catch (IOException e) {
        throw new RuntimeException("Failed to convert TableScan string: " + scanString, e);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public List<InputSplit> getSplits(JobContext context) throws IOException {
    List<InputSplit> splits = new ArrayList<InputSplit>();
    for (TableScan scan : scans) {
      SDSProperty sdsProperty = scan.getSDSProperty();
      ScanRequest scanRequest = scan.getScanRequest();
      AdminService.Iface adminClient = sdsProperty.formAdminClient();

      try {
        List<TableSplit> tableSplits = adminClient.getTableSplits(scanRequest.getTableName(),
            scanRequest.getStartKey(), scanRequest.getStopKey());
        for (TableSplit tableSplit : tableSplits) {
          splits.add(new GalaxySDSSplit(scan, tableSplit.getStartKey(), tableSplit.getStopKey(), ""));
        }
      } catch (TException e) {
        throw new IOException("Get table splits failed", e);
      }
    }

    return splits;
  }

  @Override
  public RecordReader<NullWritable, SDSRecord> createRecordReader(InputSplit split,
      TaskAttemptContext context) throws IOException {
    return new GalaxySDSRecordReader();
  }
}

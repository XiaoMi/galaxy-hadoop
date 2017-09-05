package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GlobalSecondaryIndexSpec;
import com.xiaomi.infra.galaxy.sds.thrift.TableSpec;
import com.xiaomi.infra.galaxy.sds.thrift.TableSplit;
import libthrift091.TException;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SDSTableInputFormat extends InputFormat<NullWritable, SDSRecordWritable>
    implements Configurable {
  public static String SCANS = "sds.mapreduce.input.scans";

  Configuration conf;
  List<SDSTableScan> scans;

  public void setConf(Configuration conf) {
    this.conf = conf;

    String[] scanStrings = conf.getStrings(SCANS);
    if (scanStrings.length <= 0) {
      throw new IllegalArgumentException(
          "There must be at least 1 scan configuration set to : " + SCANS);
    }

    scans = new ArrayList<SDSTableScan>();
    for (String scanString : scanStrings) {
      try {
        SDSTableScan scan = SDSMapReduceUtil.convertStringToTableScan(scanString);
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
    for (SDSTableScan scan : scans) {
      SDSTableProperty sdsProperty = scan.getSDSProperty();
      ScanRequest scanRequest = scan.getScanRequest();
      AdminService.Iface adminClient = sdsProperty.formAdminClient();

      try {
        List<TableSplit> tableSplits;
          if(scanRequest.getIndexName()==null){
            tableSplits = adminClient.getTableSplits(scanRequest.getTableName(),
                    scanRequest.getStartKey(),
                    scanRequest.getStopKey());
          }else {
            String tableName = scanRequest.getTableName();
            String indexName = scanRequest.getIndexName();
            TableSpec tableSpec = adminClient.describeTable(tableName);
            Map<String, GlobalSecondaryIndexSpec> globalSecondaryIndexes = tableSpec.getSchema().getGlobalSecondaryIndexes();
            if (globalSecondaryIndexes == null || !globalSecondaryIndexes.containsKey(indexName)) {
               throw new IOException("Global Index Name is not exist; table name = " + tableName +
                                    "index name = " + indexName);
            }
            tableSplits = adminClient.getIndexTableSplits(tableName, indexName,
                    scanRequest.getStartKey(),
                    scanRequest.getStopKey());
          }

        for (TableSplit tableSplit : tableSplits) {
          splits
              .add(new SDSTableSplit(scan, tableSplit.getStartKey(), tableSplit.getStopKey(), ""));
        }
      } catch (TException e) {
        throw new IOException("Get table splits failed", e);
      }
    }

    return splits;
  }

  @Override
  public RecordReader<NullWritable, SDSRecordWritable> createRecordReader(InputSplit split,
                                                                          TaskAttemptContext context)
      throws IOException {
    return new SDSTableRecordReader();
  }
}

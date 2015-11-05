package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SDSTableRecordReader extends RecordReader<NullWritable, SDSRecordWritable> {
  private SDSRecordWritable value;
  private TableScanner scanner;
  private Iterator<Map<String, Datum>> iterator;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (!(split instanceof SDSTableSplit)) {
      throw new IOException("Error split instance");
    }

    value = new SDSRecordWritable();
    SDSTableSplit galaxySplit = (SDSTableSplit) split;
    SDSTableScan scan = galaxySplit.getScan();
    SDSTableProperty sdsProperty = scan.getSDSProperty();
    TableService.Iface tableClient = sdsProperty.formTableClient();

    ScanRequest scanRequest = scan.getScanRequest();
    scanRequest.setStartKey(galaxySplit.getStartRow())
        .setStopKey(galaxySplit.getStopRow())
        .setInGlobalOrder(false)
        .setCacheResult(false)
        .setScanInOneSplit(true);

    scanner = new TableScanner(tableClient, scanRequest);
    iterator = scanner.iterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iterator.hasNext()) {
      this.value.setRecord(iterator.next());
      return true;
    } else {
      return false;
    }
  }

  @Override
  public NullWritable getCurrentKey() throws IOException, InterruptedException {
    // NOT care key
    return NullWritable.get();
  }

  @Override
  public SDSRecordWritable getCurrentValue() throws IOException, InterruptedException {
    return value;
  }

  @Override
  public float getProgress() throws IOException, InterruptedException {
    // TODO(pengzhang): need info about total bytes and read bytes
    return 0;
  }

  @Override
  public void close() throws IOException {
    // TODO(pengzhang): Galaxy admin has no close()?
  }
}

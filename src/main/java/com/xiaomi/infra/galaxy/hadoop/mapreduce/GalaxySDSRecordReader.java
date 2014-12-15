package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.galaxy.sds.client.TableScanner;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;

public class GalaxySDSRecordReader extends RecordReader<NullWritable, SDSRecord> {
  private SDSRecord value;
  private TableScanner scanner;
  private Iterator<Map<String, Datum>> iterator;

  @Override
  public void initialize(InputSplit split, TaskAttemptContext context) throws IOException,
      InterruptedException {
    if (!(split instanceof GalaxySDSSplit)) {
      throw new IOException("Error split instance");
    }

    value = new SDSRecord();
    GalaxySDSSplit galaxySplit = (GalaxySDSSplit)split;
    TableScan scan = galaxySplit.getScan();
    SDSProperty sdsProperty = scan.getSDSProperty();
    TableService.Iface tableClient = sdsProperty.formTableClient();

    ScanRequest scanRequest = scan.getScanRequest();
    scanRequest.setStartKey(galaxySplit.getStartRow())
        .setStopKey(galaxySplit.getStopRow())
        .setInGlobalOrder(false)
        .setCacheResult(false);

    scanner = new TableScanner(tableClient, scanRequest);
    iterator = scanner.iterator();
  }

  @Override
  public boolean nextKeyValue() throws IOException, InterruptedException {
    if (iterator.hasNext()) {
      this.value.setValues(iterator.next());
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
  public SDSRecord getCurrentValue() throws IOException, InterruptedException {
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

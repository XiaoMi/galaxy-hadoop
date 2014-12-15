package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import com.xiaomi.infra.galaxy.sds.thrift.BatchOp;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequest;
import com.xiaomi.infra.galaxy.sds.thrift.BatchRequestItem;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.Request;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import libthrift091.TException;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class GalaxySDSRecordWriter extends RecordWriter<NullWritable, Map<String, Datum>> {
  private TableService.Iface tableClient = null;
  private String tableName = null;
  private int batchNum = 0;
  List<PutRequest> puts;
  boolean clientError = false;

  public GalaxySDSRecordWriter(TableService.Iface tableClient, String tableName, int batchNum) {
    this.tableClient = tableClient;
    this.tableName = tableName;
    this.batchNum = batchNum;
    puts = new LinkedList<PutRequest>();
  }

  @Override
  public void close(TaskAttemptContext context) throws IOException {
    flush();
  }

  @Override
  public void write(NullWritable nullWritable, Map<String, Datum> record)
      throws IOException, InterruptedException {
    PutRequest putRequest = new PutRequest(tableName, record);
    puts.add(putRequest);
    if (batchNum <= 1 || puts.size() >= batchNum) {
      flush();
    }
  }

  private void flush() throws IOException {
    if (puts.isEmpty()) {
      return;
    }
    if (clientError) {
      return;
    }

    BatchRequest batchRequest = new BatchRequest();
    for (PutRequest put : puts) {
      BatchRequestItem batchRequestItem = new BatchRequestItem();
      batchRequestItem.setAction(BatchOp.PUT);
      Request request = new Request();
      request.setPutRequest(put);
      batchRequestItem.setRequest(request);
      batchRequest.addToItems(batchRequestItem);
    }
    try {
      tableClient.batch(batchRequest);
    } catch (TException te) {
      clientError = true;
      throw new IOException("Put record to table " + tableName + " failed : ", te);
    } finally {
      puts.clear();
    }
  }
}

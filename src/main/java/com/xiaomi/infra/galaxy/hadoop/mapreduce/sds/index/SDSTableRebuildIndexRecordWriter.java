package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.index;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import com.xiaomi.infra.galaxy.sds.thrift.GetRequest;
import com.xiaomi.infra.galaxy.sds.thrift.GetResult;
import com.xiaomi.infra.galaxy.sds.thrift.PutRequest;
import com.xiaomi.infra.galaxy.sds.thrift.PutResult;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class SDSTableRebuildIndexRecordWriter extends
    RecordWriter<NullWritable, SDSRecordWritable> {

  private TableService.Iface tableClient = null;
  private String tableName = null;
  private static final int MAX_RETRY = 10;
  private static final int DEFAULT_SLEEP_TIME = 1000; //ms

  public SDSTableRebuildIndexRecordWriter(TableService.Iface tableClient, String tableName) {
    this.tableClient = tableClient;
    this.tableName = tableName;
  }

  @Override
  public void write(NullWritable nullWritable, SDSRecordWritable record) throws IOException,
      InterruptedException {
    Map<String, Datum> recordMap = record.getRecord();
    int retry = 0;
    try {
      while (true) {
        if (retry > MAX_RETRY) {
          throw new RuntimeException("Retry exhausted to rebuild index for record: " +
              DatumUtil.fromDatum(recordMap).toString());
        }
        try {
          PutRequest putRequest = new PutRequest().setTableName(tableName).setRecord(recordMap);
          PutResult putResult = tableClient.putToRebuildIndex(putRequest);
          if (putResult.isSuccess()) {
            break;
          }
          ++retry;
          GetRequest getRequest = new GetRequest()
              .setTableName(tableName)
              .setKeys(recordMap);
          GetResult getResult = tableClient.get(getRequest);
          if (getRequest == null || getResult.getItem() == null || getResult.getItem().isEmpty()) {
            break; // The record has been deleted.
          }
          recordMap = getResult.getItem();
        } catch (Throwable t) {
          ++retry;
          Thread.sleep(DEFAULT_SLEEP_TIME);
        }
      }
    } catch (Throwable t) {
      throw new IOException("Failed to rebuild index for record: " +
          DatumUtil.fromDatum(recordMap).toString());
    }
  }

  @Override
  public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
  }
}

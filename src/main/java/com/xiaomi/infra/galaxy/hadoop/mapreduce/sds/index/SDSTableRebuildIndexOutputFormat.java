package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.index;

import java.io.IOException;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableOutputFormat;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table.SDSTableProperty;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: linshangquan@xiaomi.com
 */
public class SDSTableRebuildIndexOutputFormat extends SDSTableOutputFormat {

  @Override
  public RecordWriter<NullWritable, SDSRecordWritable> getRecordWriter(
      TaskAttemptContext context) throws IOException, InterruptedException {
    SDSTableProperty sdsProperty = tableOutput.getSDSProperty();
    TableService.Iface tableClient = sdsProperty.formTableClient();
    return new SDSTableRebuildIndexRecordWriter(tableClient, tableOutput.getTableName());
  }
}

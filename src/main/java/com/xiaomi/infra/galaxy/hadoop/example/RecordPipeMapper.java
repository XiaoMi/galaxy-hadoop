package com.xiaomi.infra.galaxy.hadoop.example;

import java.io.IOException;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapper;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import org.apache.hadoop.io.NullWritable;

public class RecordPipeMapper extends SDSMapper<NullWritable, SDSRecordWritable> {
  @Override
  protected void map(Object key, SDSRecordWritable record, Context context)
      throws IOException, InterruptedException {
    context.write(NullWritable.get(), record);
  }
}

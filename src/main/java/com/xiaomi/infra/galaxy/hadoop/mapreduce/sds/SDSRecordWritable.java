package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SDSRecordWritable implements Writable {
  private Map<String, Datum> record = null;

  public SDSRecordWritable() {
  }

  public SDSRecordWritable(Map<String, Datum> record) {
    this.record = record;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedByteArray(out, DatumUtil.serializeDatumMap(record));
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    byte[] bytes = WritableUtils.readCompressedByteArray(in);
    this.record = DatumUtil.deserializeDatumMap(bytes);
  }

  public Map<String, Datum> getRecord() {
    return this.record;
  }

  public void setRecord(Map<String, Datum> record) {
    this.record = record;
  }
}

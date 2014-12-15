package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;
import java.util.TreeMap;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.DatumUtil;

public class GalaxySDSSplitKey implements Writable {
  private Map<String, Datum> key = null;

  public GalaxySDSSplitKey() {
  }

  public GalaxySDSSplitKey(Map<String, Datum> rowKey) {
    this.key = rowKey;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    if (key == null) {
      WritableUtils.writeVInt(out, 0);
      return;
    }

    WritableUtils.writeVInt(out, key.size());

    for (Map.Entry<String, Datum> entry : key.entrySet()) {
      WritableUtils.writeString(out, entry.getKey());
      WritableUtils.writeCompressedByteArray(out, DatumUtil.serialize(entry.getValue()));
    }
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    int itemSize = WritableUtils.readVInt(in);
    if (itemSize == 0) {
      this.key = null;
      return;
    }

    if (this.key == null) {
      this.key = new TreeMap<String, Datum>();
    }

    for (int i = 0; i < itemSize; i++) {
      String key = WritableUtils.readString(in);
      byte[] bytes = WritableUtils.readCompressedByteArray(in);
      Datum datum = DatumUtil.deserialize(bytes);
      this.key.put(key, datum);
    }
  }

  public Map<String, Datum> getRowKey() {
    return this.key;
  }
}

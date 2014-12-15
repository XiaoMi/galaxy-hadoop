package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.util.Map;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;

public class SDSRecord {
  Map<String, Datum> values;

  public SDSRecord(Map<String, Datum> values) {
    this.values = values;
  }

  public SDSRecord() {
    this.values = null;
  }

  public Map<String, Datum> getValues() {
    return values;
  }

  public void setValues(Map<String, Datum> values) {
    this.values = values;
  }

}

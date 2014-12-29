package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SDSTableOutput implements Writable {
  private String tableName;
  private SDSTableProperty sdsProperty;

  public SDSTableOutput() {
    this.tableName = new String();
    this.sdsProperty = new SDSTableProperty();
  }

  public SDSTableOutput(String tableName, SDSTableProperty sdsProperty) {
    this.tableName = tableName;
    this.sdsProperty = sdsProperty;
  }

  public SDSTableOutput(String tableName) {
    this.tableName = tableName;
    this.sdsProperty = new SDSTableProperty();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }

  public SDSTableProperty getSDSProperty() {
    return sdsProperty;
  }

  public void setSDSProperty(SDSTableProperty sdsProperty) {
    this.sdsProperty = sdsProperty;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedString(out, tableName);
    sdsProperty.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    tableName = WritableUtils.readCompressedString(in);
    sdsProperty.readFields(in);
  }
}

package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class TableOutput implements Writable {
  private String tableName;
  private SDSProperty sdsProperty;

  public TableOutput() {
    this.tableName = new String();
    this.sdsProperty = new SDSProperty();
  }

  public TableOutput(String tableName, SDSProperty sdsProperty) {
    this.tableName = tableName;
    this.sdsProperty = sdsProperty;
  }

  public TableOutput(String tableName) {
    this.tableName = tableName;
    this.sdsProperty = new SDSProperty();
  }

  public String getTableName() {
    return tableName;
  }

  public void setTableName(String tableName) {
    this.tableName = tableName;
  }


  public SDSProperty getSDSProperty() {
    return sdsProperty;
  }

  public void setSDSProperty(SDSProperty sdsProperty) {
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

package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

public class SDSTableSplit extends InputSplit
    implements Writable {

  private SDSTableScan scan = null;
  private SDSRecordWritable startRow = null;
  private SDSRecordWritable stopRow = null;
  private String regionLocation = null;

  public SDSTableSplit() {
  }

  public SDSTableSplit(SDSTableScan scan, Map<String, Datum> startRow, Map<String, Datum> stopRow,
                       String regionLocation) {
    this.scan = scan;
    this.startRow = new SDSRecordWritable(startRow);
    this.stopRow = new SDSRecordWritable(stopRow);
    this.regionLocation = regionLocation;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[] { regionLocation };
  }

  public SDSTableScan getScan() {
    return scan;
  }

  public Map<String, Datum> getStartRow() {
    return startRow.getRecord();
  }

  public Map<String, Datum> getStopRow() {
    return stopRow.getRecord();
  }

  @Override
  public void write(DataOutput out) throws IOException {
    scan.write(out);
    startRow.write(out);
    stopRow.write(out);
    WritableUtils.writeCompressedString(out, regionLocation);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    SDSTableScan tmpScan = new SDSTableScan();
    tmpScan.readFields(in);

    scan = tmpScan;
    startRow = new SDSRecordWritable();
    startRow.readFields(in);
    stopRow = new SDSRecordWritable();
    stopRow.readFields(in);
    regionLocation = WritableUtils.readCompressedString(in);
  }

  @Override
  public String toString() {
    return "Galaxy SDS splits " + scan + " : " + startRow + " : " + stopRow;
  }
}

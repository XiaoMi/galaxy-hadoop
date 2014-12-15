package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;
import org.apache.hadoop.mapreduce.InputSplit;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;

public class GalaxySDSSplit extends InputSplit
implements Writable {

  private TableScan scan = null;
  private GalaxySDSSplitKey startRow = null;
  private GalaxySDSSplitKey stopRow = null;
  private String regionLocation = null;

  public GalaxySDSSplit() {
  }

  public GalaxySDSSplit(TableScan scan, Map<String, Datum> startRow, Map<String, Datum> endRow,
                        String regionLocation) {
    this.scan = scan;
    this.startRow = new GalaxySDSSplitKey(startRow);
    this.stopRow = new GalaxySDSSplitKey(endRow);
    this.regionLocation = regionLocation;
  }

  @Override
  public long getLength() throws IOException {
    return 0;
  }

  @Override
  public String[] getLocations() throws IOException {
    return new String[]{regionLocation};
  }

  public TableScan getScan() {
    return scan;
  }

  public Map<String, Datum> getStartRow() {
    return startRow.getRowKey();
  }

  public Map<String, Datum> getStopRow() {
    return stopRow.getRowKey();
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
    TableScan tmpScan = new TableScan();
    tmpScan.readFields(in);

    scan = tmpScan;
    startRow = new GalaxySDSSplitKey();
    startRow.readFields(in);
    stopRow = new GalaxySDSSplitKey();
    stopRow.readFields(in);
    regionLocation = WritableUtils.readCompressedString(in);
  }
  
  @Override
  public String toString() {
    return "Galaxy SDS splits " + scan + " : " + startRow + " : " + stopRow;
  }
}

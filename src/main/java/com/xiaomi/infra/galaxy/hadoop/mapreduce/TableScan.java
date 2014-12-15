package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;

public class TableScan implements Writable {
  private ScanRequest scanRequest;
  private SDSProperty sdsProperty;

  public TableScan() {
    this.scanRequest = new ScanRequest();
    this.sdsProperty = new SDSProperty();
  }

  public TableScan(ScanRequest scanRequest, SDSProperty sdsProperty) {
    this.scanRequest = scanRequest;
    this.sdsProperty = sdsProperty;
  }

  public TableScan(ScanRequest scanRequest) {
    this.scanRequest = scanRequest;
    this.sdsProperty = new SDSProperty();
  }

  public ScanRequest getScanRequest() {
    return scanRequest;
  }

  public SDSProperty getSDSProperty() {
    return sdsProperty;
  }

  @Override
  public void write(DataOutput out) throws IOException {
    try {
      TSerializer serializer = new TSerializer(new TCompactProtocol.Factory());
      byte[] bytes = serializer.serialize(scanRequest);
      WritableUtils.writeCompressedByteArray(out, bytes);
    } catch (TException te) {
      throw new IOException("Failed to serialize thrift object: " + scanRequest, te);
    }

    sdsProperty.write(out);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    ScanRequest scan = new ScanRequest();
    byte[] bytes = WritableUtils.readCompressedByteArray(in);
    try {
      TDeserializer deserializer = new TDeserializer(new TCompactProtocol.Factory());
      deserializer.deserialize(scan, bytes);
    } catch (TException te) {
      throw new IOException("Failed to serialize thrift object: " + scan, te);
    }
    this.scanRequest = scan;

    sdsProperty.readFields(in);
  }
}

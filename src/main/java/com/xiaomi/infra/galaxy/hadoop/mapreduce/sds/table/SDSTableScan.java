package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.xiaomi.infra.galaxy.sds.thrift.ScanRequest;
import libthrift091.TDeserializer;
import libthrift091.TException;
import libthrift091.TSerializer;
import libthrift091.protocol.TCompactProtocol;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SDSTableScan implements Writable {
  private ScanRequest scanRequest;
  private SDSTableProperty sdsProperty;

  public SDSTableScan() {
    this.scanRequest = new ScanRequest();
    this.sdsProperty = new SDSTableProperty();
  }

  public SDSTableScan(ScanRequest scanRequest, SDSTableProperty sdsProperty) {
    this.scanRequest = scanRequest;
    this.sdsProperty = sdsProperty;
  }

  public SDSTableScan(ScanRequest scanRequest) {
    this.scanRequest = scanRequest;
    this.sdsProperty = new SDSTableProperty();
  }

  public ScanRequest getScanRequest() {
    return scanRequest;
  }

  public SDSTableProperty getSDSProperty() {
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

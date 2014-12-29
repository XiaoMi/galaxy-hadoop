package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file;

import java.io.IOException;
import java.util.Map;

import com.xiaomi.infra.galaxy.client.io.SDSRecordReaderWriterFactory;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import com.xiaomi.infra.galaxy.sds.thrift.SLFileType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SDSFileRecordWriter extends RecordWriter<NullWritable, SDSRecordWritable> {
  private Path path;
  private com.xiaomi.infra.galaxy.api.io.RecordWriter<Map<String, Datum>> writer;

  public SDSFileRecordWriter(Configuration conf, Path path, SLFileType fileType,
                             Compression compression) {
    try {
      this.path = path;
      FileSystem fileSystem = FileSystem.get(path.toUri(), conf);
      FSDataOutputStream outputStream = fileSystem.create(path, true);
      this.writer = SDSRecordReaderWriterFactory
          .getRecordWriter(outputStream, null, fileType, compression);
    } catch (Exception e) {
      throw new RuntimeException("Failed to open file: " + path, e);
    }
  }

  @Override public void write(NullWritable nullWritable, SDSRecordWritable record)
      throws IOException, InterruptedException {
    this.writer.append(record.getRecord());
  }

  @Override public void close(TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    this.writer.close();
  }
}

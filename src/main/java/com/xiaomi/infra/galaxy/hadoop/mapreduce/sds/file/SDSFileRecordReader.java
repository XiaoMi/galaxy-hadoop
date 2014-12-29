package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;

import com.xiaomi.infra.galaxy.client.io.SDSRecordReaderWriterFactory;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

public class SDSFileRecordReader extends RecordReader<NullWritable, SDSRecordWritable> {
  private com.xiaomi.infra.galaxy.api.io.RecordReader<Map<String, Datum>> reader;

  @Override public void initialize(InputSplit inputSplit, TaskAttemptContext taskAttemptContext)
      throws IOException, InterruptedException {
    FileSplit split = (FileSplit) inputSplit;
    Path file = split.getPath();
    final FileSystem fs = file.getFileSystem(taskAttemptContext.getConfiguration());
    InputStream inputStream = fs.open(file);
    reader = SDSRecordReaderWriterFactory.getRecordReader(inputStream);
  }

  @Override public boolean nextKeyValue() throws IOException, InterruptedException {
    return reader.hasNext();
  }

  @Override public NullWritable getCurrentKey() throws IOException, InterruptedException {
    // NOT care key
    return NullWritable.get();
  }

  @Override public SDSRecordWritable getCurrentValue() throws IOException, InterruptedException {
    return new SDSRecordWritable(reader.next());
  }

  @Override public float getProgress() throws IOException, InterruptedException {
    // TODO support progress
    return 0;
  }

  @Override public void close() throws IOException {
    reader.close();
  }
}

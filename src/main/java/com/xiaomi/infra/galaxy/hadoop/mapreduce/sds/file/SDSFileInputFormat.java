package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file;

import java.io.IOException;
import java.util.List;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;

public class SDSFileInputFormat extends FileInputFormat<NullWritable, SDSRecordWritable> {
  @Override protected boolean isSplitable(JobContext context, Path filename) {
    return false;
  }

  @Override public List<InputSplit> getSplits(JobContext job) throws IOException {
    return super.getSplits(job);
  }

  @Override public RecordReader<NullWritable, SDSRecordWritable> createRecordReader(
      InputSplit inputSplit,
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    taskAttemptContext.setStatus(inputSplit.toString());
    return new SDSFileRecordReader();
  }
}

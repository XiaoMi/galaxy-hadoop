package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.file;

import java.io.IOException;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.io.thrift.Compression;
import com.xiaomi.infra.galaxy.sds.thrift.SLFileType;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.mapreduce.lib.output.FileOutputCommitter;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class SDSFileOutputFormat extends FileOutputFormat<NullWritable, SDSRecordWritable>
    implements Configurable {
  public static final String FILE_TYPE_CONF = "sds.mapreduce.input.file.type";
  public static final String COMPRESSION_CONF = "sds.mapreduce.input.file.compression";
  public static final String DEFAULT_FILE_SUFFIX = ".slf";
  private Configuration conf = null;
  private SLFileType fileType = SLFileType.DATUM_MAP;
  private Compression compression = Compression.SNAPPY;
  private FileOutputCommitter committer = null;

  @Override public void setConf(Configuration entries) {
    this.conf = entries;
    String fileTypeStr = conf.get(FILE_TYPE_CONF);
    if (fileTypeStr != null && !fileTypeStr.isEmpty()) {
      fileType = SLFileType.valueOf(fileTypeStr);
    }
    String compressionStr = conf.get(COMPRESSION_CONF);
    if (compressionStr != null && !compressionStr.isEmpty()) {
      compression = Compression.valueOf(compressionStr);
    }
  }

  @Override public Configuration getConf() {
    return this.conf;
  }

  @Override public RecordWriter<NullWritable, SDSRecordWritable> getRecordWriter(
      TaskAttemptContext taskAttemptContext) throws IOException, InterruptedException {
    Path path = this.getDefaultWorkFile(taskAttemptContext, DEFAULT_FILE_SUFFIX);
    return new SDSFileRecordWriter(conf, path, fileType, compression);
  }

  @Override public synchronized OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException {
    if (committer == null) {
      Path output = getOutputPath(context);
      committer = new FileOutputCommitter(output, context);
    }
    return committer;
  }
}

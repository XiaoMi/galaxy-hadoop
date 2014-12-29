package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.IOException;

import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

/**
 * Small committer class that does not do anything.
 */
public class NoopOutputCommitter extends OutputCommitter {
  private static NoopOutputCommitter INSTANCE = new NoopOutputCommitter();

  public static NoopOutputCommitter getInstance() {
    return INSTANCE;
  }

  @Override
  public void abortTask(TaskAttemptContext arg0) throws IOException {
  }

  @Override
  public void cleanupJob(JobContext arg0) throws IOException {
  }

  @Override
  public void commitTask(TaskAttemptContext arg0) throws IOException {
  }

  @Override
  public boolean needsTaskCommit(TaskAttemptContext arg0) throws IOException {
    return false;
  }

  @Override
  public void setupJob(JobContext arg0) throws IOException {
  }

  @Override
  public void setupTask(TaskAttemptContext arg0) throws IOException {
  }

}

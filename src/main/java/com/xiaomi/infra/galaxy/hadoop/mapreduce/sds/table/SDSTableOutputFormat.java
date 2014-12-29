package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.IOException;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSMapReduceUtil;
import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSRecordWritable;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.OutputFormat;
import org.apache.hadoop.mapreduce.RecordWriter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;

public class SDSTableOutputFormat extends OutputFormat<NullWritable, SDSRecordWritable>
    implements Configurable {
  public static String OUTPUT_TABLE = "sds.mapreduce.output.table";
  public static String BATCH_NUM = "sds.mapreduce.output.batch.number";
  public static int DEFAULT_BATCH_NUM = 1;

  Configuration conf = null;
  SDSTableOutput tableOutput = null;
  int batchNum;

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;

    String outputString = conf.get(OUTPUT_TABLE);
    try {
      tableOutput = SDSMapReduceUtil.convertStringToTableOutput(outputString);
    } catch (IOException e) {
      throw new RuntimeException("Failed to convert TableOutput string: " + outputString, e);
    }

    batchNum = conf.getInt(BATCH_NUM, DEFAULT_BATCH_NUM);
  }

  @Override
  public Configuration getConf() {
    return conf;
  }

  @Override
  public RecordWriter<NullWritable, SDSRecordWritable> getRecordWriter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    SDSTableProperty sdsProperty = tableOutput.getSDSProperty();
    TableService.Iface tableClient = sdsProperty.formTableClient();
    return new SDSTableRecordWriter(tableClient, tableOutput.getTableName(), batchNum);
  }

  @Override
  public void checkOutputSpecs(JobContext context) throws IOException, InterruptedException {

  }

  @Override
  public OutputCommitter getOutputCommitter(TaskAttemptContext context)
      throws IOException, InterruptedException {
    return NoopOutputCommitter.getInstance();
  }

}

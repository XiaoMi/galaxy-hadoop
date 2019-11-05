package com.xiaomi.infra;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import java.io.IOException;

public class BucketTraffic {
  static {
    Configuration.addDefaultResource("mapred-default.xml");
    Configuration.addDefaultResource("core-default.xml");
    Configuration.addDefaultResource("yarn-default.xml");
    Configuration.addDefaultResource("hdfs-default.xml");
    Configuration.addDefaultResource("hdfs-site.xml");
    Configuration.addDefaultResource("hbase-default.xml");
    Configuration.addDefaultResource("hbase-site.xml");
    Configuration.addDefaultResource("galaxy-site.xml");
    Configuration.addDefaultResource("yarn-site.xml");
    Configuration.addDefaultResource("core-site.xml");
    Configuration.addDefaultResource("mapred-site.xml");
  }

  enum BucketTrafficCounter {
    ProcessedLines
  }

  public static class BucketMapper extends Mapper<Object, Text, Text, LongWritable> {
    private final static LongWritable one = new LongWritable(1L);
    private Text word = new Text();
    private Counter inputLines;

    protected void setup(Context context) throws IOException, InterruptedException {
      inputLines = context.getCounter(BucketTrafficCounter.ProcessedLines);
    }

    public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
      String[] splitedLine = value.toString().split("\\t");
      try {
        String bucketName = splitedLine[15].split("/")[1];
        long length = Long.parseLong(splitedLine[13]);
        context.write(new Text(bucketName), new LongWritable(length));
        inputLines.increment(1);
      } catch (Exception ignore) {}
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // cleanup if needed
    }
  }

  public static class TrafficReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
    private LongWritable result = new LongWritable();
    boolean isReducer;

    protected void setup(Context context) throws IOException, InterruptedException {
      isReducer = context.getTaskAttemptID().getTaskType() == TaskType.REDUCE;
    }

    public void reduce(Text key, Iterable<LongWritable> values, Context context)
        throws IOException, InterruptedException {
      long sum = 0;
      for (LongWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // cleanup if needed
    }
  }

  public static void main(String[] args)
      throws IOException, ClassNotFoundException, InterruptedException {
    Thread.sleep(10000);
    Configuration conf = new Configuration();

    // Use GenericOptionsParse, supporting -D -conf etc.
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    String input = otherArgs[0];
    String output = otherArgs[1];

    final FileSystem fs = FileSystem.get(new Path(output).toUri(), conf);
    if (conf.getBoolean("cleanup-output", true)) {
      fs.delete(new Path(output), true);
    }

    conf.setBoolean("mapreduce.task.profile", true);
    conf.set("mapreduce.task.profile.params", "-agentlib:hprof=cpu=samples,depth=10,file=%s");
    conf.set("mapreduce.task.profile.reduces", ""); // no reduces

    TableMapReduceUtil
        .addDependencyJars(conf, com.xiaomi.infra.galaxy.fds.client.exception.GalaxyException.class,
            com.xiaomi.infra.galaxy.fds.client.model.FDSObjectInputStream.class,
            com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException.class,
            com.xiaomi.infra.galaxy.hadoop.fs.FDSFileSystem.class,
            org.apache.http.conn.HttpClientConnectionManager.class, // httpclient-4.3.3
            org.apache.http.config.RegistryBuilder.class,   // httpcore-4.3.3
            com.google.gson.Gson.class, org.apache.hadoop.hbase.util.Bytes.class,com.xiaomi.infra.galaxy.fds.auth.signature.SignAlgorithm.class);
    // hbase-0.94.3-mdh1.1.0.jar
    Job job = new Job(conf, "CodeLab-BucketTraffic");
    System.out.println("Running framework: " + job.getConfiguration().get("mapreduce.framework.name"));
    System.out.println("File system: " + job.getConfiguration().get("fs.default.name"));
    System.out.println("File system: " + job.getConfiguration().get("fs.AbstractFileSystem.hdfs.impl"));
    System.out.println("mapreduce.framework.name: " + job.getConfiguration().get("mapreduce.framework.name"));
    System.out.println("mapreduce.job.running.map.limit: " + job.getConfiguration().get("mapreduce.job.running.map.limit"));

    job.setJarByClass(BucketTraffic.class);
    job.setMapperClass(BucketTraffic.BucketMapper.class);
    job.setCombinerClass(BucketTraffic.TrafficReducer.class);
    job.setReducerClass(BucketTraffic.TrafficReducer.class);
    FileInputFormat.setMinInputSplitSize(job, 128 * 1024 * 1024);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(LongWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

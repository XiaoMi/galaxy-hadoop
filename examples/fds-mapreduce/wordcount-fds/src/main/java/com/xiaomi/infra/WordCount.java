package com.xiaomi.infra;

import java.io.IOException;
import java.util.StringTokenizer;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.TaskType;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import com.xiaomi.infra.galaxy.exception.GalaxyException;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectInputStream;
import com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.hadoop.fs.FDSFileSystem;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class WordCount {
  static enum WordCountCounter {
    InputWords,
    UnduplicatedWords
  };

  public static class TokenizerMapper
       extends Mapper<Object, Text, Text, IntWritable>{

    private final static IntWritable one = new IntWritable(1);
    private Text word = new Text();
    private Counter inputWords;

    protected void setup(Context context) throws IOException, InterruptedException {
      inputWords = context.getCounter(WordCountCounter.InputWords);
    }

    public void map(Object key, Text value, Context context
                    ) throws IOException, InterruptedException {
      StringTokenizer itr = new StringTokenizer(value.toString());
      while (itr.hasMoreTokens()) {
        word.set(itr.nextToken());
        context.write(word, one);
        inputWords.increment(1);
      }
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // cleanup if needed
    }
  }

  public static class IntSumReducer
       extends Reducer<Text,IntWritable,Text,IntWritable> {
    private IntWritable result = new IntWritable();
    Counter unduplicatedWords;
    boolean isReducer;

    protected void setup(Context context
                       ) throws IOException, InterruptedException {
      unduplicatedWords = context.getCounter(WordCountCounter.UnduplicatedWords);
      isReducer = context.getTaskAttemptID().getTaskType() == TaskType.REDUCE;
    }

    public void reduce(Text key, Iterable<IntWritable> values,
                       Context context
                       ) throws IOException, InterruptedException {
      int sum = 0;
      if (isReducer) {
        // only counts in reduce phase, not for combine phase
        unduplicatedWords.increment(1);
      }

      for (IntWritable val : values) {
        sum += val.get();
      }
      result.set(sum);
      context.write(key, result);
    }

    protected void cleanup(Context context) throws IOException, InterruptedException {
      // cleanup if needed
    }
  }

  public static void main(String[] args) throws Exception {
    Configuration conf = new Configuration();
    // Use GenericOptionsParse, supporting -D -conf etc.
    String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();

    if (otherArgs.length != 2) {
      System.err.println("Usage: wordcount <in> <out>");
      System.exit(2);
    }
    String input = otherArgs[0];
    String output = otherArgs[1];

    System.out.println("Running framework: " + conf.get("mapreduce.framework.name"));
    System.out.println("File system: " + conf.get("fs.default.name"));

    final FileSystem fs = FileSystem.get(new Path(output).toUri(), conf);
    if (conf.getBoolean("cleanup-output", true)) {
      fs.delete(new Path(output), true);
    }

    conf.setBoolean("mapreduce.task.profile", true);
    conf.set("mapreduce.task.profile.params", "-agentlib:hprof=cpu=samples,depth=10,file=%s");
    conf.set("mapreduce.task.profile.reduces", ""); // no reduces

    TableMapReduceUtil.addDependencyJars(conf,
        com.xiaomi.infra.galaxy.exception.GalaxyException.class,
        com.xiaomi.infra.galaxy.fds.client.model.FDSObjectInputStream.class,
        com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException.class,
        com.xiaomi.infra.galaxy.hadoop.fs.FDSFileSystem.class,
        javax.ws.rs.core.Configuration.class, // javax.ws.rs-api-2.0.jar
        javax.annotation.Priority.class, // javax.annotation-api-1.2.jar
        org.glassfish.hk2.utilities.binding.AbstractBinder.class, // hk2-api-2.2.0-b21.jar
        org.glassfish.hk2.utilities.cache.Computable.class, // hk2-utils-2.2.0-b21.jar
        org.jvnet.hk2.external.generator.ServiceLocatorGeneratorImpl.class, // hk2-locator-2.2.0-b21.jar
        org.glassfish.jersey.client.JerseyClient.class, // jersey-client-2.5.1.jar
        org.glassfish.jersey.internal.util.collection.Value.class, // jersey-common-2.5.1.jar
        org.glassfish.jersey.message.filtering.EntityFilteringFeature.class, // jersey-entity-filtering-2.5.1.jar
        org.glassfish.jersey.moxy.json.MoxyJsonFeature.class,   // jersey-media-moxy-2.5.1.jar
        org.eclipse.persistence.internal.libraries.antlr.runtime.RecognitionException.class,  // org.eclipse.persistence.antlr-2.5.0.jar
        org.eclipse.persistence.internal.queries.ContainerPolicy.class, // org.eclipse.persistence.core-2.5.0.jar
        org.eclipse.persistence.jaxb.rs.MOXyJsonProvider.class, // org.eclipse.persistence.moxy-2.5.0.jar
        org.apache.hadoop.hbase.util.Bytes.class, // hbase-0.94.3-mdh1.1.0.jar
        net.sf.cglib.proxy.MethodInterceptor.class // cglib-nodep-2.2.jar
    );

    Job job = new Job(conf, "CodeLab-Wordcount");
    job.setJarByClass(WordCount.class);
    job.setMapperClass(TokenizerMapper.class);
    job.setCombinerClass(IntSumReducer.class);
    job.setReducerClass(IntSumReducer.class);

    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(IntWritable.class);

    FileInputFormat.addInputPath(job, new Path(input));
    FileOutputFormat.setOutputPath(job, new Path(output));

    System.exit(job.waitForCompletion(true) ? 0 : 1);
  }
}

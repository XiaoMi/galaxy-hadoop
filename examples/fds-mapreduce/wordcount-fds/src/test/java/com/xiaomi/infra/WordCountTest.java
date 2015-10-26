package com.xiaomi.infra;

import static org.junit.Assert.*;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mrunit.mapreduce.MapDriver;
import org.apache.hadoop.mrunit.mapreduce.MapReduceDriver;
import org.apache.hadoop.mrunit.mapreduce.ReduceDriver;
import org.junit.Before;
import org.junit.Test;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: liupengcheng@xiaomi.com
 */
public class WordCountTest {

  MapDriver<Object, Text, Text, IntWritable> mapDriver;
  ReduceDriver<Text,IntWritable,Text,IntWritable> reduceDriver;
  MapReduceDriver<Object, Text, Text, IntWritable, Text, IntWritable> mapReduceDriver;

  @Before
  public void setUp() {
    WordCount.TokenizerMapper mapper = new WordCount.TokenizerMapper();
    WordCount.IntSumReducer reducer = new WordCount.IntSumReducer();
    mapDriver = MapDriver.newMapDriver(mapper);
    reduceDriver = ReduceDriver.newReduceDriver(reducer);
    mapReduceDriver = MapReduceDriver.newMapReduceDriver(mapper, reducer);
  }

  @Test
  public void testMapper() throws IOException {
    mapDriver.withInput(new Text(), new Text("abc xyz abc"))
      .withInput(new Text(), new Text("abc xyz abc"))
        .withOutput(new Text("abc"), new IntWritable(1))
        .withOutput(new Text("xyz"), new IntWritable(1))
        .withOutput(new Text("abc"), new IntWritable(1))
        .withOutput(new Text("abc"), new IntWritable(1))
        .withOutput(new Text("xyz"), new IntWritable(1))
        .withOutput(new Text("abc"), new IntWritable(1))
        .runTest();
    
    assertEquals("Expected 3 counter increment", 6, mapDriver.getCounters()
      .findCounter(WordCount.WordCountCounter.InputWords).getValue());
  }

  @Test
  public void testReducer() throws IOException {
    List<IntWritable> values = new ArrayList<IntWritable>();
    values.add(new IntWritable(1));
    values.add(new IntWritable(1));
    
    reduceDriver.withInput(new Text("abc"), values)
        .withInput(new Text("xyz"), values)
        .withOutput(new Text("abc"), new IntWritable(2))
        .withOutput(new Text("xyz"), new IntWritable(2))
        .runTest();
    
    assertEquals("Expected 2 counter increment", 2, reduceDriver.getCounters()
      .findCounter(WordCount.WordCountCounter.UnduplicatedWords).getValue());
  }
  
  @Test
  public void testMapReduce() throws IOException {
    mapReduceDriver.withInput(new Text(), new Text("123 ddd abc xyz abc"));
    // output key should be in order
    mapReduceDriver.withOutput(new Text("123"), new IntWritable(1))
        .withOutput(new Text("abc"), new IntWritable(2))
        .withOutput(new Text("ddd"), new IntWritable(1))
        .withOutput(new Text("xyz"), new IntWritable(1))
        .runTest();
  }
}
package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import java.util.Map;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;

public abstract class TableReducer<KEYIN, VALUEIN>
    extends Reducer<KEYIN, VALUEIN, NullWritable, Map<String, Datum>> {
}
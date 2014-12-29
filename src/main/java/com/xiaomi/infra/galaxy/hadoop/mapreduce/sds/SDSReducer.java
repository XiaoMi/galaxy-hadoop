package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import java.util.Map;

import com.xiaomi.infra.galaxy.sds.thrift.Datum;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class SDSReducer<KEYIN, VALUEIN>
    extends Reducer<KEYIN, VALUEIN, NullWritable, Map<String, Datum>> {
}
package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Reducer;

public abstract class SDSReducer<KEYIN, VALUEIN>
    extends Reducer<KEYIN, VALUEIN, NullWritable, SDSRecordWritable> {
}
package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import org.apache.hadoop.mapreduce.Mapper;

public abstract class SDSMapper<KEYOUT, VALUEOUT>
    extends Mapper<Object, SDSRecordWritable, KEYOUT, VALUEOUT> {
}

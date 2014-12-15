package com.xiaomi.infra.galaxy.hadoop.mapreduce;

import org.apache.hadoop.mapreduce.Mapper;

public abstract class TableMapper<KEYOUT, VALUEOUT>
  extends Mapper<Object, SDSRecord, KEYOUT, VALUEOUT> {
}

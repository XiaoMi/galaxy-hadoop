package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds;

import java.util.Map;

import org.apache.hadoop.conf.Configuration;

public class SDSConfiguration {
  public static final String SDS_MAPREDUCE_REST_ENDPOINT = "sds.mapreduce.rest.endpoint";
  public static final String SDS_MAPREDUCE_REST_ADMIN_PATH = "sds.mapreduce.rest.admin.path";
  public static final String SDS_MAPREDUCE_REST_TABLE_PATH = "sds.mapreduce.rest.table.path";
  public static final String SDS_MAPREDUCE_SECRET_ID = "sds.mapreduce.secret.id";
  public static final String SDS_MAPREDUCE_SECRET_KEY = "sds.mapreduce.secret.key";
  public static final String SDS_MAPREDUCE_CLIENT_MAX_RETRY = "sds.mapreduce.client.max.retry";
  public static int DEFAULT_SDS_MAPREDUCE_SCAN_LIMIT = 1000;
  public static int DEFAULT_SDS_MAPREDUCE_CLIENT_MAX_RETRY = 10;

  public static Configuration create() {
    Configuration conf = new Configuration();
    return addGalaxyResources(conf);
  }

  public static Configuration create(final Configuration rhs) {
    Configuration conf = create();
    merge(conf, rhs);
    return conf;
  }

  private static Configuration addGalaxyResources(Configuration conf) {
    conf.addResource("galaxy-default.xml");
    conf.addResource("galaxy-site.xml");
    return conf;
  }

  public static void merge(Configuration destConf, Configuration srcConf) {
    for (Map.Entry<String, String> e : srcConf) {
      destConf.set(e.getKey(), e.getValue());
    }
  }
}

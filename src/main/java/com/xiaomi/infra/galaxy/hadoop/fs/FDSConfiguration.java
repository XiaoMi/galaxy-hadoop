package com.xiaomi.infra.galaxy.hadoop.fs;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;

import com.google.common.base.Strings;
import org.apache.hadoop.conf.Configuration;

/**
 * @author zj
 */
public class FDSConfiguration {

  public static String GALAXY_FDS_SERVER_ENABLE_HTTPS =
      "galaxy.fds.server.enableHttps";

  public static String GALAXY_FDS_SERVER_ENABLE_CDN_FOR_UPLOAD =
      "galaxy.fds.server.enableCdnForUpload";

  public static boolean DEFAULT_GALAXY_FDS_SERVER_ENABLE_CDN_FOR_UPLOAD = false;

  public static String GALAXY_FDS_SERVER_ENABLE_CDN_FOR_DOWNLOAD =
      "galaxy.fds.server.enableCdnForDownload";

  public static boolean DEFAULT_GALAXY_FDS_SERVER_ENABLE_CDN_FOR_DOWNLOAD = true;

  public static String GALAXY_FDS_SERVER_REGION =
      "galaxy.fds.server.region";

  public static String DEFAULT_GALAXY_FDS_SERVER_REGION = "";

  public static final String GALAXY_FDS_SERVER_ENDPOINT =
      "galaxy.fds.server.endpoint";

  public static String DEFAULT_GALAXY_FDS_SERVER_ENDPOINT = null;

  public static String GALAXY_FDS_SERVER_ENABLE_METRICS =
      "galaxy.fds.server.enableMetrics";

  public static boolean DEFAULT_GALAXY_FDS_SERVER_ENABLE_METRICS = false;

  public static String GALAXY_FDS_SERVER_ENABLE_MD5CALCULATE =
      "galaxy.fds.server.enableMD5Calculate";

  public static boolean DEFAULT_FDS_SERVER_ENABLE_MD5CALCULATE = false;

  public static String GALAXY_FDS_REGIONS = "galaxy.fds.regions";

  public static String DEFAULT_GALAXY_FDS_REGIONS =
      "cnbj0;cnbj1;cnbj2;awsbj0;awsusor0;awssgp0;awsde0";

  public static final String GALAXY_FDS_ACCESS_KEY = "fs.fds.AccessKey";

  public static final String GALAXY_FDS_ACCESS_SECRET = "fs.fds.AccessSecret";

  private static final String ENDPOINT_SUFFIX = ".fds.api.xiaomi.com";

  public static String GALAXY_FDS_SERVER_ENABLE_THIRD_PART =
      "galaxy.fds.server.enable.third.part";
  public static boolean DEFAULT_GALAXY_FDS_SERVER_ENABLE_THIRD_PART = false;

  public static FDSClientConfiguration getFdsClientConfig(Configuration conf) {

    boolean enableHttps = conf.getBoolean(GALAXY_FDS_SERVER_ENABLE_HTTPS, false);
    boolean enableCdnForUpload = conf.getBoolean(
        GALAXY_FDS_SERVER_ENABLE_CDN_FOR_UPLOAD,
        DEFAULT_GALAXY_FDS_SERVER_ENABLE_CDN_FOR_UPLOAD);
    boolean enableCdnForDownload = conf.getBoolean(
        GALAXY_FDS_SERVER_ENABLE_CDN_FOR_DOWNLOAD,
        DEFAULT_GALAXY_FDS_SERVER_ENABLE_CDN_FOR_DOWNLOAD);
    boolean enableMetrics = conf.getBoolean(
        GALAXY_FDS_SERVER_ENABLE_METRICS,
        DEFAULT_GALAXY_FDS_SERVER_ENABLE_METRICS);
    boolean enableMD5Calculate = conf.getBoolean(
        GALAXY_FDS_SERVER_ENABLE_MD5CALCULATE,
        DEFAULT_FDS_SERVER_ENABLE_MD5CALCULATE);

    String regionName = conf.get(FDSConfiguration.GALAXY_FDS_SERVER_REGION,
        DEFAULT_GALAXY_FDS_SERVER_REGION);
    String endpoint = conf.get(FDSConfiguration.GALAXY_FDS_SERVER_ENDPOINT,
        DEFAULT_GALAXY_FDS_SERVER_ENDPOINT);

    if(endpoint == null){
      endpoint = regionName + ENDPOINT_SUFFIX;
    }

    FDSClientConfiguration fdsConfig = new FDSClientConfiguration(endpoint);
    fdsConfig.enableHttps(enableHttps);
    fdsConfig.enableCdnForUpload(enableCdnForUpload);
    fdsConfig.enableCdnForDownload(enableCdnForDownload);
    fdsConfig.setEnableMd5Calculate(enableMD5Calculate);
    if (enableMetrics) {
      fdsConfig.enableMetrics();
    } else {
      fdsConfig.disableMetrics();
    }

    return fdsConfig;
  }
}

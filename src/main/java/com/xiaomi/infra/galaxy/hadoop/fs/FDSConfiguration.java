package com.xiaomi.infra.galaxy.hadoop.fs;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
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

  public static String GALAXY_FDS_SERVER_ENABLE_METRICS =
      "galaxy.fds.server.enableMetrics";

  public static boolean DEFAULT_GALAXY_FDS_SERVER_ENABLE_METRICS = false;

  public static String GALAXY_FDS_SERVER_ENABLE_MD5CALCULATE =
      "galaxy.fds.server.enableMD5Calculate";

  public static boolean DEFAULT_FDS_SERVER_ENABLE_MD5CALCULATE = false;

  public static final String GALAXY_FDS_ACCESS_KEY = "fs.fds.AccessKey";

  public static final String GALAXY_FDS_ACCESS_SECRET = "fs.fds.AccessSecret";

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

    FDSClientConfiguration fdsConfig = new FDSClientConfiguration();
    if (!regionName.equals("")) {
      fdsConfig.setRegionName(regionName);
    }
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

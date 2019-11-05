package com.xiaomi.infra.galaxy.hadoop.fs;

import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import org.apache.hadoop.conf.Configuration;

import java.net.Inet4Address;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.util.Enumeration;

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

  public static String GALAXY_FDS_CLIENT_MAX_DOWNLOAD_BANDWIDTH = "galaxy.fds.client.max.download.bandwidth";
  public static long GALAXY_FDS_DEFAULT_CLIENT_MAX_DOWNLOAD_BANDWIDTH = 10 * 1024 * 1024;

  public static String GALAXY_FDS_CLIENT_MAX_CONNECTION = "galaxy.fds.client.max.connection";
  public static int GALAXY_FDS_DEFAULT_CLIENT_MAX_CONNECTION = 50;

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

  private final static String agentPrefix =  "galaxy-hadoop";

  public static String GALAXY_FDS_RETRY_COUNTS =
      "galaxy.fds.retry.counts";
  public static int DEFAULT_GALAXY_FDS_RETRY_COUNTS = 3;

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

    long maxDownloadBandwidth = conf.getLong(GALAXY_FDS_CLIENT_MAX_DOWNLOAD_BANDWIDTH,
        GALAXY_FDS_DEFAULT_CLIENT_MAX_DOWNLOAD_BANDWIDTH);
    int maxConnection = conf.getInt(GALAXY_FDS_CLIENT_MAX_CONNECTION,
        GALAXY_FDS_DEFAULT_CLIENT_MAX_CONNECTION);

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
    fdsConfig.setDownloadBandwidth(maxDownloadBandwidth);
    fdsConfig.setMaxConnection(maxConnection);
    String userAgent = agentPrefix + ";" + getIntranetIP();
    fdsConfig.setUserAgent(userAgent);
    if (enableMetrics) {
      fdsConfig.enableMetrics();
    } else {
      fdsConfig.disableMetrics();
    }

    return fdsConfig;
  }

  private static String getIntranetIP() {
    try {
      InetAddress address = InetAddress.getLocalHost();
      if (address.isLoopbackAddress()) {
        Enumeration<NetworkInterface> allNetInterfaces = NetworkInterface.getNetworkInterfaces();
        while (allNetInterfaces.hasMoreElements()) {
          NetworkInterface netInterface = (NetworkInterface) allNetInterfaces.nextElement();
          Enumeration<InetAddress> addresses = netInterface.getInetAddresses();
          while (addresses.hasMoreElements()) {
            InetAddress ip = addresses.nextElement();
            if (!ip.isLinkLocalAddress() && !ip.isLoopbackAddress() && ip instanceof Inet4Address) {
              return ip.getHostAddress();
            }
          }
        }
      }
      return address.getHostAddress();
    } catch (UnknownHostException e) {
      return null;
    } catch (SocketException e) {
      return null;
    }
  }

}

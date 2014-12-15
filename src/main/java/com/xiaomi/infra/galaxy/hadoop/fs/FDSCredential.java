package com.xiaomi.infra.galaxy.hadoop.fs;

import java.net.URI;

import org.apache.hadoop.conf.Configuration;

public class FDSCredential {
  public static final String ACCESS_KEY_PROPERTY = "fs.fds.AccessKey";
  public static final String ACCESS_SECRET_PROPERTY = "fs.fds.AccessSecret";

  private String accessKey;
  private String accessSecret;

  /**
   URI with key and secret  fds://ID:SECRET@BUCKET/test
   Keys specified in the URI take precedence over any
   specified using the properties fs.fds.AccessKey and fs.fds.AccessSecret.
   */
  public void initialize(URI uri, Configuration conf) {
    if (uri.getHost() == null) {
      throw new IllegalArgumentException("Invalid hostname in URI " + uri);
    }

    // First, try to parse key and secret from URI
    String userInfo = uri.getUserInfo();
    if (userInfo != null) {
      int index = userInfo.indexOf(':');
      if (index != -1) {
        accessKey = userInfo.substring(0, index);
        accessSecret = userInfo.substring(index + 1);
      } else {
        accessKey = userInfo;
      }
    }

    // Otherwise, try to get key and secret from configuration
    if (accessKey == null) {
      accessKey = conf.get(ACCESS_KEY_PROPERTY);
    }

    if (accessSecret == null) {
      accessSecret = conf.get(ACCESS_SECRET_PROPERTY);
    }

    if (accessKey == null && accessSecret == null) {
      throw new IllegalArgumentException("AWS " +
              "Access Key and Access Secret " +
              "must be specified as the " +
              "username or password " +
              "(respectively) of a fds " +
              "URL, or by setting the " +
              ACCESS_KEY_PROPERTY + " or " +
              ACCESS_SECRET_PROPERTY +
              " properties (respectively).");
    } else if (accessKey == null) {
      throw new IllegalArgumentException("AWS " +
              "Access Key ID must be specified " +
              "as the username of a fds " +
              "URL, or by setting the " +
              ACCESS_KEY_PROPERTY + " property.");
    } else if (accessSecret == null) {
      throw new IllegalArgumentException("AWS " +
              "Secret Access Key must be " +
              "specified as the password of a " +
              "fds URL, or by setting the " +
              ACCESS_SECRET_PROPERTY +
              " property.");
    }
  }

  public String getAccessSecret() {
    return accessSecret;
  }

  public String getAccessKey() {
    return accessKey;
  }
}

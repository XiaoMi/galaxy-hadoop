package com.xiaomi.infra.galaxy.hadoop.fs;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.testng.Assert;
import org.testng.annotations.Test;
/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenjiaqi@xiaomi.com
 */
public class TestFDSFileSystemStore extends FDSFileSystemStore {
  private Configuration getConfig() {
    Configuration conf = new Configuration();
    conf.set(FDSCredential.ACCESS_KEY_PROPERTY, "ak");
    conf.set(FDSCredential.ACCESS_SECRET_PROPERTY, "sk");
    return conf;
  }

  @Test
  public void testInitBucketInfo() throws URISyntaxException, IOException {
    // fds://bucket/path
    final String ENDPOINT_PREFIX = ".fds.api.xiaomi.com";
    initialize(new URI("fds://bucket-name/my/path"), getConfig());
    Assert.assertEquals(".fds.api.xiaomi.com", this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://ak:sk@bucket-name/my/path
    initialize(new URI("fds://ak:sk@bucket-name/my/path"), getConfig());
    Assert.assertEquals(".fds.api.xiaomi.com", this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://endpoint:bucket/path
    initialize(new URI("fds://bucket-name.cnbj1-fds.api.xiaomi.net/path"), getConfig());
    Assert.assertEquals("cnbj1-fds.api.xiaomi.net", this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://ak:sk@endpoint:bucket/path
    initialize(new URI("fds://ak:sk@bucket-name.cnbj1-fds.api.xiaomi.net/path"), getConfig());
    Assert.assertEquals("cnbj1-fds.api.xiaomi.net", this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://endpoint:bucket/path
    initialize(new URI("fds://bucket-name.cnbj1/path"), getConfig());
    Assert.assertEquals("cnbj1" + ENDPOINT_PREFIX, this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("cnbj1", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://ak:sk@endpoint:bucket/path
    initialize(new URI("fds://ak:sk@bucket-name.cnbj1/path"), getConfig());
    Assert.assertEquals("cnbj1" + ENDPOINT_PREFIX, this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("cnbj1", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://region-bucket/path
    initialize(new URI("fds://cnbj1-bucket-name/path"), getConfig());
    Assert.assertEquals("cnbj1" + ENDPOINT_PREFIX, this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("cnbj1", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);

    // fds://ak:sk@region-bucket/path
    initialize(new URI("fds://ak:sk@cnbj1-bucket-name/path"), getConfig());
    Assert.assertEquals("cnbj1" + ENDPOINT_PREFIX, this.fdsClientConfiguration.getEndpoint());
    Assert.assertEquals("cnbj1", this.fdsClientConfiguration.getRegionName());
    Assert.assertEquals("bucket-name", this.bucket);
  }
}

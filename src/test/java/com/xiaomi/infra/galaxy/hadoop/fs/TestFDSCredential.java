package com.xiaomi.infra.galaxy.hadoop.fs;

import java.net.URI;
import java.net.URISyntaxException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.DFSClient;
import org.junit.Assert;
import org.testng.annotations.Test;

/**
 * Copyright 2015, Xiaomi.
 * All rights reserved.
 * Author: shenjiaqi@xiaomi.com
 */
public class TestFDSCredential {
  private static final String AK = "ak";
  private static final String SK = "sk";
  private static final String AK_ESCAPED = "ak+blah";
  private static final String SK_ESCAPED = "sk%2Fblah";

  private Configuration getEmptyConfig() {
    Configuration conf = new Configuration();
    return conf;
  }

  private Configuration getAkSKCnofig() {
    Configuration conf = getEmptyConfig();
    conf.set(FDSCredential.ACCESS_KEY_PROPERTY, AK);
    conf.set(FDSCredential.ACCESS_SECRET_PROPERTY, SK);
    return conf;
  }

  private Configuration getEscapedConfig() {
    Configuration conf = getEmptyConfig();
    conf.set(FDSCredential.ACCESS_KEY_PROPERTY, AK_ESCAPED);
    conf.set(FDSCredential.ACCESS_SECRET_PROPERTY, SK_ESCAPED);
    return conf;
  }

  @Test
  public void testGetCredentialFromURI() throws URISyntaxException {

    FDSCredential credential = new FDSCredential();
    // fds://ak:skbucket/path
    credential.initialize(new URI("fds://ak:sk@bucket-name/my/path"), getEmptyConfig());
    Assert.assertEquals(AK, credential.getAccessKey());
    Assert.assertEquals(SK, credential.getAccessSecret());

    // test escape
    credential.initialize(new URI("fds://" + AK_ESCAPED + ":" + SK_ESCAPED + "@bucket-name/my/path"), getEmptyConfig());
    Assert.assertEquals("ak+blah", credential.getAccessKey());
    Assert.assertEquals("sk/blah", credential.getAccessSecret());

    // test setting credential from property
    credential.initialize(new URI("fds://bucket-name/my/path"), getEscapedConfig());
    Assert.assertEquals("ak+blah", credential.getAccessKey());
    Assert.assertEquals("sk/blah", credential.getAccessSecret());
  }
}

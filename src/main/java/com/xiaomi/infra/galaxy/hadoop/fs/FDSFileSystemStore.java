package com.xiaomi.infra.galaxy.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.util.Date;
import java.util.HashSet;
import java.util.Set;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.fds.auth.signature.XiaomiHeader;
import com.xiaomi.infra.galaxy.fds.client.FDSClientConfiguration;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDS;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata;

public class FDSFileSystemStore implements FileSystemStore {
  private static final Log LOG =
          LogFactory.getLog(FDSFileSystemStore.class);

  private Configuration conf;
  protected FDSClientConfiguration fdsClientConfiguration;
  private GalaxyFDS fdsClient;
  protected String bucket;
  private boolean enableThirdPart = false;
  private static final Set<String> VALID_REGION_SET = new HashSet<String>();
  static {
    Configuration.addDefaultResource("galaxy-site.xml");
    Configuration config = new Configuration();
    String validRegions[] = config.get(FDSConfiguration.GALAXY_FDS_REGIONS,
        FDSConfiguration.DEFAULT_GALAXY_FDS_REGIONS).split(";");
    for (String regionName: validRegions) {
      if (!Strings.isNullOrEmpty(regionName)) {
        VALID_REGION_SET.add(regionName);
      }
    }
  }

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    this.conf = conf;

    FDSCredential fdsCredential = new FDSCredential();
    fdsCredential.initialize(uri, conf);

    GalaxyFDSCredential credential = new BasicFDSCredential(
            fdsCredential.getAccessKey(), fdsCredential.getAccessSecret());

    // Use the following Configuration object to configure the Galaxy FDS.

    // URI eg, fds://ID:SECRET@BUCKET.REGION/object
    //         fds://ID:SECRET@BUCKET.ENDPOINT/object
    initializeRegionBucketInfo(uri, conf);

    fdsClientConfiguration = FDSConfiguration.getFdsClientConfig(conf);
    fdsClient = new GalaxyFDSClient(credential, fdsClientConfiguration);
    enableThirdPart = conf.getBoolean(FDSConfiguration.GALAXY_FDS_SERVER_ENABLE_THIRD_PART,
        FDSConfiguration.DEFAULT_GALAXY_FDS_SERVER_ENABLE_THIRD_PART);
  }

  protected void initializeRegionBucketInfo(URI uri, Configuration conf) {
    String resourceStr = uri.getHost();
    Preconditions.checkArgument(resourceStr != null && resourceStr.length() > 0,
        "resourceStr in uri does not exist, uri: " + uri);
    // bucketname.endpoint or bucketname.region
    if (resourceStr.contains(".")) {
      String[] parts = resourceStr.split("\\.", 2);
      bucket = parts[0];
      Preconditions.checkArgument(!Strings.isNullOrEmpty(bucket),
          "bucket name is empty, uri: " + uri);
      if (parts[1].contains(".")) {
        // bucketname.endpoint
        String endpoint = parts[1];
        Preconditions.checkArgument(!Strings.isNullOrEmpty(endpoint),
            "endpoint is null, uri: " + uri);
        conf.set(FDSConfiguration.GALAXY_FDS_SERVER_ENDPOINT, endpoint);
      } else {
        // bucketname.region
        String region = parts[1];
        conf.set(FDSConfiguration.GALAXY_FDS_SERVER_REGION, region);
      }
      return;
    }

    // region-bucketname
    if (resourceStr.contains("-")) {
      String[] parts = resourceStr.split("-", 2);
      if (VALID_REGION_SET.contains(parts[0])) {
        // assert region is in valie reg
        String region = parts[0];
        if (region.equals("cnbj0")) {
          region = "";
        }
        conf.set(FDSConfiguration.GALAXY_FDS_SERVER_REGION, region);
        bucket = Preconditions.checkNotNull(parts[1], "bucket is null, uri: " + uri);
        LOG.info("relic scheme found [fds://REGION-BUCKET/object], " +
            "use [fds://BUCKET.REGION/object] instead");
        return;
      }
    }

    // bucketname
    bucket = resourceStr;
  }

  @Override
  public FileMetadata getMetadata(String object) throws IOException {
    FDSObjectMetadata metadata;
    try {
      metadata = fdsClient.getObjectMetadata(bucket, object);
    } catch (GalaxyFDSClientException e) {
      return null;
    }

    long contentLength = Long.parseLong(metadata.getRawMetadata()
        .get(XiaomiHeader.CONTENT_LENGTH.getName()));
    Date lastModifiedTime = metadata.getLastModified();
    long time = 0;
    if (lastModifiedTime != null) {
      time = lastModifiedTime.getTime();
    }
    return new FileMetadata(object, contentLength, time);
  }

  public FDSObjectListing listSubPaths(String object) throws IOException {
    return listSubPaths(object, null);
  }

  @Override
  public FDSObjectListing listSubPaths(String object,
                                       FDSObjectListing previousList) throws IOException {
    return listSubPaths(object, previousList, "/");
  }

  @Override
  public FDSObjectListing listSubPaths(String object,
                                       FDSObjectListing previousList,
                                       String delimeter)
          throws IOException {
    String dirObject = "";
    if (!object.isEmpty()) {
      dirObject = object + "/";
    }

    try {
      if (previousList == null) {
        return fdsClient.listObjects(bucket, dirObject, delimeter);
      } else {
        Preconditions.checkArgument(bucket.equals(previousList.getBucketName()));
        Preconditions.checkArgument(dirObject.equals(previousList.getPrefix()));
        return fdsClient.listNextBatchOfObjects(previousList);
      }

    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public InputStream getObject(String object, long pos) throws IOException {
    try {
      FDSObject fdsObject = null;
      if( enableThirdPart ) {
        fdsObject = fdsClient.getObjectFromThirdParty(bucket, object, pos);
      } else {
        fdsObject = fdsClient.getObject(bucket, object, pos);
      }
      return fdsObject.getObjectContent();
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void putObject(String object, InputStream inputStream,
                        FDSObjectMetadata metatdata) throws IOException {
    try {
      fdsClient.putObject(bucket, object, inputStream, metatdata);
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void delete(String object) throws IOException {
    try {
      fdsClient.deleteObject(bucket, object);
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void storeEmptyFile(String object) throws IOException {
    FDSObjectMetadata metatdata = new FDSObjectMetadata();
    try {
      fdsClient.putObject(bucket, object, new ByteArrayInputStream(new byte[0]),
              metatdata);
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }

  @Override
  public void rename(String srcObject, String dstObject) throws IOException {
    try {
      fdsClient.renameObject(bucket, srcObject, dstObject);
    } catch (GalaxyFDSClientException e) {
      throw new IOException(e);
    }
  }
}

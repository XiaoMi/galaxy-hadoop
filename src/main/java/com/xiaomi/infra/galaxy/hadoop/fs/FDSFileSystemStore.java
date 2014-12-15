package com.xiaomi.infra.galaxy.hadoop.fs;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.Date;

import com.google.common.base.Preconditions;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.fds.auth.XiaomiHeader;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDS;
import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObject;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata;

public class FDSFileSystemStore implements FileSystemStore {
  private static final Log LOG =
          LogFactory.getLog(FDSFileSystemStore.class);

  private Configuration conf;
  private GalaxyFDS fdsClient;
  private String bucket;

  @Override
  public void initialize(URI uri, Configuration conf) throws IOException {
    this.conf = conf;

    FDSCredential fdsCredential = new FDSCredential();
    fdsCredential.initialize(uri, conf);

    GalaxyFDSCredential credential = new BasicFDSCredential(
            fdsCredential.getAccessKey(), fdsCredential.getAccessSecret());

    // Use the following Configuration object to configure the Galaxy FDS.
    URL resource = conf.getClassLoader().getResource("galaxy-site.xml");
    String fdsServer = conf.get(GalaxyFDSClient.GALAXY_FDS_SERVER_BASE_URI_KEY);
    if (fdsServer == null || fdsServer.isEmpty()) {
      throw new IOException("Invalid FDS server set: " + fdsServer);
    }

    // URI eg, fds://ID:SECRET@BUCKET/object
    bucket = uri.getHost();

    fdsClient = new GalaxyFDSClient(credential, conf);
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
      FDSObject fdsObject = fdsClient.getObject(bucket, object, pos);
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

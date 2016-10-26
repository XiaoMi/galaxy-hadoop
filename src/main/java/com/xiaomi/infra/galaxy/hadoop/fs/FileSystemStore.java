package com.xiaomi.infra.galaxy.hadoop.fs;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;

import org.apache.hadoop.conf.Configuration;

import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.model.FDSObjectMetadata;

interface FileSystemStore {
  void initialize(URI uri, Configuration conf) throws IOException;

  FileMetadata getMetadata(String key) throws IOException;

  FDSObjectListing listSubPaths(String object) throws IOException;

  FDSObjectListing listSubPaths(String object,
                                FDSObjectListing previousList)
      throws IOException;

  FDSObjectListing listSubPaths(String object,
                                FDSObjectListing previousList,
                                String delimeter)
      throws IOException;

  InputStream getObject(String object, long pos) throws IOException;

  void putObject(String object, InputStream inputStream,
                 FDSObjectMetadata metatdata) throws IOException;

  void delete(String object) throws IOException;

  void storeEmptyFile(String object) throws IOException;

  void rename(String srcObject, String dstObject) throws IOException;
}

package com.xiaomi.infra.galaxy.hadoop.fs;

public class FileMetadata {
  private final String key;
  private final long length;
  private final long lastModified;

  public FileMetadata(String key, long length, long time) {
    this.key = key;
    this.length = length;
    this.lastModified = time;
  }

  public String getKey() {
    return key;
  }

  public long getLength() {
    return length;
  }

  public long getLastModified() {
    return lastModified;
  }

  @Override
  public String toString() {
    return "FileMetadata[" + key + ", " + length + ", " + lastModified + "]";
  }
}

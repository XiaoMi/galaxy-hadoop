package com.xiaomi.infra.galaxy.hadoop.fs.shell;

public enum CommandEnum {
  CREATE_BUCKET("c", "create_bucket", CreateBucket.class),
  DELETE_BUCKET("d", "delete_bucket", DeleteBucket.class),
  LIST_BUCKETS("l", "list_buckets", ListBuckets.class),
  LIST_OBJECTS("L", "list_objects", ListObjects.class),
  ADD_BUCKET_GRANT("a", "add_bucket_grant", AddBucketGrant.class),
  ADD_OBJECT_GRANT("A", "add_object_grant", AddObjectGrant.class);

  private CommandEnum(String shortName, String longName,
      Class<? extends FDSCommand> clazz) {
    this.shortName = shortName;
    this.longName = longName;
    this.clazz = clazz;
  }

  private final String shortName;
  private final String longName;
  private final Class<? extends FDSCommand> clazz;

  String getShortName() {
    return shortName;
  }

  String getLongName() {
    return longName;
  }

  Class<? extends FDSCommand> getClazz() {
    return clazz;
  }
}

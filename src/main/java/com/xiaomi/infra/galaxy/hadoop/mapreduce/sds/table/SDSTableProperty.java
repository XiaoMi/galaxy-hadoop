package com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.table;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import com.xiaomi.infra.galaxy.hadoop.mapreduce.sds.SDSConfiguration;
import com.xiaomi.infra.galaxy.sds.client.ClientFactory;
import com.xiaomi.infra.galaxy.sds.thrift.AdminService;
import com.xiaomi.infra.galaxy.sds.thrift.CommonConstants;
import com.xiaomi.infra.galaxy.sds.thrift.Credential;
import com.xiaomi.infra.galaxy.sds.thrift.TableService;
import com.xiaomi.infra.galaxy.sds.thrift.UserType;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableUtils;

public class SDSTableProperty implements Writable {
  private String endpoint;
  private String restAdminPath;
  private String restTablePath;

  private String secretID;
  private String secretKey;
  private Integer clientMaxRetry = null;

  public String getEndpoint() {
    return endpoint;
  }

  public SDSTableProperty setEndpoint(String endpoint) {
    this.endpoint = endpoint;
    return this;
  }

  public String getRestAdminPath() {
    return restAdminPath;
  }

  public SDSTableProperty setRestAdminPath(String restAdminPath) {
    this.restAdminPath = restAdminPath;
    return this;
  }

  public String getRestTablePath() {
    return restTablePath;
  }

  public SDSTableProperty setRestTablePath(String restTablePath) {
    this.restTablePath = restTablePath;
    return this;
  }

  public String getSecretID() {
    return secretID;
  }

  public SDSTableProperty setSecretID(String secretID) {
    this.secretID = secretID;
    return this;
  }

  public String getSecretKey() {
    return secretKey;
  }

  public SDSTableProperty setSecretKey(String secretKey) {
    this.secretKey = secretKey;
    return this;
  }

  public int getClientMaxRetry() {
    return clientMaxRetry;
  }

  public SDSTableProperty setClientMaxRetry(int clientMaxRetry) {
    this.clientMaxRetry = clientMaxRetry;
    return this;
  }

  public void checkSanityAndSet(Configuration conf) {
    if (endpoint == null) {
      endpoint = conf.get(SDSConfiguration.SDS_MAPREDUCE_REST_ENDPOINT);
      if (endpoint == null) {
        throw new IllegalArgumentException("SDS Property not valid, not set endpoint");
      }
    }

    if (secretID == null) {
      secretID = conf.get(SDSConfiguration.SDS_MAPREDUCE_SECRET_ID);
      if (secretID == null) {
        throw new IllegalArgumentException("SDS Property not valid, not set secretID");
      }
    }

    if (secretKey == null) {
      secretKey = conf.get(SDSConfiguration.SDS_MAPREDUCE_SECRET_KEY);
      if (secretKey == null) {
        throw new IllegalArgumentException("SDS Property not valid, not set secretKey");
      }
    }

    if (restAdminPath == null) {
      restAdminPath = conf.get(SDSConfiguration.SDS_MAPREDUCE_REST_ADMIN_PATH,
                               CommonConstants.ADMIN_SERVICE_PATH);
    }

    if (restTablePath == null) {
      restTablePath = conf.get(SDSConfiguration.SDS_MAPREDUCE_REST_TABLE_PATH,
                               CommonConstants.TABLE_SERVICE_PATH);
    }

    if (clientMaxRetry == null) {
      clientMaxRetry = conf.getInt(SDSConfiguration.SDS_MAPREDUCE_CLIENT_MAX_RETRY,
                                   SDSConfiguration.DEFAULT_SDS_MAPREDUCE_CLIENT_MAX_RETRY);
    }
  }

  @Override
  public void write(DataOutput out) throws IOException {
    WritableUtils.writeCompressedString(out, endpoint);
    WritableUtils.writeCompressedString(out, restAdminPath);
    WritableUtils.writeCompressedString(out, restTablePath);

    WritableUtils.writeCompressedString(out, secretID);
    WritableUtils.writeCompressedString(out, secretKey);
    WritableUtils.writeVInt(out, clientMaxRetry);
  }

  @Override
  public void readFields(DataInput in) throws IOException {
    endpoint = WritableUtils.readCompressedString(in);
    restAdminPath = WritableUtils.readCompressedString(in);
    restTablePath = WritableUtils.readCompressedString(in);

    secretID = WritableUtils.readCompressedString(in);
    secretKey = WritableUtils.readCompressedString(in);
    clientMaxRetry = WritableUtils.readVInt(in);
  }

  public AdminService.Iface formAdminClient() {
    Credential credential = new Credential()
        .setSecretKeyId(secretID)
        .setSecretKey(secretKey)
        .setType(UserType.APP_SECRET);
    ClientFactory clientFactory = new ClientFactory(credential);
    AdminService.Iface adminClient = clientFactory
        .newAdminClient(endpoint + restAdminPath, clientMaxRetry > 0, clientMaxRetry);
    return adminClient;
  }

  public TableService.Iface formTableClient() {
    Credential credential = new Credential()
        .setSecretKeyId(secretID)
        .setSecretKey(secretKey)
        .setType(UserType.APP_SECRET);
    ClientFactory clientFactory = new ClientFactory(credential);
    TableService.Iface tableClient = clientFactory
        .newTableClient(endpoint + restTablePath, clientMaxRetry > 0, clientMaxRetry);
    return tableClient;
  }
}

package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.model.FDSBucket;
import com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList;

public class ListBuckets extends FDSCommand {

  public static final String NAME = "ListBuckets";
  public static final String DESCRIPTION = "List all the buckets";

  public ListBuckets(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.LIST_BUCKETS;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    optionList.add(OptionBuilder
        .withDescription("list all the buckets")
        .withLongOpt(CommandEnum.LIST_BUCKETS.getLongName())
        .create(CommandEnum.LIST_BUCKETS.getShortName()));
    return optionList;
  }

  @Override
  protected void processRawArguments(LinkedList<String> args)
      throws IOException {
    try {
      List<FDSBucket> buckets = fdsClient.listBuckets();
      if (buckets != null) {
        out.println("Total " + buckets.size() + " bucket(s)");
        for (FDSBucket bucket : buckets) {
          AccessControlList acl = fdsClient.getBucketAcl(bucket.getName());
          out.println(bucket.getName() + "\t" + formatAclInfo(acl));
        }
      } else {
        out.println("No buckets listed");
      }
    } catch (GalaxyFDSClientException e) {
      throw new IOException("List buckets failed", e);
    }
  }
}

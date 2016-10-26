package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList;

public class AddBucketGrant extends FDSCommand {

  public AddBucketGrant(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.ADD_BUCKET_GRANT;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    String description = "add grant for a bucket: \"[U|G]:uid:perm\"";
    optionList.add(OptionBuilder
        .withDescription(description)
        .withLongOpt(commandEnum.getLongName())
        .hasArgs(2)
        .withValueSeparator(' ')
        .withArgName("bucket> <grant")
        .create(commandEnum.getShortName()));
    return optionList;
  }

  @Override
  protected void processRawArguments(LinkedList<String> args)
      throws IOException {
    if (args.isEmpty()) {
      printUsage();
    } else {
      processOptions(args);
      String[] values = commandLine.getOptionValues(
          commandEnum.getShortName());
      String bucketName = values[0];
      String grantStr = values[1];

      AccessControlList.Grant grant = parseGrantFromString(grantStr);
      AccessControlList acl = new AccessControlList();
      acl.addGrant(grant);
      try {
        fdsClient.setBucketAcl(bucketName, acl);
      } catch (GalaxyFDSClientException e) {
        throw new IOException("Set bucket acl failed", e);
      }
    }
  }
}

package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList;

public class AddObjectGrant extends FDSCommand {

  public AddObjectGrant(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.ADD_OBJECT_GRANT;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    String description = "add grant for an object: \"[U|G]:uid:perm\"";
    optionList.add(OptionBuilder
        .withDescription(description)
        .withLongOpt(commandEnum.getLongName())
        .hasArgs(3)
        .withValueSeparator(' ')
        .withArgName("bucket> <object> <grant")
        .create(commandEnum.getShortName()));
    return optionList;
  }

  @Override
  protected void processRawArguments(LinkedList<String> args)
      throws IOException {
    if (args.isEmpty()) {
    } else {
      processOptions(args);
      String[] values = commandLine.getOptionValues(
          commandEnum.getShortName());
      String bucketName = values[0];
      String objectName = values[1];
      String grantStr = values[2];

      AccessControlList.Grant grant = parseGrantFromString(grantStr);
      AccessControlList acl = new AccessControlList();
      acl.addGrant(grant);
      try {
        fdsClient.setObjectAcl(bucketName, objectName, acl);
      } catch (GalaxyFDSClientException e) {
        throw new IOException("Add grant for object failed", e);
      }
    }
  }
}

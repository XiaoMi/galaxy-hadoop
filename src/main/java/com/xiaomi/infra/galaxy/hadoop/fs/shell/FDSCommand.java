package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.shell.Command;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.Grant;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.GrantType;
import com.xiaomi.infra.galaxy.fds.client.model.AccessControlList.Permission;

public abstract class FDSCommand extends Command {

  protected final GalaxyFDSClient fdsClient;
  protected CommandEnum commandEnum;
  protected Options options;
  protected CommandLine commandLine;

  protected FDSCommand(GalaxyFDSClient fdsClient) {
    this.fdsClient = fdsClient;
  }

  @Override
  public String getCommandName() {
    return getName();
  }

  @Override
  protected void processOptions(LinkedList<String> args) throws IOException {
    options = new Options();
    for (Option option : getOptions()) {
      options.addOption(option);
    }

    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(options, args.toArray(new String[0]));
    } catch (ParseException e) {
      throw new IOException("Parse command line args failed", e);
    }
  }

  public abstract List<Option> getOptions();

  @Override
  protected void run(Path path) throws IOException {
    throw new IOException("Shouldn't arrive here");
  }

  protected void printUsage() {
    HelpFormatter helpFormatter = new HelpFormatter();
    helpFormatter.printHelp(getDescription(), options);
  }

  protected static String formatAclInfo(AccessControlList acl) {
    List<Grant> grantList = acl.getGrantList();
    StringBuilder result = new StringBuilder();

    for (Grant g : grantList) {
      if (!result.toString().isEmpty()) {
        result.append(",");
      }

      if (GrantType.GROUP.equals(g.getType())) {
        result.append("G");
      } else {
        result.append("U");
      }
      result.append(":").append(g.getGranteeId())
          .append(":").append(g.getPermission());
    }
    return result.toString();
  }

  protected static Grant parseGrantFromString(String grantStr) {
    String[] token = grantStr.split(":");
    if (token.length != 3) {
      return null;
    }

    GrantType grantType;
    if ("U".equals(token[0])) {
      grantType = GrantType.USER;
    } else if ("G".equals(token[0])) {
      grantType = GrantType.GROUP;
    } else {
      return null;
    }

    String granteeId = token[1];
    Permission perm = Permission.valueOf(token[2]);
    return new Grant(granteeId, perm, grantType);
  }
}

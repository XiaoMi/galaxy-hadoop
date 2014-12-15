package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectListing;
import com.xiaomi.infra.galaxy.fds.client.model.FDSObjectSummary;
import com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException;
import com.xiaomi.infra.galaxy.fds.model.AccessControlList;

public class ListObjects extends FDSCommand {

  public static final String NAME = "ListObjects";
  public static final String DESCRIPTION =
      "List objects under specified bucket with specified prefix";

  public ListObjects(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.LIST_OBJECTS;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    optionList.add(OptionBuilder
        .withDescription("list objects under specified bucket")
        .withLongOpt(CommandEnum.LIST_OBJECTS.getLongName())
        .hasArg()
        .withArgName("bucket")
        .create(CommandEnum.LIST_OBJECTS.getShortName()));

    optionList.add(OptionBuilder
        .withDescription("the prefix to list objects")
        .withLongOpt("prefix")
        .hasArg()
        .withArgName("prefix")
        .create("p"));
    return optionList;
  }

  @Override
  protected void processRawArguments(LinkedList<String> args)
      throws IOException {
    if (args.isEmpty()) {
      printUsage();
    } else {
      processOptions(args);
      String bucketName = commandLine.getOptionValue(
          commandEnum.getShortName());
      String prefix = commandLine.getOptionValue("p", "");

      while (true) {
        try {
          FDSObjectListing listing = fdsClient.listObjects(bucketName, prefix);
          if (listing == null) {
            break;
          }

          int count = listing.getCommonPrefixes().size() +
              listing.getObjectSummaries().size();
          out.println("Total " + count + " object(s) in this page");

          List<String> commonPrefixes = listing.getCommonPrefixes();
          for (String p : commonPrefixes) {
            out.println(p);
          }

          List<FDSObjectSummary> summaries = listing.getObjectSummaries();
          for (FDSObjectSummary s : summaries) {
            AccessControlList acl = fdsClient.getObjectAcl(
                s.getBucketName(), s.getObjectName());
            out.println(s.getObjectName() + "\t" + formatAclInfo(acl));
          }

          if (listing.isTruncated()) {
            byte[] buffer = new byte[10];
            out.print("\nDo you want to show the next page of objects?(y/N)");
            System.in.read(buffer);
            String confirm = new String(buffer);
            if ("y".equalsIgnoreCase(confirm.trim())) {
              continue;
            }
          }
          break;
        } catch (GalaxyFDSClientException e) {
          throw new IOException("List objects failed", e);
        }
      }
    }
  }
}

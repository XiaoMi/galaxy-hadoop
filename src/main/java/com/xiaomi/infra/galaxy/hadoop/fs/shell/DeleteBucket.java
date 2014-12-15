package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.exception.GalaxyFDSClientException;

public class DeleteBucket extends FDSCommand {

  public static final String NAME = "DeleteBucket";
  public static final String DESCRIPTION =
      "Delete bucket with specified name";

  public DeleteBucket(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.DELETE_BUCKET;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    optionList.add(OptionBuilder
        .withDescription("delete specified bucket")
        .withLongOpt(CommandEnum.DELETE_BUCKET.getLongName())
        .hasArg()
        .withArgName("bucket")
        .create(CommandEnum.DELETE_BUCKET.getShortName()));
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
      try {
        fdsClient.deleteBucket(bucketName);
        out.println("Bucket " + bucketName + " is deleted");
      } catch (GalaxyFDSClientException e) {
        throw new IOException("Delete bucket failed", e);
      }
    }
  }
}

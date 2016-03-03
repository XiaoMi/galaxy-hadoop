package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.io.IOException;
import java.util.LinkedList;
import java.util.List;

import org.apache.commons.cli.Option;
import org.apache.commons.cli.OptionBuilder;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.exception.GalaxyFDSClientException;

public class CreateBucket extends FDSCommand {

  public static final String NAME = "CreateBucket";
  public static final String DESCRIPTION =
      "Create bucket with specified name";

  public CreateBucket(GalaxyFDSClient fdsClient) {
    super(fdsClient);
    this.commandEnum = CommandEnum.CREATE_BUCKET;
  }

  @Override
  public List<Option> getOptions() {
    List<Option> optionList = new LinkedList<Option>();
    optionList.add(OptionBuilder
        .withDescription("create specified bucket")
        .withLongOpt(CommandEnum.CREATE_BUCKET.getLongName())
        .hasArg()
        .withArgName("bucket")
        .create(CommandEnum.CREATE_BUCKET.getShortName()));
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
        fdsClient.createBucket(bucketName);
        out.println("Bucket " + bucketName + " is created successfully");
      } catch (GalaxyFDSClientException e) {
        throw new IOException("Create bucket failed", e);
      }
    }
  }
}

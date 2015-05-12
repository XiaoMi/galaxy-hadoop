package com.xiaomi.infra.galaxy.hadoop.fs.shell;

import java.lang.reflect.Constructor;
import java.util.HashMap;
import java.util.Map;

import com.xiaomi.infra.galaxy.hadoop.fs.FDSConfiguration;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.GnuParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.shell.Command;
import org.apache.hadoop.fs.shell.CommandFactory;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xiaomi.infra.galaxy.fds.client.GalaxyFDSClient;
import com.xiaomi.infra.galaxy.fds.client.credential.BasicFDSCredential;
import com.xiaomi.infra.galaxy.fds.client.credential.GalaxyFDSCredential;

public class FDSAdmin extends Configured implements Tool {

  private GalaxyFDSClient fdsClient;
  private CommandFactory commandFactory;
  private final Map<String, FDSCommand> commands =
      new HashMap<String, FDSCommand>();
  private CommandLine commandLine;
  private Options options;

  private static final String USAGE_PREFIX =
      "fds admin [generic options]";
  private static final Log LOG = LogFactory.getLog(FDSAdmin.class);

  private static final String GALAXY_FDS_ACCESS_KEY =
      "fs.fds.AccessKey";
  private static final String GALAXY_FDS_ACCESS_SECRET =
      "fs.fds.AccessSecret";

  static {
    Configuration.addDefaultResource("galaxy-site.xml");
  }

  public FDSAdmin(Configuration conf) {
    super(conf);
  }

  private void init() {
    String key = getConf().get(GALAXY_FDS_ACCESS_KEY);
    String secret = getConf().get(GALAXY_FDS_ACCESS_SECRET);
    GalaxyFDSCredential credential = new BasicFDSCredential(key, secret);
    fdsClient = new GalaxyFDSClient(credential, FDSConfiguration.getFdsClientConfig(getConf()));

    getConf().setQuietMode(true);
    commandFactory = new CommandFactory(getConf());
    registerCommands();
  }

  private void processCommandLine(String[] argv) {
    options = new Options();
    options.addOption("h", "help", false, "print this message");

    for (CommandEnum cmd : CommandEnum.values()) {
      FDSCommand command = (FDSCommand) commandFactory.getInstance(
          "-" + cmd.getShortName());
      for (Option option : command.getOptions()) {
        options.addOption(option);
      }
    }

    CommandLineParser parser = new GnuParser();
    try {
      commandLine = parser.parse(options, argv, false);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Parse command line args failed");
    }
  }

  private void printUsage() {
    HelpFormatter formatter = new HelpFormatter();
    formatter.printHelp(120, USAGE_PREFIX, null, options, null);
  }

  private void registerCommands() {
    for (CommandEnum cmd : CommandEnum.values()) {
      try {
        Constructor<? extends FDSCommand> ctor = cmd.getClazz()
            .getConstructor(GalaxyFDSClient.class);
        FDSCommand command = ctor.newInstance(fdsClient);
        commands.put("--" + cmd.getLongName(), command);
        commands.put("-" + cmd.getShortName(), command);
      } catch (Exception e) {
        throw new IllegalArgumentException("Invalid cmd:" + cmd, e);
      }
    }

    for (Map.Entry<String, FDSCommand> entry : commands.entrySet()) {
      commandFactory.addObject(entry.getValue(), entry.getKey());
    }
  }

  private void close() {}

  @Override
  public int run(String[] argv) throws Exception {
    init();
    processCommandLine(argv);

    int exitCode = -1;
    if (argv.length < 1) {
      printUsage();
    } else {
      String cmd = argv[0];
      try {
        Command instance = commandFactory.getInstance(cmd);
        if (instance == null) {
          printUsage();
        } else {
          exitCode = instance.run(argv);
        }
      } catch (IllegalArgumentException e) {
        displayError(cmd, e.getLocalizedMessage());
        printUsage();
      } catch (Exception e) {
        displayError(cmd, "Fatal internal error");
        e.printStackTrace(System.err);
        printUsage();
      }
    }
    return exitCode;
  }

  private void displayError(String cmd, String message) {
    for (String line : message.split("\n")) {
      System.err.println(cmd + ": " + line);
      if (cmd.charAt(0) != '-') {
        Command instance;
        instance = commandFactory.getInstance("-" + cmd);
        if (instance != null) {
          System.err.println("Did you mean -" + cmd + "?  This command " +
              "begins with a dash.");
        }
      }
    }
  }

  public static void main(String[] argv) throws Exception {
    FDSAdmin admin = new FDSAdmin(new Configuration());
    int exitCode = -1;
    try {
      exitCode = ToolRunner.run(admin, argv);
    } finally {
      admin.close();
    }
    System.exit(exitCode);
  }
}

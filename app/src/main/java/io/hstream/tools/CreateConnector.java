package io.hstream.tools;

import static io.hstream.tools.Utils.readStreams;

import com.google.gson.Gson;
import io.hstream.ConnectorType;
import io.hstream.CreateConnectorRequest;
import io.hstream.HStreamClient;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class CreateConnector {
  private static final Logger log = LoggerFactory.getLogger(CreateConnector.class);

  static class SinkConfig {
    String stream;
    String host;
    int port;
    String user;
    String password;
    String database;
    String table;
  }

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options);

    if (options.helpRequested) {
      CommandLine.usage(options, System.out);
      return;
    }

    HStreamClient client =
        HStreamClient.builder().serviceUrl(options.serviceUrl).requestTimeoutMs(60000).build();

    var service = Executors.newFixedThreadPool(options.threadCount);

    if (options.clear) {
      client.listConnectors().parallelStream().forEach(c -> client.deleteConnector(c.getName()));
      System.out.println("delete all connectors");
    }

    var streams = readStreams(options.path);
    System.out.printf("read from streams : %s\n", String.join(",", streams));

    var sinkConfig = new SinkConfig();
    sinkConfig.host = options.sinkOption.host;
    sinkConfig.port = options.sinkOption.port;
    sinkConfig.user = options.sinkOption.user;
    sinkConfig.password = options.sinkOption.password;
    sinkConfig.database = options.sinkOption.database;
    sinkConfig.table = options.sinkOption.table;

    var gson = new Gson();
    var countDown = new CountDownLatch(streams.size());

    for (int i = 0; i < streams.size(); i++) {
      sinkConfig.stream = streams.get(i);
      var cfg = gson.toJson(sinkConfig);
      var req =
          CreateConnectorRequest.newBuilder()
              .name("connector" + i)
              .type(ConnectorType.valueOf(options.connectorType))
              .target(options.sinkOption.target)
              .config(cfg)
              .build();
      service.submit(
          () -> {
            try {
              client.createConnector(req);
              countDown.countDown();
            } catch (Exception e) {
              log.error("create connector failed", e);
            } finally {
              countDown.countDown();
            }
          });
    }

    countDown.await();
    service.shutdown();
    service.awaitTermination(1, java.util.concurrent.TimeUnit.MINUTES);
  }

  static class SinkOption {
    @CommandLine.Option(
        names = "--target",
        description = "sink connector target, e.g. [mysql|blackhole|postgresql]")
    String target;

    @CommandLine.Option(names = "--host", description = "sink connector host")
    String host;

    @CommandLine.Option(names = "--port", description = "sink connector port")
    int port;

    @CommandLine.Option(names = "--user", description = "sink connector user name")
    String user;

    @CommandLine.Option(names = "--password", description = "sink connector password")
    String password;

    @CommandLine.Option(names = "--database", description = "sink connector database")
    String database;

    @CommandLine.Option(names = "--table", description = "sink connector table")
    String table;

    @Override
    public String toString() {
      return "SinkOption{"
          + "sinkType='"
          + target
          + '\''
          + ", host='"
          + host
          + '\''
          + ", port="
          + port
          + ", user='"
          + user
          + '\''
          + ", password='"
          + password
          + '\''
          + ", database='"
          + database
          + '\''
          + ", table='"
          + table
          + '\''
          + '}';
    }
  }

  static class Options {
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "hstream://127.0.0.1:6570";

    @CommandLine.Option(
        names = "--streams",
        description =
            "The path to the file containing the names of all streams, one stream per line")
    String path = "";

    @CommandLine.Option(
        names = "--thread-count",
        description = "threads count use to create connector.")
    int threadCount = 4;

    @CommandLine.Option(
        names = "--connector-type",
        description = "connector type, e.g. [sink|source]")
    String connectorType = "sink";

    @CommandLine.ArgGroup(exclusive = false)
    SinkOption sinkOption = new SinkOption();

    @CommandLine.Option(names = "--clear", description = "remove all connectors first")
    boolean clear = false;

    @Override
    public String toString() {
      return "Options{"
          + "helpRequested="
          + helpRequested
          + ", serviceUrl='"
          + serviceUrl
          + '\''
          + ", path='"
          + path
          + '\''
          + ", threadCount="
          + threadCount
          + ", connectorType='"
          + connectorType
          + '\''
          + ", sinkOption="
          + sinkOption
          + ", clear="
          + clear
          + '}';
    }
  }
}

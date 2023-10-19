package io.hstream.tools;

import io.hstream.HStreamClient;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class WriteLatencyBench {
  private static final AtomicLong successAppends = new AtomicLong();
  private static final AtomicLong failedAppends = new AtomicLong();
  private static final AtomicBoolean terminateFlag = new AtomicBoolean(false);
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options);

    if (options.helpRequested) {
      CommandLine.usage(options, System.out);
      return;
    }

    HStreamClient client;
    if (options.enableTLS) {
      client =
          HStreamClient.builder()
              .serviceUrl(options.serviceUrl)
              .enableTls()
              .tlsCaPath(options.caPath)
              .build();
    } else {
      client = HStreamClient.builder().serviceUrl(options.serviceUrl).build();
    }

    var streamName = options.streamNamePrefix + UUID.randomUUID();
    client.createStream(streamName, options.streamReplicationFactor, options.shardCount);
    try {
      Thread.sleep(3000);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(1);
    }

    var producer = client.newProducer().stream(streamName).requestTimeoutMs(3000).build();
    int sendFrequency = options.sendFrequency;
    var record = Utils.makeRawRecord(options.recordSize);
    while (true) {
      try {
        long beginTs = System.currentTimeMillis();
        producer.write(record).get();
        long endTs = System.currentTimeMillis();
        System.out.println("append latency: " + (endTs - beginTs) + " ms");
        Thread.sleep(1000 / sendFrequency);
      } catch (Exception e) {
        e.printStackTrace();
        System.exit(1);
      }
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

    @CommandLine.Option(names = "--stream-name-prefix")
    String streamNamePrefix = "write_latency_test_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 10240; // bytes

    @CommandLine.Option(names = "--partition-keys")
    int partitionKeys = 1;

    @CommandLine.Option(names = "--send-frequency")
    int sendFrequency = 5;

    @CommandLine.Option(names = "--enable-tls")
    boolean enableTLS = false;

    @CommandLine.Option(names = "--ca-path")
    String caPath = "";
  }
}

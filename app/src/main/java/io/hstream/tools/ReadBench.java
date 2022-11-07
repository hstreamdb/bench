package io.hstream.tools;

import io.hstream.HStreamClient;
import java.util.ArrayList;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class ReadBench {
  private static final AtomicLong successReads = new AtomicLong();
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options);

    if (options.helpRequested) {
      CommandLine.usage(options, System.out);
      return;
    }

    if (options.benchmarkDuration > 0 && options.warm >= options.benchmarkDuration) {
      System.err.println("Warmup time must be less than benchmark duration");
      System.exit(1);
    }

    HStreamClient client = HStreamClient.builder().serviceUrl(options.serviceUrl).build();

    var streams = new ArrayList<String>();
    for (int i = 0; i < options.streamCnt; i++) {
      var streamName = options.streamNamePrefix + i;
      client.createStream(
          streamName,
          options.streamReplicationFactor,
          options.shardCount,
          options.streamBacklogDuration);
      streams.add(streamName);
    }

    var batchProducerService =
        new BufferedProduceService(
            client,
            streams,
            options.threadCount,
            options.rateLimit,
            options.batchProducerOpts,
            options.payloadOpts);
    System.out.println("Start write data to hstream.");
    batchProducerService.writeNBytes(options.totalWriteSize);

    System.out.println("wrote done");
    // Sleep for a period of time to allow the node's CPU and memory metrics to reset
    // to facilitate getting the correct monitoring data
    Thread.sleep(10000L);

    var consumeService =
        new ConsumeService(client, streams, options.actTimeout, warmupDone, successReads);
    consumeService.startConsume();

    if (options.warm > 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    long lastReportTs = System.currentTimeMillis();
    long lastReadSuccessReads = 0;

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000L);
      long now = System.currentTimeMillis();
      long successRead = successReads.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (successRead - lastReadSuccessReads) * 1000 / duration;
      double throughput =
          (double) (successRead - lastReadSuccessReads)
              * options.payloadOpts.recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastReportTs = now;
      lastReadSuccessReads = successRead;

      System.out.println(
          String.format("[Read]: %f record/s, throughput %f MB/s", successPerSeconds, throughput));
      benchDurationMs -= duration;
      if (benchDurationMs <= 0) {
        break;
      }
    }

    consumeService.stopConsume();
  }

  static class Options {
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "127.0.0.1:6570";

    @CommandLine.Option(names = "--stream-name-prefix")
    String streamNamePrefix = "read_bench_stream_";

    @CommandLine.Option(names = "--stream-count", description = "number of streams")
    int streamCnt = 16;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--thread-count", description = "threads count use to write.")
    int threadCount = streamCnt;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.BufferedProducerOpts batchProducerOpts = new Utils.BufferedProducerOpts();

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.PayloadOpts payloadOpts = new Utils.PayloadOpts();

    @CommandLine.Option(names = "--total-write-size", description = "in bytes")
    long totalWriteSize = 16 * 20L * 1024 * 1024 * 1024; // bytes

    @CommandLine.Option(names = "--ack-timeout", description = "in seconds")
    int actTimeout = 60;

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible.")
    long benchmarkDuration = 400; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 60; // seconds

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @Override
    public String toString() {
      return "Options{"
          + "helpRequested="
          + helpRequested
          + ", serviceUrl='"
          + serviceUrl
          + '\''
          + ", streamNamePrefix='"
          + streamNamePrefix
          + '\''
          + ", streamCnt="
          + streamCnt
          + ", shardCount="
          + shardCount
          + ", streamReplicationFactor="
          + streamReplicationFactor
          + ", streamBacklogDuration="
          + streamBacklogDuration
          + ", threadCount="
          + threadCount
          + ", batchProducerOpts="
          + batchProducerOpts
          + ", rateLimit="
          + rateLimit
          + ", payloadOpts="
          + payloadOpts
          + ", totalWriteSize="
          + totalWriteSize
          + ", actTimeout="
          + actTimeout
          + ", benchmarkDuration="
          + benchmarkDuration
          + ", warm="
          + warm
          + ", reportIntervalSeconds="
          + reportIntervalSeconds
          + '}';
    }
  }
}

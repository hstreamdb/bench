package io.hstream.tools;

import io.hstream.*;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class TailBench {
  private static long lastFetchedCount = 0;
  private static final AtomicLong fetchedCount = new AtomicLong();
  private static long lastSuccessAppends;
  private static long lastFailedAppends;
  private static final AtomicLong successAppends = new AtomicLong();
  private static final AtomicLong failedAppends = new AtomicLong();
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
      var streamName = options.streamNamePrefix + UUID.randomUUID();
      client.createStream(
          streamName,
          options.streamReplicationFactor,
          options.shardCount,
          options.streamBacklogDuration);
      streams.add(streamName);
    }
    System.out.printf("Stream names: %s\n", streams);

    var batchProducerService =
        new BufferedProduceService(
            client,
            streams,
            options.threadCount,
            options.rateLimit,
            options.batchProducerOpts,
            options.payloadOpts);

    var consumeService =
        new ConsumeService(client, streams, options.actTimeout, warmupDone, fetchedCount);

    batchProducerService.startService(warmupDone, successAppends, failedAppends);
    consumeService.startConsume();

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    long lastReportTs = System.currentTimeMillis();

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    var recordSize = options.payloadOpts.recordSize;
    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000L);
      long now = System.currentTimeMillis();
      long totalAppend = successAppends.get();
      long totalFailedAppend = failedAppends.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (totalAppend - lastSuccessAppends) * 1000 / duration;
      double failurePerSeconds = (double) (totalFailedAppend - lastFailedAppends) * 1000 / duration;
      double throughput =
          (double) (totalAppend - lastSuccessAppends) * recordSize * 1000 / duration / 1024 / 1024;

      lastSuccessAppends = totalAppend;
      lastFailedAppends = totalFailedAppend;

      long currentFetchedCount = fetchedCount.get();
      double fetchThroughput =
          (double) (currentFetchedCount - lastFetchedCount)
              * recordSize
              * 1000
              / duration
              / 1024
              / 1024;
      lastFetchedCount = currentFetchedCount;

      lastReportTs = now;

      System.out.printf(
          String.format(
              "[Append]: success %f record/s, failed %f record/s, throughput %f MB/s%n",
              successPerSeconds, failurePerSeconds, throughput));
      System.out.printf(String.format("[Fetch]: throughput %f MB/s%n", fetchThroughput));

      if (benchDurationMs != Long.MAX_VALUE) {
        benchDurationMs -= duration;
        if (benchDurationMs <= 0) {
          batchProducerService.stopProducer();
          break;
        }
      }
    }

    batchProducerService.stopService();
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
    int streamCnt = 32;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 3600;

    @CommandLine.Option(names = "--thread-count", description = "threads count use to write.")
    int threadCount = streamCnt;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.BufferedProducerOpts batchProducerOpts = new Utils.BufferedProducerOpts();

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.PayloadOpts payloadOpts = new Utils.PayloadOpts();

    @CommandLine.Option(names = "--ack-timeout", description = "in seconds")
    int actTimeout = 60;

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible.")
    long benchmarkDuration = 400; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 60; // seconds

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
          + ", actTimeout="
          + actTimeout
          + ", reportIntervalSeconds="
          + reportIntervalSeconds
          + ", benchmarkDuration="
          + benchmarkDuration
          + ", warm="
          + warm
          + '}';
    }
  }
}

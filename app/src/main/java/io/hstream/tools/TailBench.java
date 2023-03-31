package io.hstream.tools;

import io.hstream.*;
import io.hstream.tools.Stats.PeriodStats;
import io.hstream.tools.Stats.Stats;
import java.util.ArrayList;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class TailBench {
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);
  private static final Logger log = LoggerFactory.getLogger(TailBench.class);
  private static final Stats stats = new Stats();

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
        new ConsumeService(
            client,
            streams,
            options.actTimeout,
            warmupDone,
            stats,
            options.payloadOpts.recordSize,
            "latest");

    batchProducerService.startService(warmupDone, stats);
    consumeService.startConsume();

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
      stats.resetPubStats();
      stats.resetSubStats();
    }

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    printStats(options.reportIntervalSeconds, benchDurationMs);
    batchProducerService.stopProducer();
    batchProducerService.stopService();
    consumeService.stopConsume();
  }

  private static void printStats(int reportIntervalSeconds, long benchDurationMs) {
    long statTime = System.nanoTime();
    long oldTime = System.nanoTime();

    while (true) {
      try {
        Thread.sleep(reportIntervalSeconds * 1000L);
      } catch (InterruptedException e) {
        break;
      }

      PeriodStats periodStat = stats.getPeriodStats();
      long now = System.nanoTime();
      double elapsed = (now - oldTime) / 1e9;
      benchDurationMs -= (now - oldTime) / 1e6;
      double publishRate = periodStat.messagesSent / elapsed;
      double publishThroughput = periodStat.bytesSent / elapsed / 1024 / 1024;
      double consumeRate = periodStat.messagesReceived / elapsed;
      double consumeThroughput = periodStat.bytesReceived / elapsed / 1024 / 1024;

      long backlog = Math.max(0L, periodStat.totalMessagesSent - periodStat.totalMessagesReceived);

      log.info(
          String.format(
              "Pub rate %.2f msg/s / %.2f MB/s | Consume rate %.2f msg/s %.2f MB/s | Backlog %.2f K "
                  + "| E2E Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
              publishRate,
              publishThroughput,
              consumeRate,
              consumeThroughput,
              backlog / 1000.0,
              Utils.getLatencyInMs(periodStat.endToEndLatency.getMean()),
              Utils.getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(50)),
              Utils.getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(90)),
              Utils.getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(99)),
              Utils.getLatencyInMs(periodStat.endToEndLatency.getMaxValueAsDouble())));

      oldTime = now;
      if (benchDurationMs <= 0) {
        break;
      }
    }

    long endTime = System.nanoTime();
    double elapsed = (endTime - statTime) / 1e9;
    double publishRate = stats.totalMessagesSent.sum() / elapsed;
    double publishThroughput = stats.totalBytesSent.sum() / elapsed / 1024 / 1024;
    double consumeRate = stats.totalMessagesReceived.sum() / elapsed;
    double consumeThroughput = stats.totalBytesReceived.sum() / elapsed / 1024 / 1024;
    Histogram latency = stats.endToEndCumulativeLatencyRecorder.getIntervalHistogram();
    log.info(
        String.format(
            "[Total]: Pub rate %.2f msg/s / %.2f MB/s | Consume rate %.2f msg/s %.2f MB/s "
                + "| E2E Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
            publishRate,
            publishThroughput,
            consumeRate,
            consumeThroughput,
            Utils.getLatencyInMs(latency.getMean()),
            Utils.getLatencyInMs(latency.getValueAtPercentile(50)),
            Utils.getLatencyInMs(latency.getValueAtPercentile(90)),
            Utils.getLatencyInMs(latency.getValueAtPercentile(99)),
            Utils.getLatencyInMs(latency.getMaxValueAsDouble())));
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

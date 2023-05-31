package io.hstream.tools;

import static io.hstream.tools.Utils.getLatencyInMs;

import io.hstream.HStreamClient;
import io.hstream.tools.Stats.PeriodStats;
import io.hstream.tools.Stats.Stats;
import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import org.HdrHistogram.Histogram;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class ReadBench {
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);
  private static final Stats stats = new Stats();
  private static final Logger log = LoggerFactory.getLogger(ReadBench.class);

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

    if (options.dataGenerate) {
      var streams = new ArrayList<String>();
      for (int i = 0; i < options.streamCnt; i++) {
        var streamName = options.streamNamePrefix + i + UUID.randomUUID();
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
      String path = "src/main/resources/streams.text";
      Utils.persistentStreamInfo(path, streams);
      // Sleep for a period of time to allow the node's CPU and memory metrics to reset
      // to facilitate getting the correct monitoring data
      Thread.sleep(10000L);
    }

    var streams = readStreams(options.path);
    System.out.printf("read from streams : %s\n", String.join(",", streams));

    var consumeService =
        new ConsumeService(
            client,
            streams,
            options.actTimeout,
            warmupDone,
            stats,
            options.payloadOpts.recordSize,
            options.offset);
    consumeService.startConsume();

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
      stats.resetSubStats();
    }

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    printStats(options.reportIntervalSeconds, benchDurationMs, options.latencyMode);
    consumeService.stopConsume();
  }

  private static void printStats(
      int reportIntervalSeconds, long benchDurationMs, boolean latencyMode) {
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
      double consumeRate = periodStat.messagesReceived / elapsed;
      double consumeThroughput = periodStat.bytesReceived / elapsed / 1024 / 1024;

      if (latencyMode) {
        log.info(
            String.format(
                "Consume rate %.2f msg/s / %.2f MB/s "
                    + "| E2E Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
                consumeRate,
                consumeThroughput,
                getLatencyInMs(periodStat.endToEndLatency.getMean()),
                getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(50)),
                getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(90)),
                getLatencyInMs(periodStat.endToEndLatency.getValueAtPercentile(99)),
                getLatencyInMs(periodStat.endToEndLatency.getMaxValueAsDouble())));
      } else {
        log.info(
            String.format("Consume rate %.2f msg/s / %.2f MB/s", consumeRate, consumeThroughput));
      }

      oldTime = now;
      if (benchDurationMs <= 0) {
        break;
      }
    }

    long endTime = System.nanoTime();
    double elapsed = (endTime - statTime) / 1e9;
    double consumeRate = stats.totalMessagesReceived.sum() / elapsed;
    double consumeThroughput = stats.totalBytesReceived.sum() / elapsed / 1024 / 1024;
    Histogram latency = stats.endToEndCumulativeLatencyRecorder.getIntervalHistogram();
    log.info(
        String.format(
            "[Total]: Consume rate %.2f msg/s / %.2f MB/s", consumeRate, consumeThroughput));
    if (latencyMode) {
      log.info(
          String.format(
              "[Total]: E2E Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
              getLatencyInMs(latency.getMean()),
              getLatencyInMs(latency.getValueAtPercentile(50)),
              getLatencyInMs(latency.getValueAtPercentile(90)),
              getLatencyInMs(latency.getValueAtPercentile(99)),
              getLatencyInMs(latency.getMaxValueAsDouble())));
    }
  }

  private static List<String> readStreams(String file) throws FileNotFoundException {
    var streamNames = new ArrayList<String>();
    Scanner s = new Scanner(new FileReader(file));
    while (s.hasNext()) {
      streamNames.add(s.nextLine());
    }
    return streamNames;
  }

  static class Options {
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "127.0.0.1:6570";

    @CommandLine.Option(
        names = "--data-generate",
        description = "write data to store first, then consume produced data.")
    boolean dataGenerate = false;

    @CommandLine.Option(
        names = "--streams",
        description =
            "The path to the file containing the names of all streams to be subscribed to, one stream per line")
    String path = "src/main/resources/streams.text";

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
    long rateLimit = 100000;

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

    @CommandLine.Option(
        names = {"-l", "--latency"},
        description = "use latency mode")
    boolean latencyMode = false;

    @CommandLine.Option(
        names = "--offset",
        description =
            "subscription start offset, should be [earliest | latest], default is earliest")
    String offset = "earliest";

    @Override
    public String toString() {
      return "Options{"
          + "helpRequested="
          + helpRequested
          + ", serviceUrl='"
          + serviceUrl
          + '\''
          + ", dataGenerate="
          + dataGenerate
          + ", path='"
          + path
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
          + ", latencyMode="
          + latencyMode
          + '}';
    }
  }
}

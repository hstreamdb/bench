package io.hstream.tools;

import static io.hstream.tools.Utils.persistentStreamInfo;

import io.hstream.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
import picocli.CommandLine;

public class WriteBench {
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

    ArrayList<String> streams = createStreamsConcurrently(options, client);
    if (!Objects.equals(options.path, Strings.EMPTY)) {
      persistentStreamInfo(options.path, streams);
    }

    var batchProducerService =
        new BufferedProduceService(
            client,
            streams,
            options.threadCount,
            options.rateLimit,
            options.batchProducerOpts,
            options.payloadOpts);
    batchProducerService.startService(warmupDone, successAppends, failedAppends);

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    long lastReportTs = System.currentTimeMillis();
    long lastReadSuccessAppends = 0;
    long lastReadFailedAppends = 0;
    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    long totalStartTime = System.currentTimeMillis();
    var recordSize = options.payloadOpts.recordSize;
    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000L);
      long now = System.currentTimeMillis();
      long successRead = successAppends.get();
      long failedRead = failedAppends.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (successRead - lastReadSuccessAppends) * 1000 / duration;
      double failurePerSeconds = (double) (failedRead - lastReadFailedAppends) * 1000 / duration;
      double throughput =
          (double) (successRead - lastReadSuccessAppends)
              * recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastReportTs = now;
      lastReadSuccessAppends = successRead;
      lastReadFailedAppends = failedRead;

      System.out.println(
          String.format(
              "[Append]: success %f record/s, failed %f record/s, throughput %f MB/s",
              successPerSeconds, failurePerSeconds, throughput));
      benchDurationMs -= duration;
      if (benchDurationMs <= 0) {
        batchProducerService.stopProducer();
        break;
      }
    }

    long totalEndTime = System.currentTimeMillis();
    long totalSuccess = successAppends.get();
    double totalAvgThroughput =
        (double) totalSuccess * recordSize * 1000 / (totalEndTime - totalStartTime) / 1024 / 1024;
    System.out.println(String.format("TotalAvgThroughput: %.2f MB/s", totalAvgThroughput));
    batchProducerService.stopService();
  }

  @NotNull
  private static ArrayList<String> createStreamsConcurrently(Options options, HStreamClient client)
      throws InterruptedException {
    var concurrency = Math.min(options.streamCount, options.threadCount);
    List<List<String>> streamsPerThread = new ArrayList<>(concurrency);
    for (int i = 0; i < concurrency; i++) {
      streamsPerThread.add(new ArrayList<>());
    }
    for (int i = 0; i < options.streamCount; i++) {
      var streamName = options.streamNamePrefix + i;
      if (!options.fixedStreamName) {
        streamName += UUID.randomUUID();
      }
      streamsPerThread.get(i % concurrency).add(streamName);
    }

    var start = System.currentTimeMillis();
    if (!options.doNotCreateStream) {
      List<Thread> threads = new ArrayList<>(concurrency);
      for (var streamNames : streamsPerThread) {
        var thread =
            new Thread(
                () -> {
                  for (var streamName : streamNames) {
                    client.createStream(
                        streamName,
                        options.streamReplicationFactor,
                        options.shardCount,
                        options.streamBacklogDuration);
                  }
                });
        thread.start();
        threads.add(thread);
      }
      for (var t : threads) {
        t.join();
      }
    }
    var end = System.currentTimeMillis();
    System.out.printf("total time used by create stream: %fs\n", (double) (end - start) / 1000);
    return (ArrayList<String>)
        streamsPerThread.stream().flatMap(Collection::stream).collect(Collectors.toList());
  }

  static void removeAllStreams(HStreamClient client) {
    var streams = client.listStreams();
    for (var stream : streams) {
      client.deleteStream(stream.getStreamName());
    }
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
    String streamNamePrefix = "write_bench_stream_";

    @CommandLine.Option(names = "--stream-count")
    int streamCount = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--fixed-stream-name")
    boolean fixedStreamName = false;

    @CommandLine.Option(
        names = "--not-create-stream",
        description = "only meaningful if fixedStreamName is true")
    boolean doNotCreateStream = false;

    @CommandLine.Option(
        names = "--persistent-stream-path",
        description = "file path to persistent stream name.")
    String path = "";

    @CommandLine.Option(names = "--thread-count", description = "threads count use to write.")
    int threadCount = streamCount;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.BufferedProducerOpts batchProducerOpts = new Utils.BufferedProducerOpts();

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.PayloadOpts payloadOpts = new Utils.PayloadOpts();

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible")
    long benchmarkDuration = -1; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 1; // seconds

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
          + ", streamCount="
          + streamCount
          + ", shardCount="
          + shardCount
          + ", threadCount="
          + threadCount
          + ", streamReplicationFactor="
          + streamReplicationFactor
          + ", streamBacklogDuration="
          + streamBacklogDuration
          + ", fixedStreamName="
          + fixedStreamName
          + ", doNotCreateStream="
          + doNotCreateStream
          + ", batchProducerOpts="
          + batchProducerOpts
          + ", payloadOpts="
          + payloadOpts
          + ", reportIntervalSeconds="
          + reportIntervalSeconds
          + ", rateLimit="
          + rateLimit
          + ", benchmarkDuration="
          + benchmarkDuration
          + ", warm="
          + warm
          + '}';
    }
  }
}

package io.hstream.tools;

import static io.hstream.tools.Utils.persistentStreamInfo;

import io.grpc.Status;
import io.hstream.*;
import io.hstream.tools.Stats.PeriodStats;
import io.hstream.tools.Stats.Stats;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import org.HdrHistogram.Histogram;
import org.apache.logging.log4j.util.Strings;
import org.jetbrains.annotations.NotNull;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import picocli.CommandLine;

public class WriteBench {
  private static final AtomicBoolean warmupDone = new AtomicBoolean(false);
  private static final Logger log = LoggerFactory.getLogger(WriteBench.class);
  private static final Stats stats = new Stats();
  private static BufferedProduceService bufferedProducerService;
  private static ExecutorService producerService;
  private static final AtomicBoolean terminate = new AtomicBoolean(false);

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

    if (options.unBuffered) {
      benchProduce(client, options, streams);
    } else {
      benchBufferedProduce(client, options, streams);
    }

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
      stats.resetPubStats();
    }

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    printStats(options.reportIntervalSeconds, benchDurationMs);
    if (options.unBuffered) {
      producerService.shutdown();
      producerService.awaitTermination(15, TimeUnit.SECONDS);
    } else {
      bufferedProducerService.stopService();
    }
  }

  private static void benchProduce(
      HStreamClient client, Options options, ArrayList<String> streams) {
    var threadsCnt = Math.min(options.threadCount, streams.size());
    producerService = Executors.newFixedThreadPool(threadsCnt);
    List<List<Producer>> producersPerThread = new ArrayList<>(threadsCnt);

    for (int i = 0; i < threadsCnt; i++) {
      producersPerThread.add(new ArrayList<>());
    }
    for (int i = 0; i < streams.size(); i++) {
      var producer = client.newProducer().stream(streams.get(i)).build();
      producersPerThread.get(i % threadsCnt).add(producer);
    }

    for (var producers : producersPerThread) {
      producerService.submit(
          () -> {
            var limiter = new Semaphore(1);
            Random random = new Random();
            Record record =
                Utils.makeRecord(options.payloadOpts.payloadType, options.payloadOpts.recordSize);
            while (!terminate.get()) {
              for (var producer : producers) {
                try {
                  limiter.acquire();
                } catch (InterruptedException e) {
                  throw new RuntimeException(e);
                }
                String key = "test_" + random.nextInt(options.payloadOpts.partitionKeys);
                record.setPartitionKey(key);
                final Instant sendTime = Instant.now();
                producer
                    .write(record)
                    .handle(
                        (recordId, throwable) -> {
                          limiter.release();
                          if (!warmupDone.get()) {
                            return null;
                          }

                          if (throwable != null) {
                            var status = Status.fromThrowable(throwable.getCause());
                            var errorCode = status.getCode();
                            if (errorCode == Status.UNAVAILABLE.getCode()
                                || errorCode == Status.DEADLINE_EXCEEDED.getCode()) {
                              stats.recordSendFailed();
                            } else {
                              System.exit(1);
                            }
                          } else {
                            Instant curr = Instant.now();
                            long latencyMicros =
                                TimeUnit.NANOSECONDS.toMicros(
                                    Utils.instantToNano(curr) - Utils.instantToNano(sendTime));
                            stats.recordMessageSend(options.payloadOpts.recordSize, latencyMicros);
                          }
                          return null;
                        });
              }
            }
          });
    }
  }

  private static void benchBufferedProduce(
      HStreamClient client, Options options, ArrayList<String> streams) {
    bufferedProducerService =
        new BufferedProduceService(
            client,
            streams,
            options.threadCount,
            options.rateLimit,
            options.batchProducerOpts,
            options.payloadOpts);
    bufferedProducerService.startService(warmupDone, stats);
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
      benchDurationMs -= (now - oldTime) / 1e6;
      double elapsed = (now - oldTime) / 1e9;
      double publishRate = periodStat.messagesSent / elapsed;
      double publishThroughput = periodStat.bytesSent / elapsed / 1024 / 1024;

      log.info(
          String.format(
              "Pub rate %.2f msg/s / %.2f MB/s | Pub Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
              publishRate,
              publishThroughput,
              Utils.getLatencyInMs(periodStat.publishLatency.getMean()),
              Utils.getLatencyInMs(periodStat.publishLatency.getValueAtPercentile(50)),
              Utils.getLatencyInMs(periodStat.publishLatency.getValueAtPercentile(90)),
              Utils.getLatencyInMs(periodStat.publishLatency.getValueAtPercentile(99)),
              Utils.getLatencyInMs(periodStat.publishLatency.getMaxValueAsDouble())));

      oldTime = now;
      if (benchDurationMs <= 0) {
        terminate.set(true);
        break;
      }
    }

    if (bufferedProducerService != null) {
      bufferedProducerService.stopProducer();
    }

    long endTime = System.nanoTime();
    double elapsed = (endTime - statTime) / 1e9;
    double publishRate = stats.totalMessagesSent.sum() / elapsed;
    double publishThroughput = stats.totalBytesSent.sum() / elapsed / 1024 / 1024;
    Histogram latency = stats.cumulativePublishLatencyRecorder.getIntervalHistogram();
    log.info(
        String.format(
            "[Total]: Pub rate %.2f msg/s / %.2f MB/s | Pub Latency (ms) avg: %.2f - p50: %.2f - p90: %.2f - p99: %.2f - Max: %.2f",
            publishRate,
            publishThroughput,
            Utils.getLatencyInMs(latency.getMean()),
            Utils.getLatencyInMs(latency.getValueAtPercentile(50)),
            Utils.getLatencyInMs(latency.getValueAtPercentile(90)),
            Utils.getLatencyInMs(latency.getValueAtPercentile(99)),
            Utils.getLatencyInMs(latency.getMaxValueAsDouble())));
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
    String serviceUrl = "hstream://127.0.0.1:6570";

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

    @CommandLine.Option(
        names = "--un-buffered",
        description = "use unbuffered producer, default is false")
    boolean unBuffered = false;

    @CommandLine.ArgGroup(exclusive = false)
    Utils.BufferedProducerOpts batchProducerOpts = new Utils.BufferedProducerOpts();

    @CommandLine.Option(names = "--rate-limit")
    long rateLimit = 100000;

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
          + ", streamReplicationFactor="
          + streamReplicationFactor
          + ", streamBacklogDuration="
          + streamBacklogDuration
          + ", fixedStreamName="
          + fixedStreamName
          + ", doNotCreateStream="
          + doNotCreateStream
          + ", path='"
          + path
          + '\''
          + ", threadCount="
          + threadCount
          + ", unBuffered="
          + unBuffered
          + ", batchProducerOpts="
          + batchProducerOpts
          + ", rateLimit="
          + rateLimit
          + ", payloadOpts="
          + payloadOpts
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

package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.hstream.BatchSetting;
import io.hstream.BufferedProducer;
import io.hstream.FlowControlSetting;
import io.hstream.HArray;
import io.hstream.HRecord;
import io.hstream.HStreamClient;
import io.hstream.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class WriteBench {

  private static ExecutorService executorService;

  private static long lastReportTs;
  private static long lastReadSuccessAppends;
  private static long lastReadFailedAppends;

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

    if (options.benchmarkDuration > 0 && options.warm >= options.benchmarkDuration) {
      System.err.println("Warmup time must be less than benchmark duration");
      System.exit(1);
    }

    HStreamClient client = HStreamClient.builder().serviceUrl(options.serviceUrl).build();
    var compresstionType = Utils.getCompressionType(options.compTp);

    executorService = Executors.newFixedThreadPool(options.threadCount);
    RateLimiter rateLimiter = RateLimiter.create(options.rateLimit);

    List<List<BufferedProducer>> producersPerThread = new ArrayList<>(options.threadCount);
    for (int i = 0; i < options.threadCount; i++) {
      producersPerThread.add(new ArrayList<>());
    }

    for (int i = 0; i < options.streamCount; i++) {
      var streamName = options.streamNamePrefix + i;
      if (!options.fixedStreamName) {
        streamName += UUID.randomUUID();
      }
      if (!options.doNotCreateStream) {
        client.createStream(
            streamName,
            options.streamReplicationFactor,
            options.shardCount,
            options.streamBacklogDuration);
      }
      var batchSetting =
          BatchSetting.newBuilder()
              .bytesLimit(options.batchBytesLimit)
              .ageLimit(options.batchAgeLimit)
              .recordCountLimit(-1)
              .build();
      var flowControlSetting =
          FlowControlSetting.newBuilder().bytesLimit(options.totalBytesLimit).build();
      var bufferedProducer =
          client.newBufferedProducer().stream(streamName)
              .batchSetting(batchSetting)
              .compressionType(compresstionType)
              .flowControlSetting(flowControlSetting)
              .build();
      producersPerThread.get(i % options.threadCount).add(bufferedProducer);
    }

    for (int i = 0; i < options.threadCount; ++i) {
      int index = i;
      executorService.submit(() -> append(rateLimiter, producersPerThread.get(index), options));
    }

    if (options.warm > 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    lastReportTs = System.currentTimeMillis();
    lastReadSuccessAppends = 0;
    lastReadFailedAppends = 0;
    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    long totalStartTime = System.currentTimeMillis();
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
              * options.recordSize
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
        terminateFlag.set(true);
        break;
      }
    }

    long totalEndTime = System.currentTimeMillis();
    long totalSuccess = successAppends.get();
    double totalAvgThroughput =
        (double) totalSuccess
            * options.recordSize
            * 1000
            / (totalEndTime - totalStartTime)
            / 1024
            / 1024;
    System.out.println(String.format("TotalAvgThroughput: %.2f MB/s", totalAvgThroughput));

    executorService.shutdown();
    executorService.awaitTermination(15, TimeUnit.SECONDS);
  }

  private static void stopAllBufferedProducers(List<BufferedProducer> producers) {
    for (var producer : producers) {
      producer.close();
    }
  }

  public static void append(
      RateLimiter rateLimiter, List<BufferedProducer> producers, Options options) {
    Random random = new Random();
    Record record = makeRecord(options);
    while (true) {
      for (var producer : producers) {
        if (terminateFlag.get()) {
          stopAllBufferedProducers(producers);
          return;
        }
        rateLimiter.acquire();
        String key = "test_" + random.nextInt(options.partitionKeys);
        record.setPartitionKey(key);
        producer
            .write(record)
            .handle(
                (recordId, throwable) -> {
                  if (!warmupDone.get()) {
                    return null;
                  }

                  if (throwable != null) {
                    failedAppends.incrementAndGet();
                  } else {
                    successAppends.incrementAndGet();
                  }
                  return null;
                });
      }

      // Thread.yield();
    }
  }

  static Record makeRecord(Options options) {
    if (options.payloadType.equals("raw")) {
      return makeRawRecord(options);
    }
    return makeHRecord(options);
  }

  static Record makeRawRecord(Options options) {
    Random random = new Random();
    byte[] payload = new byte[options.recordSize];
    random.nextBytes(payload);
    return Record.newBuilder().rawRecord(payload).build();
  }

  static Record makeHRecord(Options options) {
    int paddingSize = options.recordSize > 96 ? options.recordSize - 96 : 0;
    HRecord hRecord =
        HRecord.newBuilder()
            .put("int", 10)
            .put("boolean", true)
            .put("array", HArray.newBuilder().add(1).add(2).add(3).build())
            .put("string", "h".repeat(paddingSize))
            .build();
    return Record.newBuilder().hRecord(hRecord).build();
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

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(names = "--batch-age-limit", description = "in ms")
    int batchAgeLimit = 10; // ms

    @CommandLine.Option(names = "--batch-bytes-limit", description = "in bytes")
    int batchBytesLimit = 1024 * 1024; // bytes

    @CommandLine.Option(names = "--fixed-stream-name")
    boolean fixedStreamName = false;

    @CommandLine.Option(
        names = "--not-create-stream",
        description = "only meaningful if fixedStreamName is true")
    boolean doNotCreateStream = false;

    @CommandLine.Option(names = "--stream-count")
    int streamCount = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--thread-count")
    int threadCount = 1;

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.Option(names = "--ordering-keys")
    int partitionKeys = 10000;

    @CommandLine.Option(names = "--total-bytes-limit")
    int totalBytesLimit = batchBytesLimit * shardCount * 5;

    @CommandLine.Option(names = "--record-type")
    String payloadType = "raw";

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible")
    long benchmarkDuration = -1; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 1; // seconds

    @CommandLine.Option(names = "--compression", description = "Enum values: [none|gzip|zstd]")
    Utils.CompressionAlgo compTp = Utils.CompressionAlgo.none;
  }
}

package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.grpc.Status;
import io.hstream.*;
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

public class WriteQpsBench {
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

    // removeAllStreams(client);

    ExecutorService executorService = Executors.newFixedThreadPool(options.threadCount);
    RateLimiter rateLimiter = RateLimiter.create(options.rateLimit);

    var size = Math.min(options.threadCount, options.streamCount);
    List<List<Producer>> producersPerThread = new ArrayList<>(size);
    for (int i = 0; i < options.threadCount; i++) {
      producersPerThread.add(new ArrayList<>());
    }

    for (int i = 0; i < options.streamCount; i++) {
      var streamName = options.streamNamePrefix + i + UUID.randomUUID();
      client.createStream(
          streamName,
          options.streamReplicationFactor,
          options.shardCount,
          options.streamBacklogDuration);
      var producer = client.newProducer().stream(streamName).build();
      producersPerThread.get(i % size).add(producer);
    }

    for (int i = 0; i < size; ++i) {
      int index = i;
      executorService.submit(() -> append(rateLimiter, producersPerThread.get(index), options));
    }

    if (options.warm >= 0) {
      System.out.println("Warmup ...... ");
      Thread.sleep(options.warm * 1000L);
      warmupDone.set(true);
    }

    long lastReportTs = System.currentTimeMillis();
    long lastSuccessAppends = 0;
    long lastFailedAppends = 0;

    long benchDurationMs;
    if (options.benchmarkDuration <= 0 || options.benchmarkDuration >= Long.MAX_VALUE / 1000) {
      benchDurationMs = Long.MAX_VALUE;
    } else {
      benchDurationMs = options.benchmarkDuration * 1000L;
    }

    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000L);
      long now = System.currentTimeMillis();
      long successCnt = successAppends.get();
      long failedCnt = failedAppends.get();
      long duration = now - lastReportTs;
      long successSend = successCnt - lastSuccessAppends;
      long failedSend = failedCnt - lastFailedAppends;
      double successPerSeconds = (double) successSend * 1000 / duration;
      double avgResponseTimeMs = (double) duration / successSend;

      lastReportTs = now;
      lastSuccessAppends = successCnt;
      lastFailedAppends = failedCnt;

      System.out.println(
          String.format(
              "[Append-QPS]: success send %d record, failed send %d record, qps %f req/s, avgResponseTime: %f ms",
              successSend, failedSend, successPerSeconds, avgResponseTimeMs));
      benchDurationMs -= duration;
      if (benchDurationMs <= 0) {
        terminateFlag.set(true);
        break;
      }
    }
    executorService.shutdown();
    executorService.awaitTermination(15, TimeUnit.SECONDS);
  }

  public static void append(RateLimiter rateLimiter, List<Producer> producers, Options options) {
    Random random = new Random();
    Record record = Utils.makeRecord(options.payloadType, options.recordSize);
    while (true) {
      for (var producer : producers) {
        if (terminateFlag.get()) {
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
                    var status = Status.fromThrowable(throwable.getCause());
                    if (status.getCode() == Status.UNAVAILABLE.getCode()) {
                      failedAppends.incrementAndGet();
                    } else {
                      System.exit(1);
                    }
                  } else {
                    successAppends.incrementAndGet();
                  }
                  return null;
                });
        //        if (!warmupDone.get()) {
        //          try {
        //            Thread.sleep(5);
        //          } catch (Exception e) {
        //            e.printStackTrace();
        //          }
        //        }
      }
    }
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
    String streamNamePrefix = "write_qps_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(names = "--stream-count")
    int streamCount = 1;

    @CommandLine.Option(names = "--thread-count")
    int threadCount = 1;

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.Option(names = "--partition-keys")
    int partitionKeys = 10000;

    @CommandLine.Option(names = "--record-type")
    Utils.PayloadType payloadType = Utils.PayloadType.raw;

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible")
    long benchmarkDuration = -1; // seconds

    @CommandLine.Option(
        names = "--warmup",
        description = "in seconds, reduce write speed to avoid overstressing the server at startup")
    long warm = 8; // seconds
  }
}

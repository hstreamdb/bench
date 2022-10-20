package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.grpc.Status;
import io.hstream.*;
import java.util.ArrayList;
import java.util.Random;
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
  private static final AtomicBoolean terminateFlag = new AtomicBoolean(false);

  public static void main(String[] args) throws Exception {
    var options = new Options();
    var commandLine = new CommandLine(options).parseArgs(args);
    System.out.println(options.toString());

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
    var threads = new ArrayList<Thread>();
    var compresstionType = Utils.getCompressionType(options.compTp);
    RateLimiter rateLimiter = RateLimiter.create(options.rateLimit);
    for (int i = 0; i < options.streamCnt; i++) {
      var streamName = options.streamNamePrefix + UUID.randomUUID();
      client.createStream(
          streamName,
          options.streamReplicationFactor,
          options.shardCount,
          options.streamBacklogDuration);
      streams.add(streamName);
      BufferedProducer bufferedProducer =
          client.newBufferedProducer().stream(streamName)
              .batchSetting(
                  BatchSetting.newBuilder()
                      .bytesLimit(options.batchBytesLimit)
                      .ageLimit(options.batchAgeLimit)
                      .recordCountLimit(-1)
                      .build())
              .compressionType(compresstionType)
              .flowControlSetting(
                  FlowControlSetting.newBuilder().bytesLimit(options.totalBytesLimit).build())
              .build();

      var thread =
          new Thread(
              () ->
                  write(bufferedProducer, rateLimiter, options.recordSize, options.partitionKeys));
      threads.add(thread);
    }
    System.out.printf("Stream names: %s\n", streams.toString());
    for (var thread : threads) {
      thread.start();
    }

    var consumers = new ArrayList<Consumer>();
    for (int i = 0; i < options.streamCnt; i++) {
      System.out.println("create consummer " + i);
      consumers.add(read(client, streams.get(i), options.actTimeout));
    }

    if (options.warm > 0) {
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

    while (true) {
      Thread.sleep(options.reportIntervalSeconds * 1000L);
      long now = System.currentTimeMillis();
      long totalAppend = successAppends.get();
      long totalFailedAppend = failedAppends.get();
      long duration = now - lastReportTs;
      double successPerSeconds = (double) (totalAppend - lastSuccessAppends) * 1000 / duration;
      double failurePerSeconds = (double) (totalFailedAppend - lastFailedAppends) * 1000 / duration;
      double throughput =
          (double) (totalAppend - lastSuccessAppends)
              * options.recordSize
              * 1000
              / duration
              / 1024
              / 1024;

      lastSuccessAppends = totalAppend;
      lastFailedAppends = totalFailedAppend;

      long currentFetchedCount = fetchedCount.get();
      double fetchThroughput =
          (double) (currentFetchedCount - lastFetchedCount)
              * options.recordSize
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
          terminateFlag.set(true);
          break;
        }
      }
    }

    for (var thread : threads) {
      thread.join();
    }

    for (Consumer c : consumers) {
      c.stopAsync().awaitTerminated();
    }
  }

  static void write(
      BufferedProducer bufferedProducer, RateLimiter rateLimiter, int recordSize, int keyCount) {
    Random random = new Random();
    byte[] payload = new byte[recordSize];
    random.nextBytes(payload);
    var records = Record.newBuilder().rawRecord(payload).build();
    while (true) {
      if (terminateFlag.get()) {
        bufferedProducer.flush();
        bufferedProducer.close();
        return;
      }

      rateLimiter.acquire();
      String key = "key-" + random.nextInt(keyCount);
      records.setPartitionKey(key);
      bufferedProducer
          .write(records)
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
    }
  }

  static Consumer read(HStreamClient client, String streamName, int timeout) {
    var subscriptionId = UUID.randomUUID().toString();
    client.createSubscription(
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(timeout)
            .offset(Subscription.SubscriptionOffset.EARLIEST)
            .build());
    Consumer consumer =
        client
            .newConsumer()
            .subscription(subscriptionId)
            .rawRecordReceiver(
                (record, responder) -> {
                  if (warmupDone.get()) {
                    fetchedCount.incrementAndGet();
                  }
                  responder.ack();
                })
            .build();
    consumer.startAsync().awaitRunning();
    return consumer;
  }

  static class Options {
    @CommandLine.Option(
        names = {"-h", "--help"},
        usageHelp = true,
        description = "display a help message")
    boolean helpRequested = false;

    @CommandLine.Option(names = "--service-url")
    String serviceUrl = "127.0.0.1:6570";

    @CommandLine.Option(names = "--streams", description = "number of streams")
    int streamCnt = 32;

    @CommandLine.Option(names = "--stream-name-prefix")
    String streamNamePrefix = "read_bench_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 3600;

    @CommandLine.Option(names = "--record-size", description = "single record size in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(
        names = "--rate-limit",
        description = "total records append for all producers in a second")
    int rateLimit = 100000;

    @CommandLine.Option(names = "--partition-keys", description = "partition keys for each stream")
    int partitionKeys = 10000;

    @CommandLine.Option(names = "--batch-age-limit", description = "in ms")
    int batchAgeLimit = -1; // ms

    @CommandLine.Option(
        names = "--batch-bytes-limit",
        description = "total bytes of a single batch")
    int batchBytesLimit = 204800; // bytes

    @CommandLine.Option(names = "--total-bytes-limit")
    int totalBytesLimit = batchBytesLimit * 5;

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

    @CommandLine.Option(names = "--compression", description = "Enum values: [none|gzip]")
    Utils.CompressionAlgo compTp = Utils.CompressionAlgo.none;
  }
}

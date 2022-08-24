package io.hstream.tools;

import com.google.common.util.concurrent.RateLimiter;
import io.hstream.*;
import io.hstream.Record;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import picocli.CommandLine;

public class WriteReadBench {

  private static long lastReportTs;
  private static long lastReadSuccessAppends;
  private static long lastReadFailedAppends;

  private static final AtomicLong successAppends = new AtomicLong();
  private static final AtomicLong failedAppends = new AtomicLong();

  private static long lastFetchedCount = 0;
  private static final AtomicLong fetchedCount = new AtomicLong();
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

    // Stream
    String streamName = options.streamNamePrefix + UUID.randomUUID();
    client.createStream(
        streamName,
        options.streamReplicationFactor,
        options.shardCount,
        options.streamBacklogDuration);

    // Write
    RateLimiter rateLimiter = RateLimiter.create(options.rateLimit);
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

    var thread = new Thread(() -> append(rateLimiter, bufferedProducer, options));
    thread.start();

    // Read
    var consumers = fetch(client, streamName, options);

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

      lastReadSuccessAppends = successRead;
      lastReadFailedAppends = failedRead;

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

      System.out.println(
          String.format(
              "[Append]: success %f record/s, failed %f record/s, throughput %f MB/s",
              successPerSeconds, failurePerSeconds, throughput));
      System.out.println(String.format("[Fetch]: throughput %f MB/s", fetchThroughput));
      benchDurationMs -= duration;
      if (benchDurationMs <= 0) {
        terminateFlag.set(true);
        break;
      }
    }
    thread.join();
    for (var consumer : consumers) {
      consumer.stopAsync().awaitTerminated();
    }
  }

  public static void append(
      RateLimiter rateLimiter, BufferedProducer producer, WriteReadBench.Options options) {
    Random random = new Random();
    Record record = makeRecord(options);
    while (true) {
      if (terminateFlag.get()) {
        producer.close();
        return;
      }
      rateLimiter.acquire();
      String key = "test_" + random.nextInt(options.orderingKeys);
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
  }

  public static List<Consumer> fetch(HStreamClient client, String streamName, Options options) {
    var subscriptionId = "bench_WriteRead_sub_" + UUID.randomUUID();
    var subscription =
        Subscription.newBuilder().stream(streamName)
            .subscription(subscriptionId)
            .ackTimeoutSeconds(60)
            .build();
    client.createSubscription(subscription);
    var consumers = new ArrayList<Consumer>();
    for (int i = 0; i < options.consumerCount; i++) {
      Consumer consumer =
          client
              .newConsumer()
              .subscription(subscriptionId)
              .rawRecordReceiver(
                  (receivedRawRecord, responder) -> {
                    if (warmupDone.get()) {
                      fetchedCount.incrementAndGet();
                    }
                    responder.ack();
                  })
              .hRecordReceiver(
                  (receivedHRecord, responder) -> {
                    if (warmupDone.get()) {
                      fetchedCount.incrementAndGet();
                    }
                    responder.ack();
                  })
              .build();
      consumer.startAsync().awaitRunning();
      consumers.add(consumer);
    }
    return consumers;
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
    String streamNamePrefix = "readWrite_bench_stream_";

    @CommandLine.Option(names = "--stream-replication-factor")
    short streamReplicationFactor = 1;

    @CommandLine.Option(names = "--shard-count")
    int shardCount = 1;

    @CommandLine.Option(names = "--stream-backlog-duration", description = "in seconds")
    int streamBacklogDuration = 60 * 30;

    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(names = "--batch-age-limit", description = "in ms")
    int batchAgeLimit = 10; // ms

    @CommandLine.Option(names = "--batch-bytes-limit", description = "in bytes")
    int batchBytesLimit = 1024 * 1024; // bytes

    @CommandLine.Option(names = "--report-interval", description = "in seconds")
    int reportIntervalSeconds = 3;

    @CommandLine.Option(names = "--rate-limit")
    int rateLimit = 100000;

    @CommandLine.Option(names = "--ordering-keys")
    int orderingKeys = 10;

    @CommandLine.Option(names = "--total-bytes-limit")
    int totalBytesLimit = batchBytesLimit * orderingKeys * 10;
    // int totalBytesLimit = -1;

    @CommandLine.Option(names = "--record-type")
    String payloadType = "raw";

    @CommandLine.Option(names = "--consumer-count")
    int consumerCount = 1;

    @CommandLine.Option(
        names = "--bench-time",
        description = "in seconds. set bench-time <= 0 means run as long as possible.")
    long benchmarkDuration = -1; // seconds

    @CommandLine.Option(names = "--warmup", description = "in seconds")
    long warm = 60; // seconds

    @CommandLine.Option(names = "--compresstion", description = "Enum values: [None|Gzip]")
    Utils.CompressionAlgo compTp = Utils.CompressionAlgo.None;
  }
}

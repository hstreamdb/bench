package io.hstream.tools;

import io.hstream.*;
import java.io.*;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.Scanner;
import picocli.CommandLine;

public class Utils {
  public static void persistentStreamInfo(String fileName, List<String> streamNames)
      throws IOException {
    BufferedWriter writer = new BufferedWriter(new FileWriter(fileName));
    for (var stream : streamNames) {
      writer.write(stream);
      writer.write("\n");
    }
    writer.close();
  }

  static long instantToNano(Instant now) {
    return now.getEpochSecond() * 1_000_000_000L + now.getNano();
  }

  static List<String> readStreams(String file) throws FileNotFoundException {
    var streamNames = new ArrayList<String>();
    Scanner s = new Scanner(new FileReader(file));
    while (s.hasNext()) {
      streamNames.add(s.nextLine());
    }
    return streamNames;
  }

  public enum CompressionAlgo {
    none,
    gzip,
    zstd
  }

  public static CompressionType getCompressionType(CompressionAlgo c) {
    switch (c) {
      case gzip:
        return CompressionType.GZIP;
      case zstd:
        return CompressionType.ZSTD;
      case none:
        return CompressionType.NONE;
      default:
        throw new RuntimeException("Unknown compression type");
    }
  }

  public static class BufferedProducerOpts {
    @CommandLine.Option(names = "--batch-bytes-limit", description = "in bytes")
    int batchBytesLimit = 1024 * 1024;

    @CommandLine.Option(names = "--batch-age-limit", description = "in ms")
    long batchAgeLimit = 10;

    @CommandLine.Option(names = "--batch-record-count-limit", description = "")
    int batchRecordCountLimit = -1;

    @CommandLine.Option(names = "--total-bytes-limit")
    int totalBytesLimit = batchBytesLimit * 5;

    @CommandLine.Option(
        names = "--compression",
        description = "Enum values: ${COMPLETION-CANDIDATES}")
    CompressionAlgo compTp = CompressionAlgo.none;

    @Override
    public String toString() {
      return "BufferedProducerOpts{"
          + "batchBytesLimit="
          + batchBytesLimit
          + ", batchAgeLimit="
          + batchAgeLimit
          + ", batchRecordCountLimit="
          + batchRecordCountLimit
          + ", totalBytesLimit="
          + totalBytesLimit
          + '}';
    }
  }

  public enum PayloadType {
    raw,
    hrecord,
  }

  public static class PayloadOpts {
    @CommandLine.Option(names = "--record-size", description = "in bytes")
    int recordSize = 1024; // bytes

    @CommandLine.Option(
        names = "--record-type",
        description = "Enum values: ${COMPLETION-CANDIDATES}")
    PayloadType payloadType = PayloadType.raw;

    @CommandLine.Option(names = "--partition-keys")
    int partitionKeys = 10000;

    @Override
    public String toString() {
      return "PayloadOpts{"
          + "recordSize="
          + recordSize
          + ", payloadType='"
          + payloadType
          + '\''
          + ", partitionKeys="
          + partitionKeys
          + '}';
    }
  }

  public static BufferedProducer buildBufferedProducer(
      HStreamClient client, String streamName, CompressionType cmpType, BufferedProducerOpts opts) {
    var batchSetting =
        BatchSetting.newBuilder()
            .bytesLimit(opts.batchBytesLimit)
            .ageLimit(opts.batchAgeLimit)
            .recordCountLimit(opts.batchRecordCountLimit)
            .build();
    var flowControlSetting =
        FlowControlSetting.newBuilder().bytesLimit(opts.totalBytesLimit).build();
    return client.newBufferedProducer().stream(streamName)
        .batchSetting(batchSetting)
        .compressionType(cmpType)
        .flowControlSetting(flowControlSetting)
        .requestTimeoutMs(100)
        .build();
  }

  static Record makeRecord(PayloadType payloadType, int recordSize) {
    if (payloadType == PayloadType.raw) {
      return makeRawRecord(recordSize);
    } else if (payloadType == PayloadType.hrecord) {
      return makeHRecord(recordSize);
    } else {
      System.err.println("unknown payload type");
      System.exit(1);
    }
    return null;
  }

  static Record makeRawRecord(int recordSize) {
    Random random = new Random(System.currentTimeMillis());
    byte[] payload = new byte[recordSize];
    random.nextBytes(payload);
    return Record.newBuilder().rawRecord(payload).build();
  }

  static Record makeHRecord(int recordSize) {
    int paddingSize = recordSize > 48 ? recordSize - 48 : 0;
    HRecord hRecord =
        HRecord.newBuilder()
            .put("c_int", 10)
            .put("c_smallint", 1)
            .put("c_bigint", 1844674567)
            .put("c_boolean", true)
            .put("c_string", "h".repeat(paddingSize))
            .build();
    return Record.newBuilder().hRecord(hRecord).build();
  }

  public static double getLatencyInMs(double latency) {
    return latency / 1000;
  }
}

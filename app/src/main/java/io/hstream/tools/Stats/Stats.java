package io.hstream.tools.Stats;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.LongAdder;
import org.HdrHistogram.Recorder;

public class Stats {
  // producer
  public final LongAdder messagesSent = new LongAdder();
  public final LongAdder messagesSentError = new LongAdder();
  public final LongAdder bytesSent = new LongAdder();
  public final LongAdder totalMessagesSent = new LongAdder();
  public final LongAdder totalBytesSent = new LongAdder();
  public final LongAdder totalMessagesSentError = new LongAdder();
  public final Recorder publishLatencyRecorder = new Recorder(TimeUnit.SECONDS.toMicros(60), 5);
  public final Recorder cumulativePublishLatencyRecorder =
      new Recorder(TimeUnit.SECONDS.toMicros(60), 5);

  // consumer
  public final LongAdder messagesReceived = new LongAdder();
  public final LongAdder messagesReceivedError = new LongAdder();
  public final LongAdder bytesReceived = new LongAdder();
  public final LongAdder totalMessagesReceived = new LongAdder();
  public final LongAdder totalBytesReceived = new LongAdder();
  public final LongAdder totalMessagesReceivedError = new LongAdder();
  public final Recorder endToEndLatencyRecorder = new Recorder(TimeUnit.HOURS.toMicros(12), 5);
  public final Recorder endToEndCumulativeLatencyRecorder =
      new Recorder(TimeUnit.HOURS.toMicros(12), 5);

  public void recordMessageSend(long recordSize, long latencyMics) {
    messagesSent.increment();
    bytesSent.add(recordSize);
    totalMessagesSent.increment();
    totalBytesSent.add(recordSize);

    if (latencyMics > 0) {
      publishLatencyRecorder.recordValue(latencyMics);
      cumulativePublishLatencyRecorder.recordValue(latencyMics);
    }
  }

  public void recordSendFailed() {
    messagesSentError.increment();
    totalMessagesSentError.increment();
  }

  public void recordReceiveFailed() {
    messagesReceivedError.increment();
    totalMessagesReceivedError.increment();
  }

  public void recordMessageReceived(long recordSize, long latencyMics) {
    messagesReceived.increment();
    bytesReceived.add(recordSize);
    totalMessagesReceived.increment();
    totalBytesReceived.add(recordSize);

    if (latencyMics > 0) {
      endToEndLatencyRecorder.recordValue(latencyMics);
      endToEndCumulativeLatencyRecorder.recordValue(latencyMics);
    }
  }

  public void resetPubStats() {
    messagesSent.reset();
    messagesSentError.reset();
    bytesSent.reset();
    totalMessagesSent.reset();
    totalBytesSent.reset();
    totalMessagesSentError.reset();
    publishLatencyRecorder.reset();
    cumulativePublishLatencyRecorder.reset();
  }

  public void resetSubStats() {
    messagesReceived.reset();
    messagesReceivedError.reset();
    bytesReceived.reset();
    totalMessagesReceived.reset();
    totalBytesReceived.reset();
    totalMessagesReceivedError.reset();
    endToEndLatencyRecorder.reset();
    endToEndCumulativeLatencyRecorder.reset();
  }

  public PeriodStats getPeriodStats() {
    PeriodStats stats = new PeriodStats();

    stats.messagesSent = messagesSent.sumThenReset();
    stats.messageSendError = messagesSentError.sumThenReset();
    stats.bytesSent = bytesSent.sumThenReset();

    stats.messagesReceived = messagesReceived.sumThenReset();
    stats.messagesReceivedError = messagesReceivedError.sumThenReset();
    stats.bytesReceived = bytesReceived.sumThenReset();

    stats.totalMessagesSent = totalMessagesSent.sum();
    stats.totalMessageSendError = totalMessagesSentError.sum();
    stats.totalMessagesReceived = totalMessagesReceived.sum();
    stats.totalMessagesReceivedError = totalMessagesReceivedError.sum();

    stats.publishLatency = publishLatencyRecorder.getIntervalHistogram();
    stats.endToEndLatency = endToEndLatencyRecorder.getIntervalHistogram();
    return stats;
  }
}

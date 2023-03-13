package io.hstream.tools.Stats;

import static java.util.concurrent.TimeUnit.HOURS;
import static java.util.concurrent.TimeUnit.SECONDS;

import org.HdrHistogram.Histogram;

public class PeriodStats {
  public long messagesSent = 0;
  public long messageSendError = 0;
  public long bytesSent = 0;

  public long messagesReceived = 0;
  public long messagesReceivedError = 0;
  public long bytesReceived = 0;

  public long totalMessagesSent = 0;
  public long totalMessageSendError = 0;
  public long totalMessagesReceived = 0;
  public long totalMessagesReceivedError = 0;

  public Histogram publishLatency = new Histogram(SECONDS.toMicros(60), 5);
  public Histogram endToEndLatency = new Histogram(HOURS.toMicros(12), 5);
}

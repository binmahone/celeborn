package org.apache.celeborn.client;

public interface ShuffleClientMetricsReporter {
  void reportCompressionTime(String mapKey, long time);
  void reportCongestionTime(String mapKey, long time);
}

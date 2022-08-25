package io.hstream.tools;

import io.hstream.CompressionType;

public class Utils {
  public enum CompressionAlgo {
    none,
    gzip
  }

  public static CompressionType getCompressionType(CompressionAlgo c) {
    switch (c) {
      case gzip:
        return CompressionType.GZIP;
      case none:
        return CompressionType.NONE;
      default:
        throw new RuntimeException("Unknown compression type");
    }
  }
}

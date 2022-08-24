package io.hstream.tools;

import io.hstream.CompressionType;

public class Utils {
  public enum CompressionAlgo {
    None,
    GZip
  }

  public static CompressionType getCompressionType(CompressionAlgo c) {
    switch (c) {
      case GZip:
        return CompressionType.GZIP;
      case None:
        return CompressionType.NONE;
      default:
        throw new RuntimeException("Unknown compression type");
    }
  }
}

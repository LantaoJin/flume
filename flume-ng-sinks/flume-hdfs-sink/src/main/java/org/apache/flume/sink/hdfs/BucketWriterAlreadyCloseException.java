package org.apache.flume.sink.hdfs;

import java.io.IOException;

public class BucketWriterAlreadyCloseException extends IOException {

  private static final long serialVersionUID = -6419160697265440098L;

  public BucketWriterAlreadyCloseException() {
    super();
  }

  public BucketWriterAlreadyCloseException(String message) {
    super(message);
  }
  
  public BucketWriterAlreadyCloseException(String message, Throwable t) {
    super(message, t);
  }
  
  public BucketWriterAlreadyCloseException(Throwable t) {
    super(t);
  }
}

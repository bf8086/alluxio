package alluxio.client.block.stream;

public class DataMessage<T, R> {
  private final T mMessage;
  private final R mBuffer;

  public DataMessage(T message, R buffer) {
    mMessage = message;
    mBuffer = buffer;
  }

  public T getMessage() {
    return mMessage;
  }

  public R getBuffer() {
    return mBuffer;
  }
}

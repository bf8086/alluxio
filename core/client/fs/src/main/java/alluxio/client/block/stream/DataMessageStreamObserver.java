/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0
 * (the "License"). You may not use this work except in compliance with the License, which is
 * available at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.client.block.stream;

import alluxio.network.protocol.databuffer.DataBuffer;

import io.grpc.internal.ReadableBuffer;
import io.grpc.stub.ClientCallStreamObserver;
import io.grpc.stub.ClientResponseObserver;
import io.grpc.stub.StreamObserver;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamObserver} capable of handling data in raw data buffers.
 *
 * @param <T> type of the message
 */
@NotThreadSafe
public class DataMessageStreamObserver<ReqT, RespT> implements ClientResponseObserver<ReqT, RespT> {

  private final BufferRepository<RespT, ReadableBuffer> mBufferRepo;
  private final StreamObserver<DataMessage<RespT, DataBuffer>> mObserver;

  public DataMessageStreamObserver(StreamObserver<DataMessage<RespT, DataBuffer>> observer,
      BufferRepository<RespT, ReadableBuffer> bufferRepo) {
    mObserver = observer;
    mBufferRepo = bufferRepo;
  }

  @Override
  public void onNext(RespT value) {
    ReadableDataBuffer buffer = new ReadableDataBuffer(mBufferRepo.pollBuffer(value));
    mObserver.onNext(new DataMessage<>(value, buffer));
  }

  @Override
  public void onError(Throwable t) {
    mObserver.onError(t);
  }

  @Override
  public void onCompleted() {
    mObserver.onCompleted();
  }

  @Override
  public void beforeStart(ClientCallStreamObserver<ReqT> requestStream) {
    if (mObserver instanceof ClientResponseObserver) {
      ((ClientResponseObserver<ReqT, DataMessage<RespT, DataBuffer>>)mObserver).beforeStart(requestStream);
    }
  }
}

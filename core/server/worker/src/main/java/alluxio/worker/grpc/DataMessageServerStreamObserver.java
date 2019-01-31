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

package alluxio.worker.grpc;

import alluxio.client.block.stream.BufferRepository;
import alluxio.client.block.stream.DataMessage;

import io.grpc.stub.CallStreamObserver;
import io.grpc.stub.StreamObserver;
import io.netty.buffer.ByteBuf;

import javax.annotation.concurrent.NotThreadSafe;

/**
 * A {@link StreamObserver} capable of handling data in raw data buffers.
 *
 * @param <T> type of the message
 */
@NotThreadSafe
public class DataMessageServerStreamObserver<T> extends CallStreamObserver<DataMessage<T, ByteBuf>> {

  private final BufferRepository<T, ByteBuf> mBufferRepo;
  private final CallStreamObserver<T> mObserver;

  public DataMessageServerStreamObserver(CallStreamObserver<T> observer,
      BufferRepository<T, ByteBuf> bufferRepo) {
    mObserver = observer;
    mBufferRepo = bufferRepo;
  }

  @Override
  public void onNext(DataMessage<T, ByteBuf> value) {
    ByteBuf buffer = value.getBuffer();
    if (buffer != null) {
      mBufferRepo.offerBuffer(buffer, value.getMessage());
    }
    mObserver.onNext(value.getMessage());
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
  public boolean isReady() {
    return mObserver.isReady();
  }

  @Override
  public void setOnReadyHandler(Runnable onReadyHandler) {
    mObserver.setOnReadyHandler(onReadyHandler);
  }

  @Override
  public void disableAutoInboundFlowControl() {
    mObserver.disableAutoInboundFlowControl();
  }

  @Override
  public void request(int count) {
    mObserver.request(count);
  }

  @Override
  public void setMessageCompression(boolean enable) {

  }
}

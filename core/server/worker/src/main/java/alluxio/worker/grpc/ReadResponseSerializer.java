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
import alluxio.grpc.MessageSerializer;
import alluxio.grpc.ReadResponse;

import io.grpc.MethodDescriptor;
import io.grpc.internal.ReadableBuffer;
import io.netty.buffer.ByteBuf;
import org.jboss.netty.util.internal.ConcurrentIdentityHashMap;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ReadResponseSerializer extends MessageSerializer<ReadResponse>
    implements BufferRepository<ReadResponse, ByteBuf> {
  private final Map<ReadResponse, ByteBuf> mBufferMap =
      new ConcurrentIdentityHashMap<>();

  public ReadResponseSerializer(MethodDescriptor.Marshaller<ReadResponse> responseMarshaller) {
    super(responseMarshaller);
  }

  @Override
  protected ByteBuf extractMessageBuffer(ReadResponse message) {
    return pollBuffer(message);
  }

  @Override
  public void close() {
    for (ByteBuf buffer : mBufferMap.values()) {
      buffer.release();
    }
  }

  @Override
  public void offerBuffer(ByteBuf buf, ReadResponse response) {
    mBufferMap.put(response, buf);
  }

  @Override
  public ByteBuf pollBuffer(ReadResponse response) {
    return mBufferMap.remove(response);
  }
}

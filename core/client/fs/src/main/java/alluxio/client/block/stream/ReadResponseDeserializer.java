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

import alluxio.grpc.MessageDeserializer;
import alluxio.grpc.ReadResponse;

import io.grpc.internal.ReadableBuffer;
import org.jboss.netty.util.internal.ConcurrentIdentityHashMap;

import java.util.Map;

import javax.annotation.concurrent.NotThreadSafe;

@NotThreadSafe
public class ReadResponseDeserializer extends MessageDeserializer<ReadResponse>
    implements BufferRepository<ReadResponse, ReadableBuffer> {
  private final Map<ReadResponse, ReadableBuffer> mReadableBufferMap =
      new ConcurrentIdentityHashMap<>();

  @Override
  protected ReadResponse parseResponse(ReadableBuffer buffer) {
    ReadResponse response = ReadResponse.newBuilder().build();
    offerBuffer(buffer, response);
    return response;
  }

  @Override
  public void close() {
    for (ReadableBuffer buffer : mReadableBufferMap.values()) {
      buffer.close();
    }
  }

  @Override
  public void offerBuffer(ReadableBuffer buf, ReadResponse response) {
    mReadableBufferMap.put(response, buf);
  }

  @Override
  public ReadableBuffer pollBuffer(ReadResponse response) {
    return mReadableBufferMap.remove(response);
  }
}

/*
 * The Alluxio Open Foundation licenses this work under the Apache License, version 2.0 (the
 * "License"). You may not use this work except in compliance with the License, which is available
 * at www.apache.org/licenses/LICENSE-2.0
 *
 * This software is distributed on an "AS IS" basis, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND,
 * either express or implied, as more fully set forth in the License.
 *
 * See the NOTICE file distributed with this work for information regarding copyright ownership.
 */

package alluxio.grpc;

import io.grpc.MethodDescriptor;
import io.netty.buffer.ByteBuf;

import java.io.InputStream;

public abstract class MessageSerializer<T> {
  private final MethodDescriptor.Marshaller<T> mOriginalMarshaller;

  public MessageSerializer(MethodDescriptor.Marshaller<T> originalMarshaller) {
    mOriginalMarshaller = originalMarshaller;
  }

  protected abstract ByteBuf extractMessageBuffer(T message);
  public InputStream streamMessage(T message) {
    return new ZeroCopyUtils.DataBufferAttachedInputStream(mOriginalMarshaller.stream(message),
        extractMessageBuffer(message));
  }

  public abstract void close();
}

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

import io.grpc.internal.CompositeReadableBuffer;
import io.grpc.internal.ReadableBuffer;

import java.io.InputStream;

public abstract class MessageDeserializer<T> {
  protected abstract T parseResponse(ReadableBuffer buffer);
  public T parseResponse(InputStream message) {
    CompositeReadableBuffer readableBuffer = new CompositeReadableBuffer();
    readableBuffer.addBuffer(ZeroCopyUtils.getReadableBuffer(message));
    // Do not use method.parseResponse(message) because it returns a static
    // instance for empty response.
    return parseResponse(readableBuffer);
  }

  public abstract void close();
}

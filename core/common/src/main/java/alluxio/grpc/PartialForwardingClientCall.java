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

import io.grpc.Attributes;
import io.grpc.ClientCall;
import io.grpc.Metadata;
import io.grpc.Status;

import javax.annotation.Nullable;

/** A public copy of {@link io.grpc.PartialForwardingClientCall} */
public abstract class PartialForwardingClientCall<ReqT, RespT> extends ClientCall<ReqT, RespT> {
  protected abstract ClientCall<?, ?> delegate();

  @Override
  public void request(int numMessages) {
    delegate().request(numMessages);
  }

  @Override
  public void cancel(@Nullable String message, @Nullable Throwable cause) {
    delegate().cancel(message, cause);
  }

  @Override
  public void halfClose() {
    delegate().halfClose();
  }

  @Override
  public void setMessageCompression(boolean enabled) {
    delegate().setMessageCompression(enabled);
  }

  @Override
  public boolean isReady() {
    return delegate().isReady();
  }

  @Override
  public Attributes getAttributes() {
    return delegate().getAttributes();
  }

  /** A public copy of {@link io.grpc.PartialForwardingClientCallListener} */
  public abstract class CallListener<RespT> extends ClientCall.Listener<RespT> {
    protected abstract ClientCall.Listener<?> delegate();

    @Override
    public void onHeaders(Metadata headers) {
      delegate().onHeaders(headers);
    }

    @Override
    public void onClose(Status status, Metadata trailers) {
      delegate().onClose(status, trailers);
    }

    @Override
    public void onReady() {
      delegate().onReady();
    }
  }
}

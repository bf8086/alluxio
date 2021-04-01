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

package alluxio.grpc;

import io.grpc.ForwardingServerCallListener;
import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;

import java.util.UUID;

/**
 * Server side interceptor that is used to put remote client's IP Address to thread local storage.
 */
public class ClientRequestIdInjector implements ServerInterceptor {

  /**
   * A {@link ThreadLocal} variable to maintain the client's IP address along with a specific
   * thread.
   */
  private static ThreadLocal<UUID> sClientIdThreadLocal = new ThreadLocal<>();
  private static ThreadLocal<Integer> sRequestIdThreadLocal = new ThreadLocal<>();

  /**
   * @return IP address of the gRPC client that is making the call
   */
  public static UUID getClientId() {
    return sClientIdThreadLocal.get();
  }

  /**
   * @return IP address of the gRPC client that is making the call
   */
  public static int getRequestId() {
    return sRequestIdThreadLocal.get();
  }
  public static boolean hasKey() {
    return sRequestIdThreadLocal.get() != null;
  }

  public static String getKey() {
    return hasKey() ? String.format("%s-%d", getClientId(), getRequestId()) : "N/A";
  }

  @Override
  public <ReqT, RespT> ServerCall.Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
      Metadata headers, ServerCallHandler<ReqT, RespT> next) {
    /**
     * For streaming calls, below will make sure remote IP address is injected prior to creating the
     * stream.
     */
    setValue(headers, ClientRequestIdInterceptor.S_CLIENT_ID_KEY, sClientIdThreadLocal);
    setValue(headers, ClientRequestIdInterceptor.S_REQUEST_ID_KEY, sRequestIdThreadLocal);

    /**
     * For non-streaming calls to server, below listener will be invoked in the same thread that is
     * serving the call.
     */
    return new ForwardingServerCallListener.SimpleForwardingServerCallListener<ReqT>(
        next.startCall(call, headers)) {
      @Override
      public void onHalfClose() {
        setValue(headers, ClientRequestIdInterceptor.S_CLIENT_ID_KEY, sClientIdThreadLocal);
        setValue(headers, ClientRequestIdInterceptor.S_REQUEST_ID_KEY, sRequestIdThreadLocal);
        super.onHalfClose();
      }
    };
  }

  private <T> void setValue(Metadata metadata, Metadata.Key<T> key, ThreadLocal<T> variable) {
    T value = metadata.get(key);
    variable.set(value);
  }
}

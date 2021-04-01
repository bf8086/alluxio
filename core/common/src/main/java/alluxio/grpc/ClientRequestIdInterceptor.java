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

import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import javax.annotation.concurrent.ThreadSafe;

/**
 * Client side interceptor that is used to augment outgoing metadata with the unique id for the
 * channel that the RPC is being called on.
 */
@ThreadSafe
public final class ClientRequestIdInterceptor implements ClientInterceptor {
  private static final Logger LOG = LoggerFactory.getLogger(ClientRequestIdInterceptor.class);

  static ThreadLocal<Integer> tRequestId = new ThreadLocal<>();
  /** Metadata key for the channel Id. */
  public static final Metadata.Key<UUID> S_CLIENT_ID_KEY =
      Metadata.Key.of("client-id", new Metadata.AsciiMarshaller<UUID>() {
        @Override
        public String toAsciiString(UUID value) {
          return value.toString();
        }

        @Override
        public UUID parseAsciiString(String serialized) {
          return UUID.fromString(serialized);
        }
      });

  /** Metadata key for the channel Id. */
  public static final Metadata.Key<Integer> S_REQUEST_ID_KEY =
      Metadata.Key.of("request-id-bin", new Metadata.BinaryMarshaller<Integer>() {
        @Override
        public byte[] toBytes(Integer value) {
          return ByteBuffer.allocate(4).putInt(value).array();
        }

        @Override
        public Integer parseBytes(byte[] serialized) {
          return ByteBuffer.wrap(serialized).getInt();
        }
      });

  // TODO(ggezer) Consider more lightweight Id type.
  static ThreadLocal<UUID> tClientId = new ThreadLocal<>();

  /**
   * Creates the injector that augments the outgoing metadata with given Id.
   *
   * @param channelId channel id
   */
  public ClientRequestIdInterceptor() {
  }

  public static void setRequestId(int requestId) {
    tRequestId.set(requestId);
  }

  public static void setClientId(UUID clientId) {
    LOG.info("setClientID {}", clientId);
    tClientId.set(clientId);
  }

  public static String getKey() {
    return String.format("%s-%d", tClientId.get(), tRequestId.get());
  }

  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
      CallOptions callOptions, Channel next) {
    return new ForwardingClientCall.SimpleForwardingClientCall<ReqT, RespT>(
        next.newCall(method, callOptions)) {
      @Override
      public void start(Listener<RespT> responseListener, Metadata headers) {
        // Put channel Id to headers.
        if (tRequestId.get() == null) {
//          LOG.debug("missing request id for {}::{}",
//              method.getServiceName(), method.getFullMethodName());
        } else {
          headers.put(S_CLIENT_ID_KEY, tClientId.get());
          headers.put(S_REQUEST_ID_KEY, tRequestId.get());
//          LOG.warn("{}: {}-{}",
//              method.getFullMethodName(), tClientId.get(), tRequestId.get());
        }
        super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<RespT>(
            responseListener) {
          @Override
          public void onHeaders(Metadata headers) {
            super.onHeaders(headers);
          }
        }, headers);
      }
    };
  }
}

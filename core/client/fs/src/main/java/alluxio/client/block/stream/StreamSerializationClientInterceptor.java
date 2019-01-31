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

import alluxio.conf.AlluxioConfiguration;
import alluxio.conf.InstancedConfiguration;
import alluxio.conf.PropertyKey;
import alluxio.exception.status.UnauthenticatedException;
import alluxio.exception.status.UnavailableException;
import alluxio.grpc.AsyncCacheRequest;
import alluxio.grpc.AsyncCacheResponse;
import alluxio.grpc.BlockWorkerGrpc;
import alluxio.grpc.CreateLocalBlockRequest;
import alluxio.grpc.CreateLocalBlockResponse;
import alluxio.grpc.GrpcChannel;
import alluxio.grpc.GrpcChannelBuilder;
import alluxio.grpc.GrpcExceptionUtils;
import alluxio.grpc.GrpcManagedChannelPool;
import alluxio.grpc.MessageDeserializer;
import alluxio.grpc.OpenLocalBlockRequest;
import alluxio.grpc.OpenLocalBlockResponse;
import alluxio.grpc.PartialForwardingClientCall;
import alluxio.grpc.ReadRequest;
import alluxio.grpc.ReadResponse;
import alluxio.grpc.RemoveBlockRequest;
import alluxio.grpc.RemoveBlockResponse;
import alluxio.grpc.WriteRequest;
import alluxio.grpc.WriteResponse;
import alluxio.grpc.ZeroCopyUtils;
import alluxio.util.ConfigurationUtils;
import alluxio.util.network.NettyUtils;

import com.google.common.io.Closer;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;

import java.io.InputStream;

/**
 * Serialization interceptor for client.
 */
public class StreamSerializationClientInterceptor implements ClientInterceptor {
  private ZeroCopyUtils.StreamMarshaller marshaller = new ZeroCopyUtils.StreamMarshaller();
  @Override
  public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(
      MethodDescriptor<ReqT, RespT> method, CallOptions callOptions, Channel channel) {
    MessageDeserializer<RespT> responseDeserializer =
        callOptions.getOption(ZeroCopyUtils.RESPONSE_DESERILAIZER);
    ClientCall<InputStream, InputStream> call = channel.newCall(
        method.toBuilder(
            marshaller, marshaller).build(),
        callOptions);
    return new PartialForwardingClientCall<ReqT, RespT>() {

      @Override
      protected ClientCall<?, ?> delegate() {
        return call;
      }

      @Override
      public void sendMessage(ReqT message) {
        call.sendMessage(method.streamRequest(message));
      }

      @Override
      public void start(Listener<RespT> listener, Metadata headers) {
        call.start(new CallListener<InputStream>() {
          @Override
          protected Listener<?> delegate() {
            return listener;
          }

          @Override
          public void onClose(Status status, Metadata trailers) {
            if (responseDeserializer != null) {
              responseDeserializer.close();
            }
            listener.onClose(status, trailers);
          }

          @Override
          public void onMessage(InputStream message) {
            if (responseDeserializer != null) {
              try {
                RespT res = responseDeserializer.parseResponse(message);
                listener.onMessage(res);
              } catch (Exception e) {
                e.printStackTrace();
              }
            } else {
              listener.onMessage(method.parseResponse(message));
            }
          }
        }, headers);
      }
    };
  }
}

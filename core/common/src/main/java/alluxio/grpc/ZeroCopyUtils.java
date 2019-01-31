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

import com.google.common.base.Throwables;
import io.grpc.CallOptions;
import io.grpc.Drainable;
import io.grpc.MethodDescriptor;
import io.grpc.MethodDescriptor.Marshaller;
import io.grpc.ServerMethodDefinition;
import io.grpc.ServerServiceDefinition;
import io.grpc.ServiceDescriptor;
import io.grpc.internal.ReadableBuffer;
import io.netty.buffer.ByteBuf;
import org.jboss.netty.util.internal.ConcurrentIdentityHashMap;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class ZeroCopyUtils {
  public static CallOptions.Key<MessageDeserializer> RESPONSE_DESERILAIZER =
      CallOptions.Key.create("response deserializer");
  private static final Constructor<?> bufConstruct;
  private static final Field bufferList;
  private static final Field current;
  private static final Method listAdd;
  private static final Class<?> bufChainOut;
  private static final Field READABLE_BUFFER;
  private static final Class<?> BUFFER_INPUT_STREAM;
  static {
    Field tmpField = null;
    Class<?> tmpClazz = null;
    try {
      Class<?> clazz = Class.forName("io.grpc.internal.ReadableBuffers$BufferInputStream");

      Field f = clazz.getDeclaredField("buffer");
      f.setAccessible(true);
      // don't set until we've gotten past all exception cases.
      tmpField = f;
      tmpClazz = clazz;
    } catch (Exception e) {
      e.printStackTrace();
    }
    READABLE_BUFFER = tmpField;
    BUFFER_INPUT_STREAM = tmpClazz;

    Constructor<?> tmpConstruct = null;
    Field tmpBufferList = null;
    Field tmpCurrent = null;
    Class<?> tmpBufChainOut = null;
    Method tmpListAdd = null;

    try {
      Class<?> nwb = Class.forName("io.grpc.netty.NettyWritableBuffer");

      Constructor<?> tmpConstruct2 = nwb.getDeclaredConstructor(ByteBuf.class);
      tmpConstruct2.setAccessible(true);

      Class<?> tmpBufChainOut2 = Class.forName("io.grpc.internal.MessageFramer$BufferChainOutputStream");

      Field tmpBufferList2 = tmpBufChainOut2.getDeclaredField("bufferList");
      tmpBufferList2.setAccessible(true);

      Field tmpCurrent2 = tmpBufChainOut2.getDeclaredField("current");
      tmpCurrent2.setAccessible(true);

      Method tmpListAdd2 = List.class.getDeclaredMethod("add", Object.class);

      // output fields last.
      tmpConstruct = tmpConstruct2;
      tmpBufferList = tmpBufferList2;
      tmpCurrent = tmpCurrent2;
      tmpListAdd = tmpListAdd2;
      tmpBufChainOut = tmpBufChainOut2;

    } catch (Exception ex) {
      ex.printStackTrace();
    }

    bufConstruct = tmpConstruct;
    bufferList = tmpBufferList;
    current = tmpCurrent;
    listAdd = tmpListAdd;
    bufChainOut = tmpBufChainOut;

  }

  public static ReadableBuffer getReadableBuffer(InputStream is) {
    if (BUFFER_INPUT_STREAM == null || !is.getClass().equals(BUFFER_INPUT_STREAM)) {
      return null;
    }

    try {
      return (ReadableBuffer) READABLE_BUFFER.get(is);
    } catch (Exception ex) {
      throw Throwables.propagate(ex);
    }
  }

  public static boolean add(ByteBuf buf, OutputStream stream) throws IOException {
    if (bufChainOut == null) {
      return false;
    }

    if (!stream.getClass().equals(bufChainOut)) {
      return false;
    }

    try {
      if (current.get(stream) != null) {
        return false;
      }

      buf.retain();
      Object obj = bufConstruct.newInstance(buf);
      Object list = bufferList.get(stream);
      listAdd.invoke(list, obj);
      current.set(stream, obj);
      return true;
    } catch (IllegalAccessException | IllegalArgumentException | InvocationTargetException | InstantiationException e) {
      e.printStackTrace();
      return false;
    }
  }

  public static class DataBufferAttachedInputStream extends InputStream implements Drainable {
    private final InputStream mStream;
    private final ByteBuf mBuffer;

    public DataBufferAttachedInputStream(InputStream is, ByteBuf buffer) {
      mStream = is;
      mBuffer = buffer;
    }

    @Override
    public int read() throws IOException {
      return mStream.read();
    }

    @Override
    public int drainTo(OutputStream target) throws IOException {
      int bytesWritten = 0;
      if (mStream instanceof Drainable) {
        bytesWritten = ((Drainable) mStream).drainTo(target);
      }
      bytesWritten += mBuffer.readableBytes();
      if (mBuffer != null) {
        add(mBuffer, target);
      }
      return bytesWritten;
    }

    @Override
    public void close() throws IOException {
      mStream.close();
    }
  }

  public static class StreamMarshaller implements MethodDescriptor.Marshaller<InputStream> {

    @Override
    public InputStream stream(InputStream value) {
      return value;
    }

    @Override
    public InputStream parse(InputStream stream) {
      return stream;
    }
  }

  public static class SerilizationMarshaller<T> implements MethodDescriptor.Marshaller<T> {
    private final MessageSerializer<T> mSerializer;

    public SerilizationMarshaller(MessageSerializer<T> serializer) {
      mSerializer = serializer;
    }

    @Override
    public InputStream stream(T value) {
      return mSerializer.streamMessage(value);
    }

    @Override
    public T parse(InputStream stream) {
      return null;
    }
  }

  private static <ReqT, RespT>  ServerMethodDefinition<ReqT, RespT> interceptMethod(
      final ServerMethodDefinition<ReqT, RespT> definition,
      final Map<MethodDescriptor, Marshaller> marshallers) {
    MethodDescriptor<ReqT, RespT> descriptor = definition.getMethodDescriptor();
    Marshaller marshaller = marshallers.get(descriptor);
    if (marshaller != null) {
      return ServerMethodDefinition.create(
          descriptor.toBuilder(definition.getMethodDescriptor().getRequestMarshaller(), marshaller).build(),
          definition.getServerCallHandler());
    }
    return definition;
  }

  public static ServerServiceDefinition useZeroCopyMessages(
      final ServerServiceDefinition service,
      final Map<MethodDescriptor, Marshaller> marshallers) {
    List<ServerMethodDefinition<?, ?>> newMethods = new ArrayList<ServerMethodDefinition<?, ?>>();
    List<MethodDescriptor<?, ?>> newDescriptors = new ArrayList<MethodDescriptor<?, ?>>();
    // Intercepts the descriptors.
    for (final ServerMethodDefinition<?, ?> definition : service.getMethods()) {
      ServerMethodDefinition<?, ?> newMethod = interceptMethod(definition, marshallers);
      newDescriptors.add(newMethod.getMethodDescriptor());
      newMethods.add(newMethod);
    }
    // Build the new service descriptor
    final ServerServiceDefinition.Builder serviceBuilder = ServerServiceDefinition
        .builder(new ServiceDescriptor(service.getServiceDescriptor().getName(), newDescriptors));
    // Create the new service definiton.
    for (ServerMethodDefinition<?, ?> definition : newMethods) {
      serviceBuilder.addMethod(definition);
    }
    return serviceBuilder.build();
  }
}

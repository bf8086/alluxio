// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/messaging_transport.proto

package alluxio.grpc;

public final class MessagingTransportProto {
  private MessagingTransportProto() {}
  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistryLite registry) {
  }

  public static void registerAllExtensions(
      com.google.protobuf.ExtensionRegistry registry) {
    registerAllExtensions(
        (com.google.protobuf.ExtensionRegistryLite) registry);
  }
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_messaging_MessagingRequestHeader_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_messaging_MessagingRequestHeader_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_messaging_MessagingResponseHeader_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_messaging_MessagingResponseHeader_fieldAccessorTable;
  static final com.google.protobuf.Descriptors.Descriptor
    internal_static_alluxio_grpc_messaging_TransportMessage_descriptor;
  static final 
    com.google.protobuf.GeneratedMessageV3.FieldAccessorTable
      internal_static_alluxio_grpc_messaging_TransportMessage_fieldAccessorTable;

  public static com.google.protobuf.Descriptors.FileDescriptor
      getDescriptor() {
    return descriptor;
  }
  private static  com.google.protobuf.Descriptors.FileDescriptor
      descriptor;
  static {
    java.lang.String[] descriptorData = {
      "\n\036grpc/messaging_transport.proto\022\026alluxi" +
      "o.grpc.messaging\"+\n\026MessagingRequestHead" +
      "er\022\021\n\trequestId\030\001 \001(\003\"A\n\027MessagingRespon" +
      "seHeader\022\021\n\trequestId\030\001 \001(\003\022\023\n\013isThrowab" +
      "le\030\002 \001(\010\"\263\001\n\020TransportMessage\022E\n\rrequest" +
      "Header\030\001 \001(\0132..alluxio.grpc.messaging.Me" +
      "ssagingRequestHeader\022G\n\016responseHeader\030\002" +
      " \001(\0132/.alluxio.grpc.messaging.MessagingR" +
      "esponseHeader\022\017\n\007message\030\003 \001(\0142u\n\020Messag" +
      "ingService\022a\n\007connect\022(.alluxio.grpc.mes" +
      "saging.TransportMessage\032(.alluxio.grpc.m" +
      "essaging.TransportMessage(\0010\001B)\n\014alluxio" +
      ".grpcB\027MessagingTransportProtoP\001"
    };
    descriptor = com.google.protobuf.Descriptors.FileDescriptor
      .internalBuildGeneratedFileFrom(descriptorData,
        new com.google.protobuf.Descriptors.FileDescriptor[] {
        });
    internal_static_alluxio_grpc_messaging_MessagingRequestHeader_descriptor =
      getDescriptor().getMessageTypes().get(0);
    internal_static_alluxio_grpc_messaging_MessagingRequestHeader_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_messaging_MessagingRequestHeader_descriptor,
        new java.lang.String[] { "RequestId", });
    internal_static_alluxio_grpc_messaging_MessagingResponseHeader_descriptor =
      getDescriptor().getMessageTypes().get(1);
    internal_static_alluxio_grpc_messaging_MessagingResponseHeader_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_messaging_MessagingResponseHeader_descriptor,
        new java.lang.String[] { "RequestId", "IsThrowable", });
    internal_static_alluxio_grpc_messaging_TransportMessage_descriptor =
      getDescriptor().getMessageTypes().get(2);
    internal_static_alluxio_grpc_messaging_TransportMessage_fieldAccessorTable = new
      com.google.protobuf.GeneratedMessageV3.FieldAccessorTable(
        internal_static_alluxio_grpc_messaging_TransportMessage_descriptor,
        new java.lang.String[] { "RequestHeader", "ResponseHeader", "Message", });
  }

  // @@protoc_insertion_point(outer_class_scope)
}

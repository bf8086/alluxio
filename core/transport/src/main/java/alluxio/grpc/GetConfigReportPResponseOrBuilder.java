// Generated by the protocol buffer compiler.  DO NOT EDIT!
// source: grpc/meta_master.proto

package alluxio.grpc;

public interface GetConfigReportPResponseOrBuilder extends
    // @@protoc_insertion_point(interface_extends:alluxio.grpc.meta.GetConfigReportPResponse)
    com.google.protobuf.MessageOrBuilder {

  /**
   * <code>optional .alluxio.grpc.meta.ConfigCheckReport report = 1;</code>
   * @return Whether the report field is set.
   */
  boolean hasReport();
  /**
   * <code>optional .alluxio.grpc.meta.ConfigCheckReport report = 1;</code>
   * @return The report.
   */
  alluxio.grpc.ConfigCheckReport getReport();
  /**
   * <code>optional .alluxio.grpc.meta.ConfigCheckReport report = 1;</code>
   */
  alluxio.grpc.ConfigCheckReportOrBuilder getReportOrBuilder();
}

// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.2.0
// - protoc             v3.17.3
// source: rpc/common/tracing.proto

package common

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
	emptypb "google.golang.org/protobuf/types/known/emptypb"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.32.0 or later.
const _ = grpc.SupportPackageIsVersion7

// TracingServerClient is the client API for TracingServer service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type TracingServerClient interface {
	DumpTraces(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*Trace, error)
}

type tracingServerClient struct {
	cc grpc.ClientConnInterface
}

func NewTracingServerClient(cc grpc.ClientConnInterface) TracingServerClient {
	return &tracingServerClient{cc}
}

func (c *tracingServerClient) DumpTraces(ctx context.Context, in *emptypb.Empty, opts ...grpc.CallOption) (*Trace, error) {
	out := new(Trace)
	err := c.cc.Invoke(ctx, "/telepresence.common.TracingServer/DumpTraces", in, out, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// TracingServerServer is the server API for TracingServer service.
// All implementations must embed UnimplementedTracingServerServer
// for forward compatibility
type TracingServerServer interface {
	DumpTraces(context.Context, *emptypb.Empty) (*Trace, error)
	mustEmbedUnimplementedTracingServerServer()
}

// UnimplementedTracingServerServer must be embedded to have forward compatible implementations.
type UnimplementedTracingServerServer struct {
}

func (UnimplementedTracingServerServer) DumpTraces(context.Context, *emptypb.Empty) (*Trace, error) {
	return nil, status.Errorf(codes.Unimplemented, "method DumpTraces not implemented")
}
func (UnimplementedTracingServerServer) mustEmbedUnimplementedTracingServerServer() {}

// UnsafeTracingServerServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to TracingServerServer will
// result in compilation errors.
type UnsafeTracingServerServer interface {
	mustEmbedUnimplementedTracingServerServer()
}

func RegisterTracingServerServer(s grpc.ServiceRegistrar, srv TracingServerServer) {
	s.RegisterService(&TracingServer_ServiceDesc, srv)
}

func _TracingServer_DumpTraces_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error, interceptor grpc.UnaryServerInterceptor) (interface{}, error) {
	in := new(emptypb.Empty)
	if err := dec(in); err != nil {
		return nil, err
	}
	if interceptor == nil {
		return srv.(TracingServerServer).DumpTraces(ctx, in)
	}
	info := &grpc.UnaryServerInfo{
		Server:     srv,
		FullMethod: "/telepresence.common.TracingServer/DumpTraces",
	}
	handler := func(ctx context.Context, req interface{}) (interface{}, error) {
		return srv.(TracingServerServer).DumpTraces(ctx, req.(*emptypb.Empty))
	}
	return interceptor(ctx, in, info, handler)
}

// TracingServer_ServiceDesc is the grpc.ServiceDesc for TracingServer service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var TracingServer_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "telepresence.common.TracingServer",
	HandlerType: (*TracingServerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "DumpTraces",
			Handler:    _TracingServer_DumpTraces_Handler,
		},
	},
	Streams:  []grpc.StreamDesc{},
	Metadata: "rpc/common/tracing.proto",
}

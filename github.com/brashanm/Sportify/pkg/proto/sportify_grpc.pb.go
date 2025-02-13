// Code generated by protoc-gen-go-grpc. DO NOT EDIT.
// versions:
// - protoc-gen-go-grpc v1.5.1
// - protoc             v5.28.2
// source: pkg/proto/sportify.proto

package proto

import (
	context "context"
	grpc "google.golang.org/grpc"
	codes "google.golang.org/grpc/codes"
	status "google.golang.org/grpc/status"
)

// This is a compile-time assertion to ensure that this generated file
// is compatible with the grpc package it is being compiled against.
// Requires gRPC-Go v1.64.0 or later.
const _ = grpc.SupportPackageIsVersion9

const (
	NBAStream_Subscribe_FullMethodName = "/Sportify.NBAStream/Subscribe"
)

// NBAStreamClient is the client API for NBAStream service.
//
// For semantics around ctx use and closing/ending streaming RPCs, please refer to https://pkg.go.dev/google.golang.org/grpc/?tab=doc#ClientConn.NewStream.
type NBAStreamClient interface {
	Subscribe(ctx context.Context, in *SubscriptionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Update], error)
}

type nBAStreamClient struct {
	cc grpc.ClientConnInterface
}

func NewNBAStreamClient(cc grpc.ClientConnInterface) NBAStreamClient {
	return &nBAStreamClient{cc}
}

func (c *nBAStreamClient) Subscribe(ctx context.Context, in *SubscriptionRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[Update], error) {
	cOpts := append([]grpc.CallOption{grpc.StaticMethod()}, opts...)
	stream, err := c.cc.NewStream(ctx, &NBAStream_ServiceDesc.Streams[0], NBAStream_Subscribe_FullMethodName, cOpts...)
	if err != nil {
		return nil, err
	}
	x := &grpc.GenericClientStream[SubscriptionRequest, Update]{ClientStream: stream}
	if err := x.ClientStream.SendMsg(in); err != nil {
		return nil, err
	}
	if err := x.ClientStream.CloseSend(); err != nil {
		return nil, err
	}
	return x, nil
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type NBAStream_SubscribeClient = grpc.ServerStreamingClient[Update]

// NBAStreamServer is the server API for NBAStream service.
// All implementations must embed UnimplementedNBAStreamServer
// for forward compatibility.
type NBAStreamServer interface {
	Subscribe(*SubscriptionRequest, grpc.ServerStreamingServer[Update]) error
	mustEmbedUnimplementedNBAStreamServer()
}

// UnimplementedNBAStreamServer must be embedded to have
// forward compatible implementations.
//
// NOTE: this should be embedded by value instead of pointer to avoid a nil
// pointer dereference when methods are called.
type UnimplementedNBAStreamServer struct{}

func (UnimplementedNBAStreamServer) Subscribe(*SubscriptionRequest, grpc.ServerStreamingServer[Update]) error {
	return status.Errorf(codes.Unimplemented, "method Subscribe not implemented")
}
func (UnimplementedNBAStreamServer) mustEmbedUnimplementedNBAStreamServer() {}
func (UnimplementedNBAStreamServer) testEmbeddedByValue()                   {}

// UnsafeNBAStreamServer may be embedded to opt out of forward compatibility for this service.
// Use of this interface is not recommended, as added methods to NBAStreamServer will
// result in compilation errors.
type UnsafeNBAStreamServer interface {
	mustEmbedUnimplementedNBAStreamServer()
}

func RegisterNBAStreamServer(s grpc.ServiceRegistrar, srv NBAStreamServer) {
	// If the following call pancis, it indicates UnimplementedNBAStreamServer was
	// embedded by pointer and is nil.  This will cause panics if an
	// unimplemented method is ever invoked, so we test this at initialization
	// time to prevent it from happening at runtime later due to I/O.
	if t, ok := srv.(interface{ testEmbeddedByValue() }); ok {
		t.testEmbeddedByValue()
	}
	s.RegisterService(&NBAStream_ServiceDesc, srv)
}

func _NBAStream_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	m := new(SubscriptionRequest)
	if err := stream.RecvMsg(m); err != nil {
		return err
	}
	return srv.(NBAStreamServer).Subscribe(m, &grpc.GenericServerStream[SubscriptionRequest, Update]{ServerStream: stream})
}

// This type alias is provided for backwards compatibility with existing code that references the prior non-generic stream type by name.
type NBAStream_SubscribeServer = grpc.ServerStreamingServer[Update]

// NBAStream_ServiceDesc is the grpc.ServiceDesc for NBAStream service.
// It's only intended for direct use with grpc.RegisterService,
// and not to be introspected or modified (even as a copy)
var NBAStream_ServiceDesc = grpc.ServiceDesc{
	ServiceName: "Sportify.NBAStream",
	HandlerType: (*NBAStreamServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _NBAStream_Subscribe_Handler,
			ServerStreams: true,
		},
	},
	Metadata: "pkg/proto/sportify.proto",
}

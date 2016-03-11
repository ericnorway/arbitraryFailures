// Code generated by protoc-gen-go.
// source: broker.proto
// DO NOT EDIT!

/*
Package proto is a generated protocol buffer package.

It is generated from these files:
	broker.proto

It has these top-level messages:
	Publication
	ChainMAC
	PubResponse
	EchoResponse
	ReadyResponse
	ChainResponse
	SubRequest
*/
package proto

import proto1 "github.com/golang/protobuf/proto"
import fmt "fmt"
import math "math"

import (
	context "golang.org/x/net/context"
	grpc "google.golang.org/grpc"
)

// Reference imports to suppress errors if they are not otherwise used.
var _ = proto1.Marshal
var _ = fmt.Errorf
var _ = math.Inf

type Publication struct {
	PubType       uint32      `protobuf:"varint,1,opt,name=PubType" json:"PubType,omitempty"`
	PublisherID   uint64      `protobuf:"varint,2,opt,name=PublisherID" json:"PublisherID,omitempty"`
	PublicationID int64       `protobuf:"zigzag64,3,opt,name=PublicationID" json:"PublicationID,omitempty"`
	TopicID       uint64      `protobuf:"varint,4,opt,name=TopicID" json:"TopicID,omitempty"`
	BrokerID      uint64      `protobuf:"varint,5,opt,name=BrokerID" json:"BrokerID,omitempty"`
	Contents      [][]byte    `protobuf:"bytes,6,rep,name=Contents,proto3" json:"Contents,omitempty"`
	MAC           []byte      `protobuf:"bytes,7,opt,name=MAC,proto3" json:"MAC,omitempty"`
	ChainMACs     []*ChainMAC `protobuf:"bytes,8,rep,name=ChainMACs" json:"ChainMACs,omitempty"`
}

func (m *Publication) Reset()         { *m = Publication{} }
func (m *Publication) String() string { return proto1.CompactTextString(m) }
func (*Publication) ProtoMessage()    {}

func (m *Publication) GetChainMACs() []*ChainMAC {
	if m != nil {
		return m.ChainMACs
	}
	return nil
}

type ChainMAC struct {
	From string `protobuf:"bytes,1,opt,name=From" json:"From,omitempty"`
	To   string `protobuf:"bytes,2,opt,name=To" json:"To,omitempty"`
	MAC  []byte `protobuf:"bytes,3,opt,name=MAC,proto3" json:"MAC,omitempty"`
}

func (m *ChainMAC) Reset()         { *m = ChainMAC{} }
func (m *ChainMAC) String() string { return proto1.CompactTextString(m) }
func (*ChainMAC) ProtoMessage()    {}

type PubResponse struct {
	Accepted       bool   `protobuf:"varint,1,opt,name=Accepted" json:"Accepted,omitempty"`
	RequestHistory bool   `protobuf:"varint,2,opt,name=RequestHistory" json:"RequestHistory,omitempty"`
	Blocked        bool   `protobuf:"varint,3,opt,name=Blocked" json:"Blocked,omitempty"`
	TopicID        uint64 `protobuf:"varint,4,opt,name=TopicID" json:"TopicID,omitempty"`
}

func (m *PubResponse) Reset()         { *m = PubResponse{} }
func (m *PubResponse) String() string { return proto1.CompactTextString(m) }
func (*PubResponse) ProtoMessage()    {}

type EchoResponse struct {
}

func (m *EchoResponse) Reset()         { *m = EchoResponse{} }
func (m *EchoResponse) String() string { return proto1.CompactTextString(m) }
func (*EchoResponse) ProtoMessage()    {}

type ReadyResponse struct {
}

func (m *ReadyResponse) Reset()         { *m = ReadyResponse{} }
func (m *ReadyResponse) String() string { return proto1.CompactTextString(m) }
func (*ReadyResponse) ProtoMessage()    {}

type ChainResponse struct {
}

func (m *ChainResponse) Reset()         { *m = ChainResponse{} }
func (m *ChainResponse) String() string { return proto1.CompactTextString(m) }
func (*ChainResponse) ProtoMessage()    {}

type SubRequest struct {
	SubscriberID uint64   `protobuf:"varint,1,opt,name=SubscriberID" json:"SubscriberID,omitempty"`
	TopicIDs     []uint64 `protobuf:"varint,2,rep,name=TopicIDs" json:"TopicIDs,omitempty"`
}

func (m *SubRequest) Reset()         { *m = SubRequest{} }
func (m *SubRequest) String() string { return proto1.CompactTextString(m) }
func (*SubRequest) ProtoMessage()    {}

func init() {
	proto1.RegisterType((*Publication)(nil), "proto.Publication")
	proto1.RegisterType((*ChainMAC)(nil), "proto.ChainMAC")
	proto1.RegisterType((*PubResponse)(nil), "proto.PubResponse")
	proto1.RegisterType((*EchoResponse)(nil), "proto.EchoResponse")
	proto1.RegisterType((*ReadyResponse)(nil), "proto.ReadyResponse")
	proto1.RegisterType((*ChainResponse)(nil), "proto.ChainResponse")
	proto1.RegisterType((*SubRequest)(nil), "proto.SubRequest")
}

// Reference imports to suppress errors if they are not otherwise used.
var _ context.Context
var _ grpc.ClientConn

// Client API for PubBroker service

type PubBrokerClient interface {
	Publish(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*PubResponse, error)
}

type pubBrokerClient struct {
	cc *grpc.ClientConn
}

func NewPubBrokerClient(cc *grpc.ClientConn) PubBrokerClient {
	return &pubBrokerClient{cc}
}

func (c *pubBrokerClient) Publish(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*PubResponse, error) {
	out := new(PubResponse)
	err := grpc.Invoke(ctx, "/proto.PubBroker/Publish", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for PubBroker service

type PubBrokerServer interface {
	Publish(context.Context, *Publication) (*PubResponse, error)
}

func RegisterPubBrokerServer(s *grpc.Server, srv PubBrokerServer) {
	s.RegisterService(&_PubBroker_serviceDesc, srv)
}

func _PubBroker_Publish_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Publication)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(PubBrokerServer).Publish(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _PubBroker_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.PubBroker",
	HandlerType: (*PubBrokerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Publish",
			Handler:    _PubBroker_Publish_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

// Client API for SubBroker service

type SubBrokerClient interface {
	Subscribe(ctx context.Context, opts ...grpc.CallOption) (SubBroker_SubscribeClient, error)
}

type subBrokerClient struct {
	cc *grpc.ClientConn
}

func NewSubBrokerClient(cc *grpc.ClientConn) SubBrokerClient {
	return &subBrokerClient{cc}
}

func (c *subBrokerClient) Subscribe(ctx context.Context, opts ...grpc.CallOption) (SubBroker_SubscribeClient, error) {
	stream, err := grpc.NewClientStream(ctx, &_SubBroker_serviceDesc.Streams[0], c.cc, "/proto.SubBroker/Subscribe", opts...)
	if err != nil {
		return nil, err
	}
	x := &subBrokerSubscribeClient{stream}
	return x, nil
}

type SubBroker_SubscribeClient interface {
	Send(*SubRequest) error
	Recv() (*Publication, error)
	grpc.ClientStream
}

type subBrokerSubscribeClient struct {
	grpc.ClientStream
}

func (x *subBrokerSubscribeClient) Send(m *SubRequest) error {
	return x.ClientStream.SendMsg(m)
}

func (x *subBrokerSubscribeClient) Recv() (*Publication, error) {
	m := new(Publication)
	if err := x.ClientStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

// Server API for SubBroker service

type SubBrokerServer interface {
	Subscribe(SubBroker_SubscribeServer) error
}

func RegisterSubBrokerServer(s *grpc.Server, srv SubBrokerServer) {
	s.RegisterService(&_SubBroker_serviceDesc, srv)
}

func _SubBroker_Subscribe_Handler(srv interface{}, stream grpc.ServerStream) error {
	return srv.(SubBrokerServer).Subscribe(&subBrokerSubscribeServer{stream})
}

type SubBroker_SubscribeServer interface {
	Send(*Publication) error
	Recv() (*SubRequest, error)
	grpc.ServerStream
}

type subBrokerSubscribeServer struct {
	grpc.ServerStream
}

func (x *subBrokerSubscribeServer) Send(m *Publication) error {
	return x.ServerStream.SendMsg(m)
}

func (x *subBrokerSubscribeServer) Recv() (*SubRequest, error) {
	m := new(SubRequest)
	if err := x.ServerStream.RecvMsg(m); err != nil {
		return nil, err
	}
	return m, nil
}

var _SubBroker_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.SubBroker",
	HandlerType: (*SubBrokerServer)(nil),
	Methods:     []grpc.MethodDesc{},
	Streams: []grpc.StreamDesc{
		{
			StreamName:    "Subscribe",
			Handler:       _SubBroker_Subscribe_Handler,
			ServerStreams: true,
			ClientStreams: true,
		},
	},
}

// Client API for InterBroker service

type InterBrokerClient interface {
	Echo(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*EchoResponse, error)
	Ready(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*ReadyResponse, error)
	Chain(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*ChainResponse, error)
}

type interBrokerClient struct {
	cc *grpc.ClientConn
}

func NewInterBrokerClient(cc *grpc.ClientConn) InterBrokerClient {
	return &interBrokerClient{cc}
}

func (c *interBrokerClient) Echo(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*EchoResponse, error) {
	out := new(EchoResponse)
	err := grpc.Invoke(ctx, "/proto.InterBroker/Echo", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *interBrokerClient) Ready(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*ReadyResponse, error) {
	out := new(ReadyResponse)
	err := grpc.Invoke(ctx, "/proto.InterBroker/Ready", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func (c *interBrokerClient) Chain(ctx context.Context, in *Publication, opts ...grpc.CallOption) (*ChainResponse, error) {
	out := new(ChainResponse)
	err := grpc.Invoke(ctx, "/proto.InterBroker/Chain", in, out, c.cc, opts...)
	if err != nil {
		return nil, err
	}
	return out, nil
}

// Server API for InterBroker service

type InterBrokerServer interface {
	Echo(context.Context, *Publication) (*EchoResponse, error)
	Ready(context.Context, *Publication) (*ReadyResponse, error)
	Chain(context.Context, *Publication) (*ChainResponse, error)
}

func RegisterInterBrokerServer(s *grpc.Server, srv InterBrokerServer) {
	s.RegisterService(&_InterBroker_serviceDesc, srv)
}

func _InterBroker_Echo_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Publication)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(InterBrokerServer).Echo(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _InterBroker_Ready_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Publication)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(InterBrokerServer).Ready(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

func _InterBroker_Chain_Handler(srv interface{}, ctx context.Context, dec func(interface{}) error) (interface{}, error) {
	in := new(Publication)
	if err := dec(in); err != nil {
		return nil, err
	}
	out, err := srv.(InterBrokerServer).Chain(ctx, in)
	if err != nil {
		return nil, err
	}
	return out, nil
}

var _InterBroker_serviceDesc = grpc.ServiceDesc{
	ServiceName: "proto.InterBroker",
	HandlerType: (*InterBrokerServer)(nil),
	Methods: []grpc.MethodDesc{
		{
			MethodName: "Echo",
			Handler:    _InterBroker_Echo_Handler,
		},
		{
			MethodName: "Ready",
			Handler:    _InterBroker_Ready_Handler,
		},
		{
			MethodName: "Chain",
			Handler:    _InterBroker_Chain_Handler,
		},
	},
	Streams: []grpc.StreamDesc{},
}

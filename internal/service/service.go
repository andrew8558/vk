package service

import (
	"context"
	"log"
	"subpub/internal/pb"
	"subpub/internal/subpub"

	"github.com/golang/protobuf/ptypes/empty"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Service struct {
	pb.UnimplementedPubSubServer
	subPub subpub.SubPub
}

func NewService(subPub subpub.SubPub) *Service {
	return &Service{
		subPub: subPub,
	}
}

func (s *Service) Publish(ctx context.Context, req *pb.PublishRequest) (*empty.Empty, error) {
	data := req.GetData()
	if data == "" {
		return &empty.Empty{}, status.Errorf(codes.InvalidArgument, "data isn't specified")
	}

	key := req.GetKey()
	if key == "" {
		return &empty.Empty{}, status.Errorf(codes.InvalidArgument, "key isn't specified")
	}

	err := s.subPub.Publish(key, data)
	if err != nil {
		return nil, status.Error(codes.Internal, err.Error())
	}

	return &empty.Empty{}, nil
}

func (s *Service) Subscribe(req *pb.SubscribeRequest, stream pb.PubSub_SubscribeServer) error {
	key := req.GetKey()
	if key == "" {
		return status.Errorf(codes.InvalidArgument, "key isn't specified")
	}

	subscription, err := s.subPub.Subscribe(key, func(msg interface{}) {
		data, ok := msg.(string)
		if !ok {
			log.Printf("invalid message type: %T", msg)
			return
		}
		if err := stream.Send(&pb.Event{Data: data}); err != nil {
			log.Printf("send failed with error: %v", err)
		}
	})

	if err != nil {
		switch err {
		case subpub.ErrClosed:
			return status.Error(codes.Unavailable, err.Error())
		default:
			return status.Error(codes.Internal, err.Error())
		}
	}

	defer subscription.Unsubscribe()
	<-stream.Context().Done()

	return nil
}

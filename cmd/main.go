package main

import (
	"context"
	"log"
	"net"
	"os"
	"os/signal"
	"subpub/internal/pb"
	"subpub/internal/service"
	"subpub/internal/subpub"
	"syscall"

	"github.com/joho/godotenv"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

func main() {
	if err := godotenv.Load("configs/config.env"); err != nil {
		log.Print("No .env file found")
		return
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	grpcAddr := os.Getenv("GRPC_ADDR")
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	defer lis.Close()

	sp := subpub.NewSubPub()
	svc := service.NewService(sp)
	grpcServer := grpc.NewServer()
	pb.RegisterPubSubServer(grpcServer, svc)
	reflection.Register(grpcServer)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)

	go func() {
		sig := <-sigCh
		log.Printf("received program end signal: %s", sig)
		grpcServer.Stop()
		if err := sp.Close(ctx); err != nil {
			log.Printf("close subpub with error: %v", err)
		}
	}()

	log.Printf("grpc server start listening on addr: %s", grpcAddr)
	if err := grpcServer.Serve(lis); err != nil {
		log.Printf("grpc server stopped: %v", err)
	}
}

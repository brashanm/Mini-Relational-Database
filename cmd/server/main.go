// cmd/server/main.go
package main

import (
	"log"
	"net"
	"time"

	pb "github.com/yourusername/nbastream/pkg/proto"
	"github.com/yourusername/nbastream/pkg/data"
	"github.com/yourusername/nbastream/pkg/metrics"
	"github.com/yourusername/nbastream/pkg/subscription"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type server struct {
	pb.UnimplementedNBAStreamServer
	subManager *subscription.Manager
}

func (s *server) Subscribe(req *pb.SubscriptionRequest, stream pb.NBAStream_SubscribeServer) error {
	// Register the client’s subscription.
	sub, err := s.subManager.Register(req)
	if err != nil {
		return err
	}
	defer s.subManager.Unregister(sub.ID)

	// Loop: send updates as they arrive.
	for {
		select {
		case update := <-sub.Updates:
			if err := stream.Send(update); err != nil {
				log.Printf("Error sending update: %v", err)
				return err
			}
		case <-stream.Context().Done():
			log.Printf("Client disconnected, subscription ID: %s", sub.ID)
			return nil
		}
	}
}

func main() {
	// Start Prometheus metrics (exposed on port 9090).
	metrics.StartHTTPServer(":9090")

	// Create the subscription manager.
	subManager := subscription.NewManager()

	// Start the data fetcher in a separate goroutine.
	go func() {
		ticker := time.NewTicker(1 * time.Minute)
		defer ticker.Stop()
		for {
			<-ticker.C
			if err := data.FetchAndDispatch(subManager); err != nil {
				log.Printf("Error in data fetcher: %v", err)
			}
		}
	}()
	// Do an initial fetch shortly after start.
	go func() {
		// Wait a couple of seconds before the first fetch.
		// (This ensures that subscriptions can be set up soon after startup.)
		time.Sleep(2 * time.Second)
		if err := data.FetchAndDispatch(subManager); err != nil {
			log.Printf("Initial fetch error: %v", err)
		}
	}()

	// Start the gRPC server.
	lis, err := net.Listen("tcp", ":50051")
	if err != nil {
		log.Fatalf("Failed to listen: %v", err)
	}
	grpcServer := grpc.NewServer()
	pb.RegisterNBAStreamServer(grpcServer, &server{subManager: subManager})

	// Enable reflection (useful for debugging with grpcurl or similar tools).
	reflection.Register(grpcServer)

	log.Println("gRPC server listening on :50051")
	if err := grpcServer.Serve(lis); err != nil {
		log.Fatalf("Failed to serve: %v", err)
	}
}

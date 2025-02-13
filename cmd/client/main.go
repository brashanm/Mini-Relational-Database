// cmd/client/main.go
package main

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/yourusername/nbastream/pkg/proto"
	"google.golang.org/grpc"
)

func main() {
	// Connect to the gRPC server.
	conn, err := grpc.Dial("localhost:50051", grpc.WithInsecure())
	if err != nil {
		log.Fatalf("Failed to connect to server: %v", err)
	}
	defer conn.Close()
	client := pb.NewNBAStreamClient(conn)

	reader := bufio.NewReader(os.Stdin)
	fmt.Println("NBA Real‑Time Subscription CLI")
	fmt.Println("Enter commands in the format: subscribe [team|player] <Name>")
	fmt.Println("Type 'exit' to quit.")

	for {
		fmt.Print("> ")
		input, err := reader.ReadString('\n')
		if err != nil {
			log.Printf("Error reading input: %v", err)
			continue
		}
		input = strings.TrimSpace(input)
		if input == "exit" {
			break
		}

		parts := strings.Split(input, " ")
		if len(parts) < 3 || parts[0] != "subscribe" {
			fmt.Println("Invalid command. Usage: subscribe [team|player] <Name>")
			continue
		}

		subType := parts[1]
		name := strings.Join(parts[2:], " ")
		req := &pb.SubscriptionRequest{}
		if subType == "team" {
			req.Team = name
		} else if subType == "player" {
			req.Player = name
		} else {
			fmt.Println("Unknown subscription type. Use 'team' or 'player'.")
			continue
		}

		// Create a cancellable context.
		ctx, cancel := context.WithCancel(context.Background())
		stream, err := client.Subscribe(ctx, req)
		if err != nil {
			fmt.Printf("Error subscribing: %v\n", err)
			cancel()
			continue
		}
		fmt.Printf("Subscribed to %s: %s\n", subType, name)

		// Start a goroutine to listen for streamed updates.
		done := make(chan struct{})
		go func() {
			for {
				update, err := stream.Recv()
				if err != nil {
					fmt.Printf("Subscription ended: %v\n", err)
					break
				}
				fmt.Printf("[Update] %s: %s\n", update.Type, update.Message)
			}
			close(done)
		}()

		fmt.Println("Press ENTER to unsubscribe.")
		_, _ = reader.ReadString('\n')
		cancel()
		<-done
		fmt.Println("Unsubscribed.")
	}

	fmt.Println("Exiting CLI.")
}

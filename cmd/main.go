package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/Jeyakaran-tech/pubSubMySQL/types"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	ctx := context.Background()
	// Start a fake server running locally.
	srv := pstest.NewServer()
	defer srv.Close()
	// Connect to the server without using TLS.
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	// Use the connection when creating a pubsub client.
	pubSubClient, err := pubsub.NewClient(ctx, "LMS", option.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}
	defer pubSubClient.Close()

	topic, err := pubSubClient.CreateTopic(ctx, "Pipeline-MySQL")
	if err != nil {
		panic(err)
	}

	message := types.Message{
		ServiceName: "xkhsd",
		Payload:     "dkjgsdukshfas",
		Severity:    "debug",
		Timestamp:   time.Now(),
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	receive(ctx, topic.Publish(ctx, &pubsub.Message{Data: bytes}), pubSubClient, topic)
}

func receive(ctx context.Context, psResult *pubsub.PublishResult, pubSubClient *pubsub.Client, topic *pubsub.Topic) {

	sub, createSubErr := pubSubClient.CreateSubscription(context.Background(), "LMS_Subscription",
		pubsub.SubscriptionConfig{Topic: topic})

	if createSubErr != nil {
		panic(createSubErr)
	}

	receiveErr := sub.Receive(ctx, func(ctx context.Context, m *pubsub.Message) {
		fmt.Println(string(m.Data))
		fmt.Println("Data Received")
		m.Ack() // Acknowledge that we've consumed the message.
	})
	if receiveErr != nil {
		log.Println(receiveErr)
	}
	sub.Delete(ctx)

}

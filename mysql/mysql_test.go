package mysql_test

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"cloud.google.com/go/pubsub"
	"cloud.google.com/go/pubsub/pstest"
	"github.com/Jeyakaran-tech/pubSubMySQL/mysql"
	r "github.com/Jeyakaran-tech/pubSubMySQL/types"
	"google.golang.org/api/option"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var (
	repo r.Repository
)
var message = &r.Message{
	ID:          1,
	ServiceName: "Immediate payments",
	Payload:     "multiple",
	Severity:    "info",
	Timestamp:   time.Now(),
}
var (
	user     = "docker"
	password = "secret"
	db       = "user"
	port     = "3308"
	dialect  = "mysql"
	dsn      = "%s:%s@tcp(localhost:%s)/%s"
	idleConn = 25
	maxConn  = 25
)

func TestMain(m *testing.M) {
	ctx := context.Background()
	srv := pstest.NewServer()
	defer srv.Close()
	conn, err := grpc.Dial(srv.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		panic(err)
	}
	defer conn.Close()
	pubSubClient, err := pubsub.NewClient(ctx, "LMS", option.WithGRPCConn(conn))
	if err != nil {
		panic(err)
	}
	defer pubSubClient.Close()

	topic, err := pubSubClient.CreateTopic(ctx, "Pipeline-MySQL")
	if err != nil {
		panic(err)
	}

	bytes, err := json.Marshal(message)
	if err != nil {
		panic(err)
	}
	topic.Publish(ctx, &pubsub.Message{Data: bytes})

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
	cfg, _ := sub.Config(ctx)
	sub.ReceiveSettings.MaxExtension = cfg.AckDeadline

	pool, err := dockertest.NewPool("")
	if err != nil {
		log.Fatalf("Could not connect to docker: %s", err)
	}

	opts := dockertest.RunOptions{
		Repository: "bitnami/mysql",
		Tag:        "5.7",
		Env: []string{
			"MYSQL_ROOT_PASSWORD=root",
			"MYSQL_USER=" + user,
			"MYSQL_PASSWORD=" + password,
			"MYSQL_DATABASE=" + db,
		},
		ExposedPorts: []string{"3306"},
		PortBindings: map[docker.Port][]docker.PortBinding{
			"3306": {
				{HostIP: "0.0.0.0", HostPort: port},
			},
		},
	}

	resource, err := pool.RunWithOptions(&opts)
	if err != nil {
		log.Fatalf("Could not start resource: %s", err.Error())
	}

	dsn = fmt.Sprintf(dsn, user, password, port, db)
	if err = pool.Retry(func() error {
		repo, err = mysql.NewRepository(dialect, dsn, idleConn, maxConn)
		return err
	}); err != nil {
		log.Fatalf("Could not connect to docker: %s", err.Error())
	}

	defer func() {
		repo.Close()
	}()

	err = repo.Up()
	if err != nil {
		panic(err)
	}

	code := m.Run()

	if err := pool.Purge(resource); err != nil {
		log.Fatalf("Could not purge resource: %s", err)
	}

	os.Exit(code)
}

func TestCreate(t *testing.T) {
	err := repo.Create(message)
	assert.NoError(t, err)
}

func TestFind(t *testing.T) {
	users, err := repo.Find()
	assert.NotEmpty(t, users)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(users))
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
		// context.CancelFunc()
	})
	if receiveErr != nil {
		log.Println(receiveErr)
	}
	cfg, _ := sub.Config(ctx)

	sub.ReceiveSettings.MaxExtension = cfg.AckDeadline
}

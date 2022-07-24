package mysql_test

import (
	"fmt"
	"log"
	"os"
	"testing"
	"time"

	"github.com/Jeyakaran-tech/pubSubMySQL/mysql"
	r "github.com/Jeyakaran-tech/pubSubMySQL/types"

	"github.com/ory/dockertest/v3"
	"github.com/ory/dockertest/v3/docker"
	"github.com/stretchr/testify/assert"
)

var (
	repo r.Repository
)

var u = &r.Message{
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
	err := repo.Create(u)
	assert.NoError(t, err)
}

func TestFind(t *testing.T) {
	users, err := repo.Find()
	assert.NotEmpty(t, users)
	assert.NoError(t, err)
	assert.NotEqual(t, 0, len(users))
}

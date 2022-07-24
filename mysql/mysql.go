//
//  integration-test-golang
//
//  Copyright © 2020. All rights reserved.
//

package mysql

import (
	"context"
	"database/sql"
	"time"

	"github.com/Jeyakaran-tech/pubSubMySQL/types"
	_ "github.com/go-sql-driver/mysql"
)

type repository struct {
	db *sql.DB
}

// NewRepository will create a variable that represent the Repository struct
func NewRepository(dialect, dsn string, idleConn, maxConn int) (types.Repository, error) {
	db, err := sql.Open(dialect, dsn)
	if err != nil {
		return nil, err
	}

	err = db.Ping()
	if err != nil {
		return nil, err
	}

	db.SetMaxIdleConns(idleConn)
	db.SetMaxOpenConns(maxConn)

	return &repository{db}, nil
}

// Close attaches the provider and close the connection
func (r *repository) Close() {
	r.db.Close()
}

// Up attaches the provider and create the table
func (r *repository) Up() error {
	ctx := context.Background()

	query1 :=
		"CREATE TABLE IF NOT EXISTS service_logs (" +
			"service_name VARCHAR(100) NOT NULL," +
			"payload VARCHAR(2048) NOT NULL," +
			"severity ENUM(\"debug\", \"info\", \"warn\", \"error\", \"fatal\") NOT NULL," +
			"timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL," +
			"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL" +
			")"

	query2 :=
		"CREATE TABLE service_severity (" +
			"service_name VARCHAR(100) NOT NULL," +
			"severity ENUM(\"debug\", \"info\", \"warn\", \"error\", \"fatal\") NOT NULL," +
			"count INT(4) NOT NULL," +
			"created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP NOT NULL" +
			")"

	stmt1, err1 := r.db.PrepareContext(ctx, query1)
	if err1 != nil {
		return err1
	}
	defer stmt1.Close()

	_, err1 = stmt1.ExecContext(ctx)
	if err1 != nil {
		return err1
	}

	stmt2, err2 := r.db.PrepareContext(ctx, query2)
	if err2 != nil {
		return err2
	}
	defer stmt2.Close()

	_, err2 = stmt2.ExecContext(ctx)
	if err2 != nil {
		return err2
	}
	return nil

}

// Find attaches the user repository and find all data
func (r *repository) Find() ([]*types.Message, error) {
	messages := make([]*types.Message, 0)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, "SELECT id, name, email, phone FROM users")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	for rows.Next() {
		m := new(types.Message)
		err = rows.Scan(
			&m.ID,
			&m.ServiceName,
			&m.Payload,
			&m.Severity,
			&m.Timestamp,
		)

		if err != nil {
			return nil, err
		}
		messages = append(messages, m)
	}

	return messages, nil
}

// Create attaches the user repository and creating the data
func (r *repository) Create(message *types.Message) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	query := "INSERT INTO users (id, name, email, phone) VALUES (?, ?, ?, ?)"
	stmt, err := r.db.PrepareContext(ctx, query)
	if err != nil {
		return err
	}
	defer stmt.Close()

	_, err = stmt.ExecContext(ctx, message.ID, message.ServiceName, message.Payload, message.Severity, message.Timestamp)
	return err
}

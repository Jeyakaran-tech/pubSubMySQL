package types

import "time"

// Repository represent the repositories
type Repository interface {
	Close()
	Up() error
	Find() ([]*Message, error)
	Create(user *Message) error
}

// UserModel represent the user model
type Message struct {
	ID          int       `json:"id"`
	ServiceName string    `json:"service_name"`
	Payload     string    `json:"payload"`
	Severity    string    `json:"severity"`
	Timestamp   time.Time `json:"timestamp"`
}

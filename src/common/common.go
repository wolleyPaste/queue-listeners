package common

import "time"

type Paste struct {
	ID string

	Title string
	Text  string

	Password string

	ExpirationDate time.Time
	CreationDate   time.Time
}

type PostgreSQL struct {
	DBName string

	Host string
	Port string

	Username string
	Password string
}

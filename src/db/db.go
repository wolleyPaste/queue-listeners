package db

import (
	"database/sql"
	"fmt"
	"queue-listeners/src/common"

	_ "github.com/lib/pq"
)

var db *sql.DB

// InitPostgres initializes the PostgreSQL connection
func InitPostgres(postgreSQLDB common.PostgreSQL) error {
	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable",
		postgreSQLDB.Host, postgreSQLDB.Port, postgreSQLDB.Username, postgreSQLDB.Password, postgreSQLDB.DBName)
	var err error
	db, err = sql.Open("postgres", connStr)
	if err != nil {
		return err
	}

	return db.Ping()
}

func InsertPaste(paste common.Paste) error {
	_, err := db.Exec(`
	INSERT INTO pastes (id, text, title, password, expiration, creation)
	VALUES ($1, $2, $3, $4, $5, $6)`, paste.ID, paste.Text, paste.Title, paste.Password, paste.ExpirationDate, paste.CreationDate)

	return err
}

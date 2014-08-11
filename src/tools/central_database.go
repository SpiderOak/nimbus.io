package tools

import (
	"fmt"
	"os"
)

import (
	"database/sql"
	_ "github.com/lib/pq"
)

var (
	sqlDB *sql.DB
)

// OpenCentralDatabase returns a *sql.DB pointer.
// This is NOT a database connection
// see http://go-database-sql.org/accessing.html
func OpenCentralDatabase() (*sql.DB, error) {
	var err error

	if sqlDB == nil {
		if sqlDB, err = openCentralDatabase(); err != nil {
			return nil, err
		}
	}

	return sqlDB, err
}

func openCentralDatabase() (*sql.DB, error) {
	databaseName := "nimbusio_central"

	databaseHost := os.Getenv("NIMBUSIO_CENTRAL_DATABASE_HOST")
	if databaseHost == "" {
		databaseHost = "localhost"
	}
	databasePort := os.Getenv("NIMBUSIO_CENTRAL_DATABASE_PORT")
	if databasePort == "" {
		databasePort = "5432"
	}
	databaseUser := os.Getenv("NIMBUSIO_CENTRAL_USER")
	if databaseUser == "" {
		databaseUser = "nimbusio_central_user"
	}

	databasePassword := os.Getenv("NIMBUSIO_CENTRAL_USER_PASSWORD")

	// go-pgsql gets a kernal panic if password is an empty string
	if databasePassword == "" {
		databasePassword = "none"
	}

	dataSourceName := fmt.Sprintf(
		"dbname=%s host=%s port=%s user=%s password=%s sslmode=%s",
		databaseName, databaseHost, databasePort, databaseUser,
		databasePassword, "disable")

	return sql.Open("postgres", dataSourceName)
}

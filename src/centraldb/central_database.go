package centraldb

import (
	"fmt"
	"os"
)

import (
	"database/sql"
	_ "github.com/lib/pq"
)

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

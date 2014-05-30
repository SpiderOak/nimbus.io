package tools

import (
	"fmt"
	"os"
	"strings"
)

import (
	"database/sql"
	_ "github.com/jbarham/gopgsqldriver"
)

// OpenNodeDatabase returns a *sql.DB pointer.
// This is NOT a database connection
// see http://go-database-sql.org/accessing.html
func OpenNodeDatabase(nodeName, password, host, port string) (*sql.DB, error) {
	databaseName := fmt.Sprintf("nimbusio_node.%s", nodeName)

	databaseUser := fmt.Sprintf("nimbusio_node_user_%s",
		strings.Replace(nodeName, "-", "_", -1))

	// go-pgsql gets a kernal panic if password is an empty string
	if password == "" {
		password = "none"
	}

	dataSourceName := fmt.Sprintf(
		"dbname=%s host=%s port=%s user=%s password=%s",
		databaseName, host, port, databaseUser, password)

	return sql.Open("postgres", dataSourceName)
}

// OpenLocalNodeDatabase returns a *sql.DB pointer.
// This is NOT a database connection
// see http://go-database-sql.org/accessing.html
func OpenLocalNodeDatabase() (*sql.DB, error) {
	nodeName := os.Getenv("NIMBUSIO_NODE_NAME")
	password := os.Getenv("NIMBUSIO_NODE_USER_PASSWORD")

	host := os.Getenv("NIMBUSIO_NODE_DATABASE_HOST")
	if host == "" {
		host = "localhost"
	}

	port := os.Getenv("NIMBUSIO_NODE_DATABASE_PORT")
	if port == "" {
		port = "5432"
	}

	return OpenNodeDatabase(nodeName, password, host, port)
}

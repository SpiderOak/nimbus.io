package routing

import (
	"net/http"
)

// RouterError represents a specific HTTP error
type RouterError interface {
	error
	HTTPCode() int
	ErrorMessage() string
}

// Router identifies the destination for an incoming request
type Router interface {

	// Route reads a request and decides where it should go <host:port>.
	// Will return RouterError when the caller should reply to the request
	// with a specific HTTP code
	Route(req *http.Request) (string, error)
}

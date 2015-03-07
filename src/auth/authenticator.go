package auth

import (
	"types"
)

func NewAuthenticator() {
	Authenticate(string, AccessType)(types.CollectionRow, error)
}

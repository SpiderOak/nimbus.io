package auth

import (
	"types"
)

type Authenticator interface {
	Authenticate(collectionName string, accessType AccessType) (
		types.CollectionRow, error)
}

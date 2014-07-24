package routing

import (
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"
	"os"

	"fog"
)

const (
	keySize = 32
)

var (
	destHashKey []byte
)

func init() {
	destHashKey = make([]byte, keySize)

	keyPath := os.Getenv("NIMBUSIO_URL_DEST_HASH_KEY")
	if keyPath != "" {
		file, err := os.Open(keyPath)
		if err != nil {
			fog.Critical("No valid file at NIMBUSIO_URL_DEST_HASH_KEY %s %s",
				keyPath, err)
		}
		defer file.Close()
		_, err = file.Read(destHashKey)
		if err != nil {
			fog.Critical("Error reading file at NIMBUSIO_URL_DEST_HASH_KEY %s %s",
				keyPath, err)
		}
	} else {
		_, err := rand.Read(destHashKey)
		if err != nil {
			fog.Critical("Error reading destHashKey rand.Read %s", err)
		}
	}
}

/*
consistentHashDest picks an available host in a semi-stable way based on
collection + path hashing, despite hosts becoming available an unavailable
dynamically.

Uses HMAC with key contained in the file pointed to by env
NIMBUSIO_URL_DEST_HASH_KEY or a random key if that file is unspecified.

Returns a host.
*/
func consistentHashDest(hostsForCollection, availableHosts []string,
	collectionName, path string) (string, error) {

	hmacGenerator := hmac.New(sha256.New, destHashKey)
	hmacGenerator.Write([]byte(collectionName))
	hmacGenerator.Write([]byte(path))

	/*
	   since our target host is not available, we want to pick a new one.
	   but we want to do this in a way that is somewhat stable.  we could
	   easily just make buckets for each of the available hosts, but then
	   the destination would change every time ANY host changes
	   availability.

	   iterate, modifying our hash, until we hit an available host.
	*/
	for i := 0; i < 25; i++ {
		hashValue := hmacGenerator.Sum(nil)
		hexValue := hex.EncodeToString(hashValue)
		x := new(big.Int)
		number, ok := x.SetString(hexValue, 16)
		if !ok {
			return "", fmt.Errorf("unable to convert '%s'", hexValue)
		}
		hostIndex := int(number.Uint64() % uint64(len(hostsForCollection)))
		destHost := hostsForCollection[hostIndex]

		for j := 0; j < len(availableHosts); j++ {
			if destHost == availableHosts[j] {
				return destHost, nil
			}
		}

		hmacGenerator.Write([]byte(destHost))
		hmacGenerator.Write([]byte(fmt.Sprintf("%d", i)))
	}

	// if we can't find a host after 25 iterations, we suck
	return "", fmt.Errorf("consistentHashDest unable to find a host %s %s",
		hostsForCollection, availableHosts)
}

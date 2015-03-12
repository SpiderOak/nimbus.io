package auth

import (
	"crypto/hmac"
	"crypto/sha256"
	"fmt"
	"regexp"
	"strconv"
	"types"

	"centraldb"
)

var (
	authTextRegexp = regexp.MustCompile(`^NIMBUS.IO (\d+):([0-9a-fA-F]+)$`)
)

// PasswordAuthentication verifies that the user is valid
// it returns nil if the user is  valid, or an err if not
func PasswordAuthentication(centralDB centraldb.CentralDB,
	customerID uint32,
	method string,
	timestamp int64,
	path string,
	authText string) error {

	var err error
	var customerRow types.CustomerRow
	var customerKeyRow types.CustomerKeyRow

	customerKeyID, signature, err := parseAuthText(authText)
	if err != nil {
		return err
	}

	customerKeyRow, err = centralDB.GetCustomerKeyRow(customerKeyID)
	if err != nil {
		return fmt.Errorf("unable to get customer key row: %d, %s",
			customerKeyID, err)
	}

	// IRC chat 09-29-2012 Alan points out a user could send us any key
	// so let's verify that this key belongs to the user
	if customerKeyRow.CustomerID != customerID {
		return fmt.Errorf("invalid customer key: customer id mismatch %d != %d",
			customerKeyRow.CustomerID, customerID)
	}

	customerRow, err = centralDB.GetCustomerRowByID(customerID)
	if err != nil {
		return fmt.Errorf("unable to get customer row: %d, %s",
			customerID, err)
	}

	stringToSign := fmt.Sprintf("%s\n%s\n%d\n%s",
		customerRow.UserName,
		method,
		timestamp,
		path)

	h := hmac.New(sha256.New, customerKeyRow.Key)
	h.Write([]byte(stringToSign))
	expectedSignature := fmt.Sprintf("%x", h.Sum(nil))

	if signature != expectedSignature {
		return fmt.Errorf("signature mismatch: %s != %s",
			signature, expectedSignature)
	}

	return nil
}

func parseAuthText(authText string) (uint32, string, error) {
	matches := authTextRegexp.FindStringSubmatch(authText)
	if matches == nil {
		return 0, "", fmt.Errorf("unmatched auth text %s", authText)
	}

	keyID, err := strconv.Atoi(matches[1])
	if err != nil {
		return 0, "", err
	}

	return uint32(keyID), matches[2], nil
}

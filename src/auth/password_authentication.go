package auth

import (
	"fmt"
	"types"

	"centraldb"
)

// PasswordAuthentication verifies that the user is valid
func PasswordIsValid(centralDB centraldb.CentralDB, customerID uint32) (bool, error) {
	var err error
	var customerRow types.CustomerRow

	customerRow, err = centralDB.GetCustomerRowByID(customerID)
	if err != nil {
		return false, fmt.Errorf("unable to get customer row: %d, %s",
			customerID, err)
	}
	fmt.Printf("customer row = %s", customerRow)

	return false, fmt.Errorf("not implemented")
}

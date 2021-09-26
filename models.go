package end2end

import (
	"fmt"
	"github.com/strongo/validation"
	"strings"
)

const (
	// E2ETestKind1 defines table or collection name for an entity to be stored in
	E2ETestKind1 = "E2ETest1"
)

// TestData describes a test entity to be stored in a DALgo database
type TestData struct {
	StringProp  string
	IntegerProp int
}

// Validate returns error if not valid
func (v TestData) Validate() error {
	if strings.TrimSpace(v.StringProp) == "" {
		return validation.NewErrRecordIsMissingRequiredField("StringProp")
	}
	if v.IntegerProp < 0 {
		return validation.NewErrBadRecordFieldValue("IntegerProp", fmt.Sprintf("should be > 0, got: %v", v.IntegerProp))
	}
	return nil
}

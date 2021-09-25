package end2end

import (
	"fmt"
	"github.com/strongo/validation"
	"strings"
)

const E2ETestKind = "E2ETest"

type TestData struct {
	StringProp  string
	IntegerProp int
}

func (v TestData) Validate() error {
	if strings.TrimSpace(v.StringProp) == "" {
		return validation.NewErrRecordIsMissingRequiredField("StringProp")
	}
	if v.IntegerProp < 0 {
		return validation.NewErrBadRecordFieldValue("IntegerProp", fmt.Sprintf("should be > 0, got: %v", v.IntegerProp))
	}
	return nil
}

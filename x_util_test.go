package birch

import (
	"errors"
	"testing"
)

func IsTooSmall(err error) bool { return errors.Is(err, errTooSmall) }

func requireErrEqual(t *testing.T, err1 error, err2 error) {
	t.Helper()
	if err1 != nil && err2 != nil {
		if err1.Error() != err2.Error() {
			t.Fatalf("errors not equal: %q, %q", err1.Error(), err2.Error())
		}
		return
	}
	if err1 == nil && err2 == nil {
		return
	}

	t.Fatalf("errors are not equal '%v' and '%v'", err1, err2)
}

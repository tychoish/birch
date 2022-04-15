package birch

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/require"
)

func IsTooSmall(err error) bool { return errors.Is(err, errTooSmall) }

func requireErrEqual(t *testing.T, err1 error, err2 error) {
	if err1 != nil && err2 != nil {
		require.Equal(t, err1.Error(), err2.Error())
		return
	}

	require.Equal(t, err1, err2)
}

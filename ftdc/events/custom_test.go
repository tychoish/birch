package events

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestRollupRoundTrip(t *testing.T) {
	data := MakeCustom(4)
	assert.NoError(t, data.Add("a", 1.2))
	assert.NoError(t, data.Add("f", 100))
	assert.NoError(t, data.Add("b", 45.0))
	assert.NoError(t, data.Add("d", []int64{45, 32}))
	assert.Error(t, data.Add("foo", Custom{}))
	assert.Len(t, data, 4)

	t.Run("NewBSON", func(t *testing.T) {
		payload, err := data.MarshalBSON()
		if err != nil {
			t.Fatal(err)
		}

		rt := Custom{}
		err = rt.UnmarshalBSON(payload)
		if err != nil {
			t.Fatal(err)
		}

		if len(rt) != 4 {
			t.Fatalf("lengths of %d and %d are not expected", len(rt), 4)
		}
		assert.Equal(t, "a", rt[0].Name)
		assert.Equal(t, "b", rt[1].Name)
		assert.Equal(t, "d", rt[2].Name)
		assert.Equal(t, "f", rt[3].Name)
		assert.Equal(t, 1.2, rt[0].Value)
		assert.Equal(t, 45.0, rt[1].Value)
		if 100 != rt[3].Value.(int32) {
			t.Fatalf("values are not equal %v and %v", 100, rt[3].Value)
		}
	})
}

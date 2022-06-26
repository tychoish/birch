package mongowire

import (
	"bytes"
	"context"
	"io"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/tychoish/birch"
)

func TestReadMessage(t *testing.T) {
	canceled, cancel := context.WithCancel(context.Background())
	cancel()

	smallMessage := createSmallMessage(t)
	smallMessageBytes := smallMessage.Serialize()
	largeMessage := createLargeMessage(t, 3*1024*1024)
	largeMessageBytes := largeMessage.Serialize()

	for _, test := range []struct {
		name            string
		ctx             context.Context
		reader          io.Reader
		expectedMessage Message
		hasErr          bool
	}{
		{
			name:   "EmptyReader",
			ctx:    context.TODO(),
			reader: bytes.NewReader([]byte{}),
			hasErr: true,
		},
		{
			name:   "NoHeader",
			ctx:    context.TODO(),
			reader: bytes.NewReader([]byte{'a', 'b', 'c'}),
			hasErr: true,
		},
		{
			name:   "CanceledContext",
			ctx:    canceled,
			reader: bytes.NewReader(smallMessageBytes),
			hasErr: true,
		},
		{
			name:   "MessageTooLarge",
			ctx:    context.TODO(),
			reader: bytes.NewReader(createLargeMessage(t, 200*1024*1024).Serialize()),
			hasErr: true,
		},
		{
			name:   "InvalidHeaderSize",
			ctx:    context.TODO(),
			reader: bytes.NewReader(int32ToBytes(-1)),
			hasErr: true,
		},
		{
			name:   "InvalidMessageHeader",
			ctx:    context.TODO(),
			reader: bytes.NewReader(append(int32ToBytes(20), bytes.Repeat([]byte{'a'}, 4)...)),
			hasErr: true,
		},
		{
			name:            "SmallMessage",
			ctx:             context.TODO(),
			reader:          bytes.NewReader(smallMessageBytes),
			expectedMessage: smallMessage,
		},
		{
			name:            "LargeMesage",
			ctx:             context.TODO(),
			reader:          bytes.NewReader(largeMessageBytes),
			expectedMessage: largeMessage,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			message, err := ReadMessage(test.ctx, test.reader)
			if test.hasErr {
				assert.Error(t, err)
				assert.Nil(t, message)
			} else {
				if err != nil {
					t.Error(err)
				}
				if test.expectedMessage.Header() != message.Header() {
					t.Error("values should be equal")
				}
				if test.expectedMessage.Serialize() != message.Serialize() {
					t.Error("values should be equal")
				}
			}
		})
	}
}

func TestSendMessage(t *testing.T) {
	t.Run("CanceledContext", func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		w := &mockWriter{}
		assert.Error(t, SendMessage(ctx, createSmallMessage(t), w))
		assert.Empty(t, w.data)
	})
	t.Run("SmallMessage", func(t *testing.T) {
		w := &mockWriter{}
		smallMessage := createSmallMessage(t)
		if err := SendMessage(context.TODO(), smallMessage, w); err != nil {
			t.Fatal(err)
		}
		if w.data != smallMessage.Serialize() {
			t.Error("values should be equal")
		}
	})
	t.Run("LargeMessage", func(t *testing.T) {
		w := &mockWriter{}
		largeMessage := createLargeMessage(t, 3*1024*1024)
		if err := SendMessage(context.TODO(), largeMessage, w); err != nil {
			t.Fatal(err)
		}
		if w.data != largeMessage.Serialize() {
			t.Error("values should be equal")
		}
	})
}

type mockWriter struct {
	data []byte
}

func (w *mockWriter) Write(p []byte) (int, error) {
	w.data = append(w.data, p...)
	return len(p), nil
}

func createSmallMessage(t *testing.T) Message {
	bytes, err := birch.DC.Elements(birch.EC.String("foo", "bar")).MarshalBSON()
	if err != nil {
		t.Fatal(err)
	}
	query, err := birch.ReadDocument(bytes)
	if err != nil {
		t.Fatal(err)
	}
	bytes, err = birch.DC.Elements(birch.EC.String("bar", "foo")).MarshalBSON()
	if err != nil {
		t.Fatal(err)
	}
	project, err := birch.ReadDocument(bytes)
	if err != nil {
		t.Fatal(err)
	}

	return NewQuery("ns", 0, 0, 1, query, project)
}

func createLargeMessage(t *testing.T, size int) Message {
	doc := birch.DC.Elements(birch.EC.Binary("foo", bytes.Repeat([]byte{'a'}, size)))
	return NewQuery("ns", 0, 0, 1, doc, nil)
}

func int32ToBytes(i int32) []byte {
	data := make([]byte, 4)
	writeInt32(i, data, 0)
	return data
}

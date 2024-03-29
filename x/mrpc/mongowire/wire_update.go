package mongowire

import (
	"bytes"
	"errors"

	"github.com/tychoish/birch"
)

func NewUpdate(ns string, flags int32, filter, update *birch.Document) Message {
	return &updateMessage{
		header: MessageHeader{
			RequestID: 19,
			OpCode:    OP_UPDATE,
		},
		Namespace: ns,
		Flags:     flags,
		Filter:    filter,
		Update:    update,
	}
}

func (m *updateMessage) HasResponse() bool     { return false }
func (m *updateMessage) Header() MessageHeader { return m.header }
func (m *updateMessage) Scope() *OpScope {
	return &OpScope{Type: m.header.OpCode, Context: m.Namespace}
}

func (m *updateMessage) Serialize() []byte {
	size := 16 /* header */ + 8 /* update header */
	size += len(m.Namespace) + 1
	size += getDocSize(m.Filter)
	size += getDocSize(m.Update)

	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	writeInt32(0, buf)

	writeCString(m.Namespace, buf)

	writeInt32(m.Flags, buf)

	m.Filter.WriteTo(buf)
	m.Update.WriteTo(buf)

	return buf.Bytes()
}

func (h *MessageHeader) parseUpdateMessage(buf []byte) (Message, error) {
	var (
		err error
		loc int
	)

	if len(buf) < 4 {
		return nil, errors.New("invalid update message -- message must have length of at least 4 bytes")
	}

	m := &updateMessage{
		header: *h,
	}

	m.Reserved = readInt32(buf[loc:])
	loc += 4

	m.Namespace, err = readCString(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += len(m.Namespace) + 1

	if len(buf) < (loc + 4) {
		return nil, errors.New("invalid update message -- message length is too short")
	}

	m.Flags = readInt32(buf[loc:])
	loc += 4

	m.Filter, err = birch.ReadDocument(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += getDocSize(m.Filter)

	if len(buf) < loc {
		return m, errors.New("invalid update message -- message length is too short")
	}

	m.Update, err = birch.ReadDocument(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += getDocSize(m.Update) // nolint

	return m, nil
}

package mongowire

import (
	"bytes"
	"errors"
)

func NewGetMore(ns string, number int32, cursorID int64) Message {
	return &getMoreMessage{
		header: MessageHeader{
			RequestID: 19,
			OpCode:    OP_GET_MORE,
		},
		Namespace: ns,
		NReturn:   number,
		CursorId:  cursorID,
	}
}

func (m *getMoreMessage) HasResponse() bool     { return true }
func (m *getMoreMessage) Header() MessageHeader { return m.header }

func (m *getMoreMessage) Scope() *OpScope {
	return &OpScope{
		Type:    m.header.OpCode,
		Context: m.Namespace,
	}
}

func (m *getMoreMessage) Serialize() []byte {
	size := 16 /* header */ + 16 /* query header */
	size += len(m.Namespace) + 1

	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	writeInt32(0, buf)

	writeCString(m.Namespace, buf)
	writeInt32(m.NReturn, buf)
	writeInt64(m.CursorId, buf)

	return buf.Bytes()
}

func (h *MessageHeader) parseGetMoreMessage(buf []byte) (Message, error) {
	var (
		err error
		loc int
	)

	if len(buf) < 4 {
		return nil, errors.New("invalid get more message -- message must have length of at least 4 bytes")
	}

	qm := &getMoreMessage{
		header: *h,
	}

	qm.Reserved = readInt32(buf)
	loc += 4

	qm.Namespace, err = readCString(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += len(qm.Namespace) + 1

	if len(buf) < loc+12 {
		return nil, errors.New("invalid get more message -- message length is too short")
	}
	qm.NReturn = readInt32(buf[loc:])
	loc += 4

	qm.CursorId = readInt64(buf[loc:])
	loc += 8 // nolint

	return qm, nil
}

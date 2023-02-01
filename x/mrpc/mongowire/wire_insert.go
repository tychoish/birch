package mongowire

import (
	"bytes"
	"errors"

	"github.com/tychoish/birch"
)

func NewInsert(ns string, docs ...*birch.Document) Message {
	msg := &insertMessage{
		header: MessageHeader{
			RequestID: 19,
			OpCode:    OP_INSERT,
		},
		Namespace: ns,
	}
	for _, d := range docs {
		msg.Docs = append(msg.Docs, *d.Copy())
	}
	return msg
}

func (m *insertMessage) HasResponse() bool     { return false }
func (m *insertMessage) Header() MessageHeader { return m.header }
func (m *insertMessage) Scope() *OpScope {
	return &OpScope{Type: m.header.OpCode, Context: m.Namespace}
}

func (m *insertMessage) Serialize() []byte {
	size := 16 /* header */ + 4 /* update header */
	size += len(m.Namespace) + 1
	for _, d := range m.Docs {
		size += getDocSize(&d)
	}

	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	bufWriteInt32(m.Flags, buf)

	writeCString(m.Namespace, buf)

	for _, d := range m.Docs {
		d.WriteTo(buf)
	}

	return buf.Bytes()
}

func (h *MessageHeader) parseInsertMessage(buf []byte) (Message, error) {
	m := &insertMessage{
		header: *h,
	}

	var err error
	loc := 0

	if len(buf) < 4 {
		return m, errors.New("invalid insert message -- message must have length of at least 4 bytes")
	}

	m.Flags = readInt32(buf[loc:])
	loc += 4

	m.Namespace, err = readCString(buf[loc:])
	if err != nil {
		return m, err
	}
	loc += len(m.Namespace) + 1

	for loc < len(buf) {
		doc, err := birch.ReadDocument(buf[loc:])
		if err != nil {
			return nil, err
		}
		m.Docs = append(m.Docs, *doc.Copy())
		loc += getDocSize(doc)
	}

	return m, nil
}

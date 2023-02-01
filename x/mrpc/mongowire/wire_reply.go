package mongowire

import (
	"bytes"
	"errors"

	"github.com/tychoish/birch"
)

func NewReply(cursorID int64, flags, startingFrom, numReturned int32, docs []birch.Document) Message {
	return &ReplyMessage{
		header: MessageHeader{
			RequestID: 19,
			OpCode:    OP_REPLY,
		},
		Flags:          flags,
		CursorId:       cursorID,
		StartingFrom:   startingFrom,
		NumberReturned: numReturned,
		Docs:           docs,
	}
}

// because its a response
func (m *ReplyMessage) HasResponse() bool     { return false }
func (m *ReplyMessage) Header() MessageHeader { return m.header }
func (m *ReplyMessage) Scope() *OpScope       { return nil }

func (m *ReplyMessage) Serialize() []byte {
	size := 16 /* header */ + 20 /* reply header */
	for _, d := range m.Docs {
		size += getDocSize(&d)
	}
	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	bufWriteInt32(m.Flags, buf)
	bufWriteInt64(m.CursorId, buf)
	bufWriteInt32(m.StartingFrom, buf)
	bufWriteInt32(m.NumberReturned, buf)

	for _, d := range m.Docs {
		d.WriteTo(buf)
	}

	return buf.Bytes()
}

func (h *MessageHeader) parseReplyMessage(buf []byte) (Message, error) {
	var loc int

	if len(buf) < 20 {
		return nil, errors.New("invalid reply message -- message must have length of at least 20 bytes")
	}

	rm := &ReplyMessage{
		header: *h,
	}

	rm.Flags = readInt32(buf[loc:])
	loc += 4

	rm.CursorId = readInt64(buf[loc:])
	loc += 8

	rm.StartingFrom = readInt32(buf[loc:])
	loc += 4

	rm.NumberReturned = readInt32(buf[loc:])
	loc += 4

	for loc < len(buf) {
		doc, err := birch.ReadDocument(buf[loc:])
		if err != nil {
			return nil, err
		}
		rm.Docs = append(rm.Docs, *doc.Copy())
		loc += getDocSize(doc)
	}

	return rm, nil
}

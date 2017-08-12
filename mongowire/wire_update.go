package mongowire

import "github.com/pkg/errors"

func NewUpdate(ns string, flags int32, filter, update SimpleBSON) Message {
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

func (m *updateMessage) Serialize() []byte {
	size := 16 /* header */ + 8 /* update header */
	size += len(m.Namespace) + 1
	size += int(m.Filter.Size)
	size += int(m.Update.Size)

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	loc := 16

	writeInt32(0, buf, loc)
	loc += 4

	writeCString(m.Namespace, buf, &loc)

	writeInt32(m.Flags, buf, loc)
	loc += 4

	m.Filter.Copy(&loc, buf)
	m.Update.Copy(&loc, buf)

	return buf
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

	m.Filter, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += int(m.Filter.Size)

	if len(buf) < loc {
		return m, errors.New("invalid update message -- message length is too short")
	}

	m.Update, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return nil, err
	}
	loc += int(m.Filter.Size) // nolint

	return m, nil
}

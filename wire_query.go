package mongonet

import "github.com/pkg/errors"

func NewQuery(ns string, flags, skip, toReturn int32, query, project SimpleBSON) Message {
	return &queryMessage{
		header: MessageHeader{
			RequestID: 19,
			OpCode:    OP_QUERY,
		},
		Flags:     flags,
		Namespace: ns,
		Skip:      skip,
		NReturn:   toReturn,
		Query:     query,
		Project:   project,
	}
}

func (m *queryMessage) HasResponse() bool     { return true }
func (m *queryMessage) Header() MessageHeader { return m.header }

func (m *queryMessage) Serialize() []byte {
	size := 16 /* header */ + 12 /* query header */
	size += len(m.Namespace) + 1
	size += int(m.Query.Size)
	size += int(m.Project.Size)

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	writeInt32(m.Flags, buf, 16)

	loc := 20
	writeCString(m.Namespace, buf, &loc)
	writeInt32(m.Skip, buf, loc)
	loc += 4

	writeInt32(m.NReturn, buf, loc)
	loc += 4

	m.Query.Copy(&loc, buf)
	m.Project.Copy(&loc, buf)

	return buf
}

func (h *MessageHeader) parseQueryMessage(buf []byte) (Message, error) {
	if len(buf) < 4 {
		return nil, errors.New("invalid query message -- message must have length of at least 4 bytes")
	}

	loc := 0
	qm := &queryMessage{
		header: *h,
	}

	qm.Flags = readInt32(buf)
	loc += 4

	tmp, err := readCString(buf[loc:])
	qm.Namespace = tmp
	if err != nil {
		return nil, errors.WithStack(err)
	}
	loc += len(qm.Namespace) + 1

	if len(buf) < loc+8 {
		return qm, errors.New("invalid query message -- message length is too short")
	}
	qm.Skip = readInt32(buf[loc:])
	loc += 4

	qm.NReturn = readInt32(buf[loc:])
	loc += 4

	qm.Query, err = parseSimpleBSON(buf[loc:])
	if err != nil {
		return nil, errors.WithStack(err)
	}
	loc += int(qm.Query.Size)

	if loc < len(buf) {
		qm.Project, err = parseSimpleBSON(buf[loc:])
		if err != nil {
			return nil, errors.WithStack(err)
		}
		loc += int(qm.Project.Size) // nolint
	}

	return qm, nil
}

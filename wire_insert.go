package mongonet

func (m *InsertMessage) HasResponse() bool {
	return false
}

func (m *InsertMessage) Header() MessageHeader {
	return m.header
}

func (m *InsertMessage) Serialize() []byte {
	size := 16 /* header */ + 4 /* update header */
	size += len(m.Namespace) + 1
	for _, d := range m.Docs {
		size += int(d.Size)
	}

	m.header.Size = int32(size)

	buf := make([]byte, size)
	m.header.WriteInto(buf)

	loc := 16

	writeInt32(m.Flags, buf, loc)
	loc += 4

	writeCString(m.Namespace, buf, &loc)

	for _, d := range m.Docs {
		d.Copy(&loc, buf)
	}

	return buf
}

func parseInsertMessage(header MessageHeader, buf []byte) (Message, error) {
	m := &InsertMessage{}
	m.header = header

	var err error
	loc := 0

	m.Flags = readInt32(buf[loc:])
	loc += 4

	m.Namespace, err = readCString(buf[loc:])
	if err != nil {
		return m, err
	}
	loc += len(m.Namespace) + 1

	for loc < len(buf) {
		doc, err := parseSimpleBSON(buf[loc:])
		if err != nil {
			return nil, err
		}
		m.Docs = append(m.Docs, doc)
		loc += int(doc.Size)
	}

	return m, nil
}

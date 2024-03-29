package mongowire

import (
	"bytes"
	"errors"

	"github.com/tychoish/birch"
)

func NewCommand(db, name string, args, metadata *birch.Document, inputs []birch.Document) Message {
	return &CommandMessage{
		header: MessageHeader{
			OpCode:    OP_COMMAND,
			RequestID: 19,
		},
		DB:          db,
		CmdName:     name,
		CommandArgs: args,
		Metadata:    metadata,
		InputDocs:   inputs,
	}
}

func (m *CommandMessage) HasResponse() bool     { return true }
func (m *CommandMessage) Header() MessageHeader { return m.header }

func (m *CommandMessage) Scope() *OpScope {
	return &OpScope{
		Type:    m.header.OpCode,
		Context: m.DB,
		Command: m.CmdName,
	}
}

func (m *CommandMessage) Serialize() []byte {
	size := 16 /* header */
	size += len(m.DB) + 1
	size += len(m.CmdName) + 1
	size += getDocSize(m.CommandArgs)
	size += getDocSize(m.Metadata)
	for _, d := range m.InputDocs {
		size += getDocSize(&d)
	}
	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	writeCString(m.DB, buf)
	writeCString(m.CmdName, buf)

	m.CommandArgs.WriteTo(buf)
	m.Metadata.WriteTo(buf)

	for _, d := range m.InputDocs {
		d.WriteTo(buf)
	}

	return buf.Bytes()
}

func (h *MessageHeader) parseCommandMessage(buf []byte) (Message, error) {
	var err error

	cmd := &CommandMessage{
		header: *h,
	}

	cmd.DB, err = readCString(buf)
	if err != nil {
		return cmd, err
	}

	if len(buf) < len(cmd.DB)+1 {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[len(cmd.DB)+1:]

	cmd.CmdName, err = readCString(buf)
	if err != nil {
		return nil, err
	}
	if len(buf) < len(cmd.CmdName)+1 {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[len(cmd.CmdName)+1:]

	cmd.CommandArgs, err = birch.ReadDocument(buf)
	if err != nil {
		return nil, err
	}

	size, err := cmd.CommandArgs.Validate()
	if err != nil {
		return nil, err
	}

	if len(buf) < int(size) {
		return cmd, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[size:]

	cmd.Metadata, err = birch.ReadDocument(buf)
	if err != nil {
		return nil, err
	}

	size, err = cmd.Metadata.Validate()
	if err != nil {
		return nil, err
	}

	if len(buf) < int(size) {
		return cmd, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[size:]

	for len(buf) > 0 {
		doc, err := birch.ReadDocument(buf)
		if err != nil {
			return nil, err
		}
		size, err = doc.Validate()
		if err != nil {
			return nil, err
		}

		buf = buf[size:]
		cmd.InputDocs = append(cmd.InputDocs, *doc.Copy())
	}

	return cmd, nil
}

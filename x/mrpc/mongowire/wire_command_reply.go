package mongowire

import (
	"bytes"
	"errors"

	"github.com/tychoish/birch"
)

func NewCommandReply(reply, metadata *birch.Document, output []birch.Document) Message {
	return &CommandReplyMessage{
		header: MessageHeader{
			OpCode:    OP_COMMAND_REPLY,
			RequestID: 19,
		},
		CommandReply: reply,
		Metadata:     metadata,
		OutputDocs:   output,
	}
}

func (m *CommandReplyMessage) HasResponse() bool     { return false }
func (m *CommandReplyMessage) Header() MessageHeader { return m.header }
func (m *CommandReplyMessage) Scope() *OpScope       { return nil }

func (m *CommandReplyMessage) Serialize() []byte {
	size := 16 /* header */

	size += getDocSize(m.CommandReply)
	size += getDocSize(m.Metadata)
	for _, d := range m.OutputDocs {
		size += getDocSize(&d)
	}
	m.header.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)

	m.CommandReply.WriteTo(buf)
	m.Metadata.WriteTo(buf)

	for _, d := range m.OutputDocs {
		d.WriteTo(buf)
	}

	return buf.Bytes()
}

func (h *MessageHeader) parseCommandReplyMessage(buf []byte) (Message, error) {
	rm := &CommandReplyMessage{
		header: *h,
	}

	var err error

	rm.CommandReply, err = birch.ReadDocument(buf)
	if err != nil {
		return nil, err
	}

	replySize := getDocSize(rm.CommandReply)
	if len(buf) < replySize {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[replySize:]

	rm.Metadata, err = birch.ReadDocument(buf)
	if err != nil {
		return nil, err
	}
	metaSize := getDocSize(rm.Metadata)
	if len(buf) < metaSize {
		return nil, errors.New("invalid command message -- message length is too short")
	}
	buf = buf[metaSize:]

	for len(buf) > 0 {
		doc, err := birch.ReadDocument(buf)
		if err != nil {
			return nil, err
		}
		buf = buf[getDocSize(doc):]
		rm.OutputDocs = append(rm.OutputDocs, *doc.Copy())
	}

	return rm, nil
}

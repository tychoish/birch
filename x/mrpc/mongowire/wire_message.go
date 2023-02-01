package mongowire

import (
	"bytes"
	"errors"
	"fmt"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/mrpc/model"
)

type OpMessageSection interface {
	Type() uint8
	Name() string
	DB() string
	Documents() []birch.Document
	Serialize() []byte
}

const (
	OpMessageSectionBody             = 0
	OpMessageSectionDocumentSequence = 1
)

type opMessagePayloadType0 struct {
	Document *birch.Document
}

func (p *opMessagePayloadType0) Type() uint8 { return OpMessageSectionBody }

func (p *opMessagePayloadType0) Name() string {
	return p.Document.ElementAt(0).Key()
}

func (p *opMessagePayloadType0) DB() string {
	key, err := p.Document.LookupErr("$db")
	if err != nil {
		return ""
	}

	val, ok := key.StringValueOK()
	if !ok {
		return ""
	}

	return val
}

func (p *opMessagePayloadType0) Documents() []birch.Document {
	return []birch.Document{*p.Document.Copy()}
}

func (p *opMessagePayloadType0) Serialize() []byte {
	buf := bytes.NewBuffer(make([]byte, 0, 1+getDocSize(p.Document)))
	buf.Write([]byte{p.Type()})
	p.Document.WriteTo(buf)
	return buf.Bytes()
}

type opMessagePayloadType1 struct {
	Size       int32
	Identifier string
	Payload    []birch.Document
}

func (p *opMessagePayloadType1) Type() uint8 { return OpMessageSectionDocumentSequence }

func (p *opMessagePayloadType1) Name() string { return p.Identifier }

func (p *opMessagePayloadType1) DB() string { return "" }

func (p *opMessagePayloadType1) Documents() []birch.Document { return p.Payload }

func (p *opMessagePayloadType1) Serialize() []byte {
	size := 1                     // kind
	size += 4                     // size
	size += len(p.Identifier) + 1 // identifier
	for _, doc := range p.Payload {
		size += getDocSize(&doc)
	}
	p.Size = int32(size)

	buf := bytes.NewBuffer(make([]byte, 0, size))
	buf.Write([]byte{p.Type()})
	bufWriteInt32(p.Size, buf)
	writeCString(p.Identifier, buf)
	for _, doc := range p.Payload { // payload
		doc.WriteTo(buf)
	}
	return buf.Bytes()
}

func (m *OpMessage) Header() MessageHeader { return m.header }
func (m *OpMessage) HasResponse() bool     { return m.Flags > 1 }

func (m *OpMessage) Scope() *OpScope {
	var cmd string
	var db string
	// OP_MSG is expected to have exactly one body section.
	for _, section := range m.Items {
		if _, ok := section.(*opMessagePayloadType0); ok {
			cmd = section.Name()
			db = section.DB()
			break
		}
	}
	return &OpScope{
		Type:    m.header.OpCode,
		Context: db,
		Command: cmd,
	}
}

func (m *OpMessage) Serialize() []byte {
	if len(m.serialized) > 0 {
		return m.serialized
	}

	size := 16 // header
	size += 4  // flags
	sections := []byte{}
	for _, section := range m.Items {
		sections = append(sections, section.Serialize()...)
		switch p := section.(type) {
		case *opMessagePayloadType0:
			size += 1 // kind // nolint
			size += getDocSize(p.Document)
		case *opMessagePayloadType1:
			size += int(p.Size)
		}
	}
	if m.Checksum != 0 && (m.Flags&1) == 1 {
		size += 4
	}
	m.header.Size = int32(size)
	buf := bytes.NewBuffer(make([]byte, 0, size))
	m.header.WriteTo(buf)
	bufWriteInt32(int32(m.Flags), buf)

	buf.Write(sections)

	if m.Checksum != 0 && (m.Flags&1) == 1 {
		bufWriteInt32(m.Checksum, buf)
	}

	m.serialized = buf.Bytes()
	return m.serialized
}

func NewOpMessage(moreToCome bool, documents []birch.Document, items ...model.SequenceItem) Message {
	msg := &OpMessage{
		header: MessageHeader{
			OpCode:    OP_MSG,
			RequestID: 19,
		},
		Items: make([]OpMessageSection, len(documents)),
	}

	for idx := range documents {
		msg.Items[idx] = &opMessagePayloadType0{
			Document: documents[idx].Copy(),
		}
	}

	if moreToCome {
		msg.Flags = msg.Flags & 1
	}

	for idx := range items {
		item := items[idx]
		it := &opMessagePayloadType1{
			Identifier: item.Identifier,
		}
		for jdx := range item.Documents {
			it.Payload = append(it.Payload, *item.Documents[jdx].Copy())
			it.Size += int32(getDocSize(&item.Documents[jdx]))
		}
		msg.Items = append(msg.Items, it)
	}

	return msg
}

func (h *MessageHeader) parseMsgBody(body []byte) (Message, error) {
	if len(body) < 4 {
		return nil, errors.New("invalid op message - message must have length of at least 4 bytes")
	}

	msg := &OpMessage{
		header: *h,
	}

	loc := 0
	msg.Flags = uint32(readInt32(body[loc:]))
	loc += 4
	checksumPresent := (msg.Flags & 1) == 1

	for loc < len(body)-4 {
		kind := int32(body[loc])
		loc++

		var err error
		switch kind {
		case OpMessageSectionBody:
			section := &opMessagePayloadType0{}
			docSize := int(readInt32(body[loc:]))
			section.Document, err = birch.ReadDocument(body[loc : loc+docSize])
			loc += getDocSize(section.Document)
			msg.Items = append(msg.Items, section)
		case OpMessageSectionDocumentSequence:
			section := &opMessagePayloadType1{}
			section.Size = readInt32(body[loc:])
			loc += 4
			section.Identifier, err = readCString(body[loc:])
			if err != nil {
				return nil, fmt.Errorf("could not read identifier: %w", err)
			}
			loc += len(section.Identifier) + 1 // c string null terminator

			for remaining := int(section.Size) - 1 - 4 - len(section.Identifier) - 1; remaining > 0; {
				docSize := int(readInt32(body[loc:]))
				doc, err := birch.ReadDocument(body[loc : loc+docSize])
				if err != nil {
					return nil, fmt.Errorf("could not read payload document: %w", err)
				}
				section.Payload = append(section.Payload, *doc.Copy())
				remaining -= docSize
				loc += docSize
			}
			msg.Items = append(msg.Items, section)
		default:
			return nil, fmt.Errorf("unrecognized kind bit %d", kind)
		}
	}

	if checksumPresent && loc == len(body)-4 {
		msg.Checksum = readInt32(body[loc:])
		loc += 4
	}

	msg.header.Size = int32(loc)

	return msg, nil
}

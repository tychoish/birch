package mongowire

import "github.com/pkg/errors"

type OpType int32

const (
	OP_REPLY         OpType = 1
	OP_MSG                  = 1000
	OP_UPDATE               = 2001
	OP_INSERT               = 2002
	RESERVED                = 2003
	OP_QUERY                = 2004
	OP_GET_MORE             = 2005
	OP_DELETE               = 2006
	OP_KILL_CURSORS         = 2007
	OP_COMMAND              = 2010
	OP_COMMAND_REPLY        = 2011
)

type MessageHeader struct {
	Size       int32 // total message size
	RequestID  int32
	ResponseTo int32
	OpCode     OpType
}

func (h *MessageHeader) WriteInto(buf []byte) {
	writeInt32(h.Size, buf, 0)
	writeInt32(h.RequestID, buf, 4)
	writeInt32(h.ResponseTo, buf, 8)
	writeInt32(int32(h.OpCode), buf, 12)
}

func (h *MessageHeader) Parse(body []byte) (Message, error) {
	var (
		m   Message
		err error
	)

	switch h.OpCode {
	case OP_REPLY:
		m, err = h.parseReplyMessage(body)
	case OP_UPDATE:
		m, err = h.parseUpdateMessage(body)
	case OP_INSERT:
		m, err = h.parseInsertMessage(body)
	case OP_QUERY:
		m, err = h.parseQueryMessage(body)
	case OP_GET_MORE:
		m, err = h.parseGetMoreMessage(body)
	case OP_DELETE:
		m, err = h.parseDeleteMessage(body)
	case OP_KILL_CURSORS:
		m, err = h.parseKillCursorsMessage(body)
	case OP_COMMAND:
		m, err = h.parseCommandMessage(body)
	case OP_COMMAND_REPLY:
		m, err = h.parseCommandReplyMessage(body)
	default:
		return nil, errors.Errorf("unknown op code: %s", h.OpCode)
	}

	return m, errors.WithStack(err)
}

// ------------

type Message interface {
	Header() MessageHeader
	Serialize() []byte
	HasResponse() bool
}

// OP_REPLY
type replyMessage struct {
	header MessageHeader

	Flags          int32
	CursorId       int64
	StartingFrom   int32
	NumberReturned int32

	Docs []SimpleBSON
}

// OP_UPDATE
type updateMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter SimpleBSON
	Update SimpleBSON
}

// OP_QUERY
type queryMessage struct {
	header MessageHeader

	Flags     int32
	Namespace string
	Skip      int32
	NReturn   int32

	Query   SimpleBSON
	Project SimpleBSON
}

// OP_GET_MORE
type getMoreMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	NReturn   int32
	CursorId  int64
}

// OP_INSERT
type insertMessage struct {
	header MessageHeader

	Flags     int32
	Namespace string

	Docs []SimpleBSON
}

// OP_DELETE
type deleteMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter SimpleBSON
}

// OP_KILL_CURSORS
type killCursorsMessage struct {
	header MessageHeader

	Reserved   int32
	NumCursors int32
	CursorIds  []int64
}

// OP_COMMAND
type commandMessage struct {
	header MessageHeader

	DB          string
	CmdName     string
	CommandArgs SimpleBSON
	Metadata    SimpleBSON
	InputDocs   []SimpleBSON
}

// OP_COMMAND_REPLY
type commandReplyMessage struct {
	header MessageHeader

	CommandReply SimpleBSON
	Metadata     SimpleBSON
	OutputDocs   []SimpleBSON
}

package mongowire

import (
	"fmt"
	"io"
)

type MessageHeader struct {
	Size       int32 // total message size
	RequestID  int32
	ResponseTo int32
	OpCode     OpType
}

func (h *MessageHeader) WriteTo(wr io.Writer) {
	bufWriteInt32(h.Size, wr)
	bufWriteInt32(h.RequestID, wr)
	bufWriteInt32(h.ResponseTo, wr)
	bufWriteInt32(int32(h.OpCode), wr)
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
	case OP_MSG:
		m, err = h.parseMsgBody(body)
	default:
		return nil, fmt.Errorf("unknown op code: %s", h.OpCode)
	}

	return m, err
}

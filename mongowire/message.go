package mongowire

import (
	"github.com/tychoish/mongorpc/bson"
	"github.com/tychoish/mongorpc/model"
)

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

	Docs []bson.Simple
}

// OP_UPDATE
type updateMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter bson.Simple
	Update bson.Simple
}

// OP_QUERY
type queryMessage struct {
	header MessageHeader

	Flags     int32
	Namespace string
	Skip      int32
	NReturn   int32

	Query   bson.Simple
	Project bson.Simple
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

	Docs []bson.Simple
}

// OP_DELETE
type deleteMessage struct {
	header MessageHeader

	Reserved  int32
	Namespace string
	Flags     int32

	Filter bson.Simple
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
	CommandArgs bson.Simple
	Metadata    bson.Simple
	InputDocs   []bson.Simple
}

// OP_COMMAND_REPLY
type commandReplyMessage struct {
	header MessageHeader

	CommandReply bson.Simple
	Metadata     bson.Simple
	OutputDocs   []bson.Simple
}

func GetModel(m Message) (interface{}, bool) {
	switch m := m.(type) {
	case *commandMessage:
		return &model.Command{
			DB:        m.DB,
			Command:   m.CmdName,
			Arguments: m.CommandArgs,
			Metadata:  m.Metadata,
			Inputs:    m.InputDocs,
		}, true
	case *deleteMessage:
		return &model.Delete{
			Namespace: m.Namespace,
			Filter:    m.Filter,
		}, true
	case *insertMessage:
		return &model.Insert{
			Namespace: m.Namespace,
			Documents: m.Docs,
		}, true
	case *queryMessage:
		return &model.Query{
			Namespace: m.Namespace,
			Skip:      m.Skip,
			NReturn:   m.NReturn,
			Query:     m.Query,
			Project:   m.Project,
		}, true
	case *updateMessage:
		update := &model.Update{
			Namespace: m.Namespace,
			Filter:    m.Filter,
			Update:    m.Update,
		}

		switch m.Flags {
		case 1:
			update.Upsert = true
		case 2:
			update.Multi = true
		case 3:
			update.Upsert = true
			update.Multi = true
		}

		return update, true
	case *replyMessage:
		reply := &model.Reply{
			StartingFrom: m.StartingFrom,
			CursorID:     m.CursorId,
			Contents:     m.Docs,
		}

		switch m.Flags {
		case 1:
			reply.QueryFailure = true
		case 0:
			reply.CursorNotFound = true
		}

		return reply, true
	default:
		return nil, false
	}
}

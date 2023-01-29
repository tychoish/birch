package mongowire

import (
	"bytes"
	"testing"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/x/mrpc/model"
)

func TestMessage(t *testing.T) {
	bz, err := birch.DC.Elements(birch.EC.String("foo", "bar")).MarshalBSON()
	if err != nil {
		t.Fatal(err)
	}
	query, err := birch.ReadDocument(bz)
	if err != nil {
		t.Fatal(err)
	}
	bz, err = birch.DC.Elements(birch.EC.String("bar", "foo")).MarshalBSON()
	if err != nil {
		t.Fatal(err)
	}
	project, err := birch.ReadDocument(bz)
	if err != nil {
		t.Fatal(err)
	}

	headerSize := 16
	for _, test := range []struct {
		name        string
		message     Message
		header      MessageHeader
		hasResponse bool
		scope       *OpScope
		bodySize    int
	}{
		{
			name:     OP_REPLY.String(),
			message:  NewReply(1, 0, 0, 1, []birch.Document{*query, *project}),
			header:   MessageHeader{RequestID: 19, OpCode: OP_REPLY},
			scope:    nil,
			bodySize: 20 + getDocSize(query) + getDocSize(project),
		},
		{
			name: OP_MSG.String(),
			message: NewOpMessage(
				false,
				[]birch.Document{*query},
				model.SequenceItem{Identifier: "foo", Documents: []birch.Document{*project, *query}},
				model.SequenceItem{Identifier: "bar", Documents: []birch.Document{*query}},
			),
			header:   MessageHeader{RequestID: 19, OpCode: OP_MSG},
			scope:    &OpScope{Type: OP_MSG, Command: "foo"},
			bodySize: 4 + (1 + getDocSize(query)) + (1 + 4 + 3 + 1 + getDocSize(project) + getDocSize(query)) + (1 + 4 + 3 + 1 + getDocSize(query)),
		},
		{
			name:     OP_UPDATE.String(),
			message:  NewUpdate("ns", 0, query, project),
			header:   MessageHeader{RequestID: 19, OpCode: OP_UPDATE},
			scope:    &OpScope{Type: OP_UPDATE, Context: "ns"},
			bodySize: 8 + 3 + getDocSize(query) + getDocSize(project),
		},
		{
			name:     OP_INSERT.String(),
			message:  NewInsert("ns", query, project),
			header:   MessageHeader{RequestID: 19, OpCode: OP_INSERT},
			scope:    &OpScope{Type: OP_INSERT, Context: "ns"},
			bodySize: 4 + 3 + getDocSize(query) + getDocSize(project),
		},
		{
			name:        OP_GET_MORE.String(),
			message:     NewGetMore("ns", 5, 98),
			header:      MessageHeader{RequestID: 19, OpCode: OP_GET_MORE},
			hasResponse: true,
			scope:       &OpScope{Type: OP_GET_MORE, Context: "ns"},
			bodySize:    16 + 3,
		},
		{
			name:     OP_DELETE.String(),
			message:  NewDelete("ns", 0, query),
			header:   MessageHeader{RequestID: 19, OpCode: OP_DELETE},
			scope:    &OpScope{Type: OP_DELETE, Context: "ns"},
			bodySize: 8 + 3 + getDocSize(query),
		},
		{
			name:     OP_KILL_CURSORS.String(),
			message:  NewKillCursors(1, 2, 3),
			header:   MessageHeader{RequestID: 19, OpCode: OP_KILL_CURSORS},
			scope:    &OpScope{Type: OP_KILL_CURSORS},
			bodySize: 8 + 8*3,
		},
		{
			name:        OP_COMMAND.String(),
			message:     NewCommand("db", "cmd", query, project, []birch.Document{*query, *project}),
			header:      MessageHeader{RequestID: 19, OpCode: OP_COMMAND},
			hasResponse: true,
			scope:       &OpScope{Type: OP_COMMAND, Context: "db", Command: "cmd"},
			bodySize:    3 + 4 + 2*getDocSize(query) + 2*getDocSize(project),
		},
		{
			name:     OP_COMMAND_REPLY.String(),
			message:  NewCommandReply(query, project, []birch.Document{*query, *project}),
			header:   MessageHeader{RequestID: 19, OpCode: OP_COMMAND_REPLY},
			bodySize: 2*getDocSize(query) + 2*getDocSize(project),
		},
		{
			name:        OP_QUERY.String(),
			message:     NewQuery("ns", 0, 0, 1, query, project),
			header:      MessageHeader{RequestID: 19, OpCode: OP_QUERY},
			hasResponse: true,
			scope:       &OpScope{Type: OP_QUERY, Context: "ns"},
			bodySize:    12 + 3 + getDocSize(query) + getDocSize(project),
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if test.header != test.message.Header() {
				t.Error("values should be equal")
			}
			if test.hasResponse != test.message.HasResponse() {
				t.Error("values should be equal")
			}
			if test.scope != nil {
				if test.scope.Type != test.message.Scope().Type {
					t.Error("values should be equal")
				}
				if test.scope.Context != test.message.Scope().Context {
					t.Error("values should be equal")
				}
			}
			if headerSize+test.bodySize != len(test.message.Serialize()) {
				t.Error("values should be equal")
			}
			if int32(headerSize+test.bodySize) != test.message.Header().Size {
				t.Error("values should be equal")
			}
			m, err := test.header.Parse(test.message.Serialize()[headerSize:])
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(test.message.Serialize(), m.Serialize()) {
				t.Error("values should be equal")
			}
			if test.message.Header() != m.Header() {
				t.Error("values should be equal")
			}
		})
	}
}

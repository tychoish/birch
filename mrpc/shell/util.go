package shell

import (
	"context"
	"io"

	"github.com/tychoish/birch"
	"github.com/tychoish/birch/mrpc/mongowire"
	"github.com/tychoish/grip"
	"github.com/tychoish/grip/message"
)

// WriteResponse sends a response the the writer output.
func WriteResponse(ctx context.Context, w io.Writer, resp mongowire.Message, op string) {
	grip.Error(message.WrapError(mongowire.SendMessage(ctx, resp, w), message.Fields{
		"message": "could not write response",
		"op":      op,
	}))
}

// WriteErrorResponse writes a response indicating an error occurred to the
// writer output.
func WriteErrorResponse(ctx context.Context, w io.Writer, t mongowire.OpType, err error, op string) {
	doc, _ := MakeErrorResponse(false, err).MarshalDocument()
	resp, err := ResponseToMessage(t, doc)
	if err != nil {
		grip.Error(message.WrapError(err, message.Fields{
			"message": "could not write response",
			"op":      op,
		}))
		return
	}
	WriteResponse(ctx, w, resp, op)
}

// WriteOKResponse writes a response indicating that the request was ok.
func WriteOKResponse(ctx context.Context, w io.Writer, t mongowire.OpType, op string) {
	doc, _ := MakeErrorResponse(true, nil).MarshalDocument()
	resp, err := ResponseToMessage(t, doc)
	if err != nil {
		grip.Error(message.WrapError(err, message.Fields{
			"message": "could not write response",
			"op":      op,
		}))
		return
	}
	WriteResponse(ctx, w, resp, op)
}

// WriteOKResponse writes a response indicating that the request was not ok.
func WriteNotOKResponse(ctx context.Context, w io.Writer, t mongowire.OpType, op string) {
	doc, _ := MakeErrorResponse(false, nil).MarshalDocument()
	resp, err := ResponseToMessage(t, doc)
	if err != nil {
		grip.Error(message.WrapError(err, message.Fields{
			"message": "could not write response",
			"op":      op,
		}))
		return
	}
	WriteResponse(ctx, w, resp, op)

}

// ResponseToMessage converts a response into a wire protocol reply.
func ResponseToMessage(t mongowire.OpType, doc *birch.Document) (mongowire.Message, error) {
	if t == mongowire.OP_MSG {
		return mongowire.NewOpMessage(false, []birch.Document{*doc}), nil
	}
	return mongowire.NewReply(0, 0, 0, 1, []birch.Document{*doc}), nil
}

// RequestToMessage converts a request into a wire protocol query.
func RequestToMessage(t mongowire.OpType, doc *birch.Document) (mongowire.Message, error) {
	if t == mongowire.OP_MSG {
		return mongowire.NewOpMessage(false, []birch.Document{*doc}), nil
	}

	// <namespace.$cmd  format is required to indicate that the OP_QUERY should
	// be interpreted as an OP_COMMAND.
	const namespace = "mrpc.$cmd"
	return mongowire.NewQuery(namespace, 0, 0, 1, doc, birch.NewDocument()), nil
}

// RequestMessageToDocument converts a wire protocol request message into a
// document.
func RequestMessageToDocument(msg mongowire.Message) (*birch.Document, error) {
	opMsg, ok := msg.(*mongowire.OpMessage)
	if ok {
		for _, section := range opMsg.Items {
			if section.Type() == mongowire.OpMessageSectionBody && len(section.Documents()) != 0 {
				return section.Documents()[0].Copy(), nil
			}
		}
		return nil, fmt.Errorsf("%s message did not contain body", msg.Header().OpCode)
	}
	opCmdMsg, ok := msg.(*mongowire.CommandMessage)
	if !ok {
		return nil, fmt.Errorsf("message is not of type %s", mongowire.OP_COMMAND.String())
	}
	return opCmdMsg.CommandArgs, nil
}

// ResponseMessageToDocument converts a wire protocol response message into a
// document.
func ResponseMessageToDocument(msg mongowire.Message) (*birch.Document, error) {
	if opReplyMsg, ok := msg.(*mongowire.ReplyMessage); ok {
		return &opReplyMsg.Docs[0], nil
	}
	if opCmdReplyMsg, ok := msg.(*mongowire.CommandReplyMessage); ok {
		return opCmdReplyMsg.CommandReply, nil
	}
	if opMsg, ok := msg.(*mongowire.OpMessage); ok {
		for _, section := range opMsg.Items {
			if section.Type() == mongowire.OpMessageSectionBody && len(section.Documents()) != 0 {
				return section.Documents()[0].Copy(), nil
			}
		}
		return nil, fmt.Errorsf("%s response did not contain body", mongowire.OP_MSG.String())
	}
	return nil, fmt.Errorsf("message is not of type %s, %s, nor %s", mongowire.OP_COMMAND_REPLY.String(), mongowire.OP_REPLY.String(), mongowire.OP_MSG.String())
}

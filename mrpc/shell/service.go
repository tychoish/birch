package shell

import (
	"context"
	"fmt"
	"io"

	"errors"

	"github.com/tychoish/birch/mrpc"
	"github.com/tychoish/birch/mrpc/mongowire"
)

type shellService struct {
	mrpc.Service
}

// NewShellService returns a service for mongo shell clients listening on the
// given host and port.
func NewShellService(host string, port int) (mrpc.Service, error) {
	s := &shellService{Service: mrpc.NewBasicService(host, port)}
	if err := s.registerHandlers(); err != nil {
		return nil, fmt.Errorf("could not register handlers: %w", err)
	}
	return s, nil
}

// MakeShellService takes an existing mrpc.Service and adds support for mongo
// shell clients.
func MakeShellService(service mrpc.Service) (mrpc.Service, error) {
	s := &shellService{Service: service}
	if err := s.registerHandlers(); err != nil {
		return nil, fmt.Errorf("could not register handlers: %w", err)
	}
	return s, nil
}

// Constants representing required shell commands.
const (
	isMasterCommand   = "isMaster"
	whatsMyURICommand = "whatsmyuri"
	// The shell sends commands with different casing so we need two different
	// handlers for the different "buildinfo" commands
	buildInfoCommand               = "buildInfo"
	BuildinfoCommand               = "buildinfo"
	endSessionsCommand             = "endSessions"
	getCmdLineOptsCommand          = "getCmdLineOpts"
	getLogCommand                  = "getLog"
	getFreeMonitoringStatusCommand = "getFreeMonitoringStatus"
	replSetGetStatusCommand        = "replSetGetStatus"
	listCollectionsCommand         = "listCollections"
)

func (s *shellService) registerHandlers() error {
	for name, handler := range map[string]mrpc.HandlerFunc{
		// Required initialization commands
		isMasterCommand:                s.isMaster,
		whatsMyURICommand:              s.whatsMyURI,
		BuildinfoCommand:               s.buildInfo,
		buildInfoCommand:               s.buildInfo,
		endSessionsCommand:             s.endSessions,
		getLogCommand:                  s.getLog,
		replSetGetStatusCommand:        s.replSetGetStatus,
		getFreeMonitoringStatusCommand: s.getFreeMonitoringStatus,
		listCollectionsCommand:         s.listCollections,
		getCmdLineOptsCommand:          s.getCmdLineOpts,
	} {
		for _, opType := range []mongowire.OpType{mongowire.OP_COMMAND, mongowire.OP_MSG} {
			if err := s.RegisterOperation(&mongowire.OpScope{
				Type:    opType,
				Command: name,
			}, handler); err != nil {
				return errors.Wrapf(err, "could not register %s handler for %s", opType.String(), name)
			}
		}
	}

	return nil
}

const opMsgWireVersion = 6

func (s *shellService) isMaster(ctx context.Context, w io.Writer, msg mongowire.Message) {
	t := msg.Header().OpCode
	doc, _ := makeIsMasterResponse(0, opMsgWireVersion).MarshalDocument()
	resp, err := ResponseToMessage(t, doc)
	if err != nil {
		WriteErrorResponse(ctx, w, t, fmt.Errorf("could not make response: %w", err), isMasterCommand)
		return
	}
	WriteResponse(ctx, w, resp, isMasterCommand)
}

func (s *shellService) whatsMyURI(ctx context.Context, w io.Writer, msg mongowire.Message) {
	t := msg.Header().OpCode
	doc, _ := makeWhatsMyURIResponse(s.Address()).MarshalDocument()
	resp, err := ResponseToMessage(t, doc)
	if err != nil {
		WriteErrorResponse(ctx, w, t, fmt.Errorf("could not make response: %w", err), whatsMyURICommand)
		return
	}
	WriteResponse(ctx, w, resp, whatsMyURICommand)
}

func (s *shellService) buildInfo(ctx context.Context, w io.Writer, msg mongowire.Message) {
	doc, _ := makeBuildInfoResponse("0.0.0").MarshalDocument()
	resp, err := ResponseToMessage(msg.Header().OpCode, doc)
	if err != nil {
		WriteErrorResponse(ctx, w, msg.Header().OpCode, fmt.Errorf("could not make response: %w", err), buildInfoCommand)
		return
	}
	WriteResponse(ctx, w, resp, buildInfoCommand)
}

func (s *shellService) endSessions(ctx context.Context, w io.Writer, msg mongowire.Message) {
	WriteNotOKResponse(ctx, w, msg.Header().OpCode, getCmdLineOptsCommand)
}

func (s *shellService) getCmdLineOpts(ctx context.Context, w io.Writer, msg mongowire.Message) {
	WriteNotOKResponse(ctx, w, msg.Header().OpCode, getCmdLineOptsCommand)
}

func (s *shellService) getFreeMonitoringStatus(ctx context.Context, w io.Writer, msg mongowire.Message) {
	WriteNotOKResponse(ctx, w, msg.Header().OpCode, getFreeMonitoringStatusCommand)
}

func (s *shellService) getLog(ctx context.Context, w io.Writer, msg mongowire.Message) {
	doc, _ := makeGetLogResponse([]string{}).MarshalDocument()
	resp, err := ResponseToMessage(msg.Header().OpCode, doc)
	if err != nil {
		return
	}
	WriteResponse(ctx, w, resp, getLogCommand)
}

func (s *shellService) listCollections(ctx context.Context, w io.Writer, msg mongowire.Message) {
	WriteNotOKResponse(ctx, w, msg.Header().OpCode, listCollectionsCommand)
}

func (s *shellService) replSetGetStatus(ctx context.Context, w io.Writer, msg mongowire.Message) {
	WriteNotOKResponse(ctx, w, msg.Header().OpCode, replSetGetStatusCommand)
}

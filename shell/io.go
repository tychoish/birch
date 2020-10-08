package shell

import (
	"github.com/deciduosity/birch"
	"github.com/pkg/errors"
)

func intOK(ok bool) int {
	if ok {
		return 1
	}
	return 0
}

// ErrorResponse represents a response indicating whether the operation was okay
// and errors, if any.
type ErrorResponse struct {
	OK           int    `bson:"ok"`
	ErrorMessage string `bson:"errmsg,omitempty"`
}

// MakeErrorResponse returns an ErrorResponse with the given ok status and error
// message, if any.
func MakeErrorResponse(ok bool, err error) ErrorResponse {
	resp := ErrorResponse{OK: intOK(ok)}
	if err != nil {
		resp.ErrorMessage = err.Error()
	}
	return resp
}

func (r ErrorResponse) MarshalDocument() (*birch.Document, error) {
	return birch.DC.Elements(
		birch.EC.Int("ok", r.OK),
		birch.EC.String("errmsg", r.ErrorMessage)), nil
}

// MakeSuccessResponse returns an ErrorResponse that is ok and has no error.
func MakeSuccessResponse() ErrorResponse {
	return ErrorResponse{OK: intOK(true)}
}

func (r ErrorResponse) SuccessOrError() error {
	if r.ErrorMessage != "" {
		return errors.New(r.ErrorMessage)
	}
	if r.OK == 0 {
		return errors.New("response was not ok")
	}
	return nil
}

type isMasterResponse struct {
	ErrorResponse  `bson:"error_response,inline"`
	MinWireVersion int `bson:"minWireVersion"`
	MaxWireVersion int `bson:"maxWireVersion"`
}

func (imr isMasterResponse) MarshalDocument() (*birch.Document, error) {
	doc, _ := imr.ErrorResponse.MarshalDocument()
	return doc.Append(
		birch.EC.Int("minWireVersion", imr.MinWireVersion),
		birch.EC.Int("maxWireVersion", imr.MaxWireVersion)), nil
}

func makeIsMasterResponse(minWireVersion, maxWireVersion int) isMasterResponse {
	return isMasterResponse{
		MinWireVersion: minWireVersion,
		MaxWireVersion: maxWireVersion,
		ErrorResponse:  MakeSuccessResponse(),
	}
}

// whatsMyURIResponse represents a response indicating the service's URI.
type whatsMyURIResponse struct {
	ErrorResponse `bson:"error_response,inline"`
	You           string `bson:"you"`
}

func (resp whatsMyURIResponse) MarshalDocument() (*birch.Document, error) {
	doc, _ := resp.ErrorResponse.MarshalDocument()
	return doc.Append(birch.EC.String("you", resp.You)), nil
}

func makeWhatsMyURIResponse(uri string) whatsMyURIResponse {
	return whatsMyURIResponse{You: uri, ErrorResponse: MakeSuccessResponse()}
}

// buildInfoResponse represents a response indicating the service's build
// information.
type buildInfoResponse struct {
	ErrorResponse `bson:"error_response,inline"`
	Version       string `bson:"version"`
}

func (resp buildInfoResponse) MarshalDocument() (*birch.Document, error) {
	doc, _ := resp.ErrorResponse.MarshalDocument()
	return doc.Append(birch.EC.String("version", resp.Version)), nil
}

func makeBuildInfoResponse(version string) buildInfoResponse {
	return buildInfoResponse{Version: version, ErrorResponse: MakeSuccessResponse()}
}

// getLogResponse represents a response indicating the service's currently
// available logs.
type getLogResponse struct {
	ErrorResponse `bson:"error_response,inline"`
	Log           []string `bson:"log"`
}

func (resp getLogResponse) MarshalDocument() (*birch.Document, error) {
	doc, _ := resp.ErrorResponse.MarshalDocument()
	return doc.Append(birch.EC.SliceString("log", resp.Log)), nil
}

func makeGetLogResponse(log []string) getLogResponse {
	return getLogResponse{Log: log, ErrorResponse: MakeSuccessResponse()}
}

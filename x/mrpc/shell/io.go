package shell

import (
	"errors"
	"fmt"

	"github.com/tychoish/birch"
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

func (r *ErrorResponse) UnmarshalDocument(in *birch.Document) error {
	var ok bool

	for elem := range in.Iterator() {
		switch elem.Key() {
		case "ok":
			if r.OK, ok = elem.Value().IntOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		case "errmsg":
			if r.ErrorMessage, ok = elem.Value().StringValueOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		}
	}

	return nil
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

func (imr *isMasterResponse) UnmarshalDocument(in *birch.Document) error {
	if err := imr.ErrorResponse.UnmarshalDocument(in); err != nil {
		return err
	}

	var ok bool

	for elem := range in.Iterator() {
		switch elem.Key() {
		case "minWireVersion":
			if imr.MinWireVersion, ok = elem.Value().IntOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		case "maxWireVersion":
			if imr.MaxWireVersion, ok = elem.Value().IntOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		}
	}

	return nil
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

func (resp *whatsMyURIResponse) UnmarshalDocument(in *birch.Document) error {
	if err := resp.ErrorResponse.UnmarshalDocument(in); err != nil {
		return err
	}

	var ok bool

	for elem := range in.Iterator() {
		switch elem.Key() {
		case "you":
			if resp.You, ok = elem.Value().StringValueOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		}
	}

	return nil
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

func (resp *buildInfoResponse) UnmarshalDocument(in *birch.Document) error {
	if err := resp.ErrorResponse.UnmarshalDocument(in); err != nil {
		return err
	}

	var ok bool
	for elem := range in.Iterator() {
		switch elem.Key() {
		case "version":
			if resp.Version, ok = elem.Value().StringValueOK(); !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}
		}
	}

	return nil
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

func (resp *getLogResponse) UnmarshalDocument(in *birch.Document) error {
	if err := resp.ErrorResponse.UnmarshalDocument(in); err != nil {
		return err
	}

	for elem := range in.Iterator() {
		switch elem.Key() {
		case "version":
			array, ok := elem.Value().MutableArrayOK()
			if !ok {
				return fmt.Errorf("could not parse value of correct type [%s] for key %s",
					elem.Value().Type().String(), elem.Key())
			}

			resp.Log = make([]string, 0, array.Len())
			for value := range array.Iterator() {
				str, ok := value.StringValueOK()
				if !ok {
					return fmt.Errorf("could not parse value of correct type [%s] in array",
						value.Type().String())
				}

				resp.Log = append(resp.Log, str)
			}
		}
	}

	return nil
}

func makeGetLogResponse(log []string) getLogResponse {
	return getLogResponse{Log: log, ErrorResponse: MakeSuccessResponse()}
}

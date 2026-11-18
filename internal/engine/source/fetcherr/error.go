// Package fetcherr describes transport failure facts shared by acquisition edges.
package fetcherr

import (
	"errors"
	"fmt"
	"net"
	"net/http"
)

// StatusClass distinguishes transient and permanent failed responses.
type StatusClass uint8

// Terminal, Client and Transient classify rejected responses.
const (
	Terminal StatusClass = 1 << iota
	Client
	Transient
)

// ResponseError retains failed response facts and an optional original cause.
type ResponseError struct {
	StatusCode int
	Op         string
	Err        error
	// Message is live diagnostic text; persisted outcomes never retain it.
	Message string
}

// Error preserves live diagnostics or describes a redacted response.
func (e *ResponseError) Error() string {
	if e.Message != "" {
		return e.Message
	}
	if e.Err != nil {
		return e.Err.Error()
	}
	return fmt.Sprintf("HTTP %d", e.StatusCode)
}

// Unwrap exposes the original transport cause.
func (e *ResponseError) Unwrap() error { return e.Err }

// Classify categorizes a response already rejected by its acquisition edge.
func Classify(status int) StatusClass {
	if status == http.StatusTooManyRequests || status >= 500 && status < 600 {
		return Transient
	}
	if status >= 400 && status < 500 {
		return Client
	}
	return Terminal
}

// Classes collects every failed response category in a wrapped or joined error.
func Classes(err error) StatusClass {
	var classes StatusClass
	if response, ok := errors.AsType[*ResponseError](err); ok {
		classes = Classify(response.StatusCode)
	}
	if wrapped, ok := errors.AsType[interface {
		error
		Unwrap() []error
	}](err); ok && errors.Is(wrapped, err) {
		for _, child := range wrapped.Unwrap() {
			classes |= Classes(child)
		}
	} else if child := errors.Unwrap(err); child != nil {
		classes |= Classes(child)
	}
	return classes
}

// DialTimeout recognizes a connection timeout without deciding caller liveness.
func DialTimeout(err error) bool {
	op, ok := errors.AsType[*net.OpError](err)
	return ok && (op.Op == "dial" || op.Op == "connect") && op.Timeout()
}

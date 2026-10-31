package cache

import (
	"errors"
	"net/http"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/minio/minio-go/v7"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
)

type machineError struct{ cause error }

func (e *machineError) Error() string { return "machine credential acquisition failed" }
func (e *machineError) Unwrap() error { return e.cause }
func (e *machineError) Is(target error) bool {
	return target == manifest.ErrMissingSecret && credentialsUnavailable(e.cause)
}

// MachineCredentialErr masks machine diagnostics while preserving typed causes.
func MachineCredentialErr(cause error) error {
	if cause == nil {
		return nil
	}
	if _, ok := errors.AsType[*machineError](cause); ok {
		return cause
	}
	return &machineError{cause}
}

func credentialsUnavailable(err error) bool {
	if errors.Is(err, transport.ErrAuthenticationRequired) || errors.Is(err, transport.ErrAuthorizationFailed) || errors.Is(err, auth.ErrBasicCredentialNotFound) {
		return true
	}
	if response, ok := errors.AsType[*fetcherr.ResponseError](err); ok && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		return true
	}
	if response, ok := errors.AsType[*errcode.ErrorResponse](err); ok && (response.StatusCode == http.StatusUnauthorized || response.StatusCode == http.StatusForbidden) {
		return true
	}
	if codes, ok := errors.AsType[errcode.Errors](err); ok {
		for _, code := range codes {
			if code.Code == errcode.ErrorCodeUnauthorized || code.Code == errcode.ErrorCodeDenied {
				return true
			}
		}
	}
	if code, ok := errors.AsType[errcode.Error](err); ok && (code.Code == errcode.ErrorCodeUnauthorized || code.Code == errcode.ErrorCodeDenied) {
		return true
	}
	response, ok := errors.AsType[minio.ErrorResponse](err)
	return ok && response.Code == "AccessDenied"
}

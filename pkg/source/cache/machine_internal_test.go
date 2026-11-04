package cache

import (
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"

	"github.com/go-git/go-git/v5/plumbing/transport"
	"github.com/minio/minio-go/v7"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/errcode"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
)

func TestMachineCredentialErr_Unavailable(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name        string
		cause       error
		unavailable bool
	}{
		{"HTTP unauthorized", &fetcherr.ResponseError{StatusCode: http.StatusUnauthorized}, true},
		{"HTTP forbidden", &fetcherr.ResponseError{StatusCode: http.StatusForbidden}, true},
		{"registry unauthorized", &errcode.ErrorResponse{StatusCode: http.StatusUnauthorized}, true},
		{"registry forbidden", &errcode.ErrorResponse{StatusCode: http.StatusForbidden}, true},
		{"registry unauthorized code", errcode.Error{Code: errcode.ErrorCodeUnauthorized}, true},
		{"registry denied codes", errcode.Errors{{Code: errcode.ErrorCodeNameUnknown}, {Code: errcode.ErrorCodeDenied}}, true},
		{"Git authentication", transport.ErrAuthenticationRequired, true},
		{"Git authorization", transport.ErrAuthorizationFailed, true},
		{"ORAS basic", auth.ErrBasicCredentialNotFound, true},
		{"S3 denied", minio.ErrorResponse{Code: "AccessDenied"}, true},
		{"registry missing", errcode.Error{Code: errcode.ErrorCodeNameUnknown}, false},
		{"S3 missing", minio.ErrorResponse{Code: "NoSuchBucket"}, false},
		{"transient", &fetcherr.ResponseError{StatusCode: http.StatusServiceUnavailable}, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := MachineCredentialErr(fmt.Errorf("credential-canary: %w", tc.cause))
			if errors.Is(err, manifest.ErrMissingSecret) != tc.unavailable || strings.Contains(err.Error(), "canary") {
				t.Fatalf("unavailable=%t: %v", tc.unavailable, err)
			}
		})
	}
}

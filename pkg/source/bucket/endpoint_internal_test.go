package bucket

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/assert"
)

func TestNormalizeEndpoint(t *testing.T) {
	t.Parallel()
	cases := []struct {
		name       string
		endpoint   string
		insecure   bool
		wantHost   string
		wantSecure bool
		wantErr    string
	}{
		{
			name:       "https scheme stripped, tls forced on",
			endpoint:   "https://s3.example.com",
			wantHost:   "s3.example.com",
			wantSecure: true,
		},
		{
			name:       "http scheme stripped, tls forced off (ignores insecure flag)",
			endpoint:   "http://s3.example.com",
			insecure:   false,
			wantHost:   "s3.example.com",
			wantSecure: false,
		},
		{
			name:       "no scheme defaults to secure",
			endpoint:   "s3.example.com",
			wantHost:   "s3.example.com",
			wantSecure: true,
		},
		{
			name:       "no scheme with insecure flag",
			endpoint:   "minio.local:9000",
			insecure:   true,
			wantHost:   "minio.local:9000",
			wantSecure: false,
		},
		{
			name:       "trailing slash trimmed",
			endpoint:   "https://s3.example.com/",
			wantHost:   "s3.example.com",
			wantSecure: true,
		},
		{
			name:     "empty endpoint errors",
			endpoint: "",
			wantErr:  "empty",
		},
		{
			name:     "scheme-only endpoint errors after trim",
			endpoint: "https://",
			wantErr:  "empty",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			testNormalizeEndpointCase(t, &tc)
		},
		)
	}
}

func TestSchemeFor(t *testing.T) {
	t.Parallel()
	{
		got := schemeFor(true)
		assert.ErrorIf(t, got != "https", "schemeFor(true) = %q, want https", got)
	}
	{
		got := schemeFor(false)
		assert.ErrorIf(t, got != "http", "schemeFor(false) = %q, want http", got)
	}
}

func testNormalizeEndpointCase(t *testing.T, tc *struct {
	name       string
	endpoint   string
	insecure   bool
	wantHost   string
	wantSecure bool
	wantErr    string
},
) {
	t.Helper()

	endpointInfo, err := normalizeEndpoint(tc.endpoint, tc.insecure)

	if tc.wantErr != "" {
		if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
			t.Fatalf("err = %v, want substring %q", err, tc.wantErr)
		}
		return
	}
	assert.FatalIf(t, err != nil, "unexpected err: %v", err)
	if endpointInfo.host != tc.wantHost {
		t.Errorf("host = %q, want %q", endpointInfo.host, tc.wantHost)
	}
	if endpointInfo.secure != tc.wantSecure {
		t.Errorf("secure = %v, want %v", endpointInfo.secure, tc.wantSecure)
	}
}

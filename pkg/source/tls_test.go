package source

import (
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil"
)

func TestBuildTLSConfig_AllEmpty(t *testing.T) {
	_, err := BuildTLSConfig("", "", "")
	if err == nil || !strings.Contains(err.Error(), "no TLS material") {
		t.Errorf("expected no-material error; got %v", err)
	}
}

func TestBuildTLSConfig_CertWithoutKey(t *testing.T) {
	_, err := BuildTLSConfig("-cert-", "", "")
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Errorf("expected paired-creds error; got %v", err)
	}
}

func TestBuildTLSConfig_KeyWithoutCert(t *testing.T) {
	_, err := BuildTLSConfig("", "-key-", "")
	if err == nil || !strings.Contains(err.Error(), "must provide both") {
		t.Errorf("expected paired-creds error; got %v", err)
	}
}

func TestBuildTLSConfig_CAOnly(t *testing.T) {
	caPEM := testutil.SelfSignedCA(t)
	cfg, err := BuildTLSConfig("", "", caPEM)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if cfg.RootCAs == nil {
		t.Errorf("expected RootCAs populated from ca.crt")
	}
	if len(cfg.Certificates) != 0 {
		t.Errorf("expected no client cert when only ca.crt is supplied")
	}
	if cfg.MinVersion != 0x0303 { // TLS 1.2
		t.Errorf("MinVersion = %v, want TLS 1.2", cfg.MinVersion)
	}
}

func TestBuildTLSConfig_FullMaterial(t *testing.T) {
	certPEM, keyPEM := testutil.SelfSignedClientCert(t)
	caPEM := testutil.SelfSignedCA(t)
	cfg, err := BuildTLSConfig(certPEM, keyPEM, caPEM)
	if err != nil {
		t.Fatalf("BuildTLSConfig: %v", err)
	}
	if len(cfg.Certificates) != 1 {
		t.Errorf("expected 1 client cert, got %d", len(cfg.Certificates))
	}
	if cfg.RootCAs == nil {
		t.Errorf("expected RootCAs populated")
	}
}

func TestBuildTLSConfig_InvalidCAPEM(t *testing.T) {
	_, err := BuildTLSConfig("", "", "-----BEGIN CERTIFICATE-----\nnot-pem\n-----END CERTIFICATE-----")
	if err == nil || !strings.Contains(err.Error(), "did not parse as PEM") {
		t.Errorf("expected PEM parse error; got %v", err)
	}
}

func TestBuildTLSConfig_InvalidCertKey(t *testing.T) {
	_, err := BuildTLSConfig("-not-cert-", "-not-key-", "")
	if err == nil || !strings.Contains(err.Error(), "parse tls.crt/tls.key") {
		t.Errorf("expected cert/key parse error; got %v", err)
	}
}


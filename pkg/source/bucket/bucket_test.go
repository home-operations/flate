package bucket_test

import (
	"context"
	"strings"
	"testing"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source/bucket"
)

func TestFetcher_NonGenericProviderFailsLoud(t *testing.T) {
	f := &bucket.Fetcher{}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider: manifest.BucketProviderAmazon,
		BucketName: "x", Endpoint: "s3.amazonaws.com",
	}
	_, err := f.Fetch(context.Background(), b)
	if err == nil {
		t.Fatalf("expected error for unimplemented provider")
	}
	if !strings.Contains(err.Error(), "not implemented") {
		t.Errorf("error should say 'not implemented'; got %v", err)
	}
}

func TestFetcher_SecretRefWithoutGetter(t *testing.T) {
	f := &bucket.Fetcher{} // no Secrets
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider: manifest.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b)
	if err == nil {
		t.Fatalf("expected error when SecretRef set but no SecretGetter")
	}
	if !strings.Contains(err.Error(), "SecretGetter") {
		t.Errorf("error should mention SecretGetter; got %v", err)
	}
}

func TestFetcher_SecretRefMissingKeys(t *testing.T) {
	f := &bucket.Fetcher{
		Secrets: func(ns, name string) *manifest.Secret {
			return &manifest.Secret{
				Name: name, Namespace: ns,
				StringData: map[string]any{"accesskey": "k"}, // secretkey missing
			}
		},
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider: manifest.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b)
	if err == nil {
		t.Fatalf("expected error when accesskey/secretkey missing")
	}
	if !strings.Contains(err.Error(), "missing accesskey/secretkey") {
		t.Errorf("error should say missing accesskey/secretkey; got %v", err)
	}
}

func TestFetcher_SecretRefNotFound(t *testing.T) {
	f := &bucket.Fetcher{
		Secrets: func(_, _ string) *manifest.Secret { return nil },
	}
	b := &manifest.Bucket{
		Name: "b", Namespace: "ns",
		Provider: manifest.BucketProviderGeneric,
		BucketName: "x", Endpoint: "minio:9000",
		SecretRef: &manifest.LocalObjectReference{Name: "creds"},
	}
	_, err := f.Fetch(context.Background(), b)
	if err == nil {
		t.Fatalf("expected error when SecretRef not resolvable")
	}
	if !strings.Contains(err.Error(), "secret ns/creds not found") {
		t.Errorf("error should name the missing secret; got %v", err)
	}
}

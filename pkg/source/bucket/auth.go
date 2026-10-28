package bucket

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

type bucketAcquisition struct {
	bucket    *manifest.Bucket
	endpoint  string
	options   minio.Options
	material  any
	providers []credentials.Provider
}

func (f *operation) prepare(b *manifest.Bucket) (bucketAcquisition, error) {
	creds, authErr := f.resolveCredentials(b)
	if authErr != nil && !errors.Is(authErr, manifest.ErrMissingSecret) {
		return bucketAcquisition{}, authErr
	}
	endpoint, secure, err := normalizeEndpoint(b.Endpoint, b.Insecure)
	if err != nil {
		return bucketAcquisition{}, err
	}
	owner := manifest.DiagnosticFor(b.Named(), b)
	resolved, err := source.ResolveTransport(f.secrets, owner, []source.TransportInput{
		{Reference: b.ProxySecretRef, Use: source.TransportProxy},
		{Reference: b.CertSecretRef, Use: source.TransportCert},
	})
	if err != nil {
		return bucketAcquisition{}, err
	}
	if _, err := resolved.ProxyURL(&url.URL{Scheme: schemeFor(secure), Host: endpoint}); err != nil {
		return bucketAcquisition{}, fmt.Errorf("%w: resolve bucket proxy: invalid proxy configuration", manifest.ErrInput)
	}
	var material any
	if creds != nil {
		value, _ := creds.GetWithContext(nil)
		if value.AccessKeyID != "" || value.SecretAccessKey != "" {
			material = value
		}
	}
	if b.STS != nil || b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric && (b.Provider != sourcev1.BucketProviderAmazon || material == nil && authErr == nil) {
		if authErr == nil {
			authErr = source.MissingSecretErr(manifest.DiagnosticFor(b.Named(), b), "", "provider authentication unavailable offline")
		}
		return bucketAcquisition{}, authErr
	}
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Credentials: material})
	if err != nil {
		return bucketAcquisition{}, err
	}
	request := bucketAcquisition{
		bucket: b, endpoint: endpoint, material: resolved.Identity(material),
		options: minio.Options{MaxRetries: 1, Creds: creds, Secure: secure, Region: b.Region, Transport: tr},
	}
	if authErr != nil {
		request.providers = []credentials.Provider{&credentials.EnvAWS{}, &credentials.IAM{}}
	}
	return request, authErr
}

func (r bucketAcquisition) machineCredentials(ctx context.Context) (*credentials.Credentials, error) {
	cc := &credentials.CredContext{Context: ctx, Client: &http.Client{Transport: r.options.Transport}}
	for _, provider := range r.providers {
		value, err := provider.RetrieveWithCredContext(cc)
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if err != nil {
			if request, ok := errors.AsType[*url.Error](err); ok {
				target, parseErr := url.Parse(request.URL)
				dial, isDial := errors.AsType[*net.OpError](request.Err)
				timeout, isNetwork := errors.AsType[net.Error](request.Err)
				if parseErr == nil && target.Scheme+"://"+target.Host == credentials.DefaultIAMRoleEndpoint &&
					(isDial && dial.Op == "dial" || isNetwork && timeout.Timeout() || errors.Is(request.Err, context.DeadlineExceeded)) {
					continue
				}
			}
			return nil, err
		}
		if value.SignerType == credentials.SignatureAnonymous || value.AccessKeyID == "" && value.SecretAccessKey == "" {
			continue
		}
		if value.AccessKeyID == "" || value.SecretAccessKey == "" {
			return nil, fmt.Errorf("%w: no usable machine AWS credentials", manifest.ErrInput)
		}
		return credentials.NewStatic(value.AccessKeyID, value.SecretAccessKey, value.SessionToken, value.SignerType), nil
	}
	return nil, source.MissingSecretErr(manifest.DiagnosticFor(r.bucket.Named(), r.bucket), "", "no machine AWS credentials")
}

func (f *operation) resolveCredentials(b *manifest.Bucket) (*credentials.Credentials, error) {
	if b.SecretRef == nil {
		return credentials.NewStaticV4("", "", ""), nil
	}
	if f.secrets == nil {
		return nil, fmt.Errorf("%s references secretRef but no SecretGetter is wired", manifest.DiagnosticFor(b.Named(), b).Label())
	}
	sec := f.secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(b.Named(), b), b.SecretRef.Name, "not found")
	}
	access, _, err := sec.Value("accesskey")
	if err != nil {
		return nil, fmt.Errorf("%s: secret %s/%s: %w", manifest.DiagnosticFor(b.Named(), b).Label(), b.Namespace, b.SecretRef.Name, err)
	}
	secret, _, err := sec.Value("secretkey")
	if err != nil {
		return nil, fmt.Errorf("%s: secret %s/%s: %w", manifest.DiagnosticFor(b.Named(), b).Label(), b.Namespace, b.SecretRef.Name, err)
	}
	if access == "" || secret == "" || manifest.HasValuePlaceholder(access) || manifest.HasValuePlaceholder(secret) {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(b.Named(), b), b.SecretRef.Name, "missing accesskey/secretkey")
	}
	return credentials.NewStaticV4(access, secret, ""), nil
}

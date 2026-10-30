package bucket

import (
	"cmp"
	"context"
	"errors"
	"fmt"
	"io/fs"
	"net"
	"net/url"
	"os"
	"path/filepath"

	sourcev1 "github.com/fluxcd/source-controller/api/v1"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"

	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
)

type bucketAcquisition struct {
	network  *source.Network
	bucket   *manifest.Bucket
	endpoint string
	options  minio.Options
	material any
	resolved source.TransportMaterial
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
	if b.STS != nil || b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric && b.Provider != sourcev1.BucketProviderAmazon {
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
		network: f.Network, bucket: b, endpoint: endpoint, material: resolved.Identity(material), resolved: resolved,
		options: minio.Options{MaxRetries: 1, Creds: creds, Secure: secure, Region: b.Region, Transport: tr},
	}
	return request, authErr
}

func (r *bucketAcquisition) machineCredentials(ctx context.Context, providers []credentials.Provider) error {
	for _, provider := range providers {
		_, file := provider.(*credentials.FileAWSCredentials)
		if file {
			config, shared := os.Getenv("AWS_CONFIG_FILE"), os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
			if config == "" || shared == "" {
				home, err := os.UserHomeDir()
				if err != nil {
					return err
				}
				config, shared = cmp.Or(config, filepath.Join(home, ".aws", "config")), cmp.Or(shared, filepath.Join(home, ".aws", "credentials"))
			}
			configured := false
			for _, path := range []string{config, shared} {
				_, err := os.Stat(path) //nolint:gosec // Native AWS profiles accept user-selected paths; this only checks their presence.
				if err != nil && !errors.Is(err, fs.ErrNotExist) {
					return err
				}
				configured = configured || err == nil
			}
			if !configured {
				continue
			}
		}
		value, err := r.network.AWSCredentials(ctx, provider)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err != nil {
			if file {
				return err
			}
			if request, ok := errors.AsType[*url.Error](err); ok {
				target, parseErr := url.Parse(request.URL)
				dial, isDial := errors.AsType[*net.OpError](request.Err)
				timeout, isNetwork := errors.AsType[net.Error](request.Err)
				if parseErr == nil && target.Scheme+"://"+target.Host == credentials.DefaultIAMRoleEndpoint &&
					(isDial && dial.Op == "dial" || isNetwork && timeout.Timeout() || errors.Is(request.Err, context.DeadlineExceeded)) {
					continue
				}
			}
			return err
		}
		if value.SignerType == credentials.SignatureAnonymous || value.AccessKeyID == "" && value.SecretAccessKey == "" {
			continue
		}
		if value.AccessKeyID == "" || value.SecretAccessKey == "" {
			return fmt.Errorf("%w: no usable machine AWS credentials", manifest.ErrInput)
		}
		r.options.Creds = credentials.NewStatic(value.AccessKeyID, value.SecretAccessKey, value.SessionToken, value.SignerType)
		r.material = r.resolved.Identity(value)
		return nil
	}
	r.options.Creds = credentials.NewStaticV4("", "", "")
	return nil
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

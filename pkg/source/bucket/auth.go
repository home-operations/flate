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
	work     manifest.AcquisitionWork
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
	endpointInfo, err := normalizeEndpoint(b.Endpoint, b.Insecure)
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
	if _, proxyURLErr := resolved.ProxyURL(
		&url.URL{Scheme: schemeFor(endpointInfo.secure), Host: endpointInfo.host},
	); proxyURLErr != nil {
		return bucketAcquisition{}, manifest.NewError(
			manifest.ErrInput,
			nil,
			"resolve bucket proxy: invalid proxy configuration",
		)
	}
	var material any
	if creds != nil {
		value, _ := creds.GetWithContext(nil)
		if value.AccessKeyID != "" || value.SecretAccessKey != "" {
			material = value
		}
	}
	if b.STS != nil ||
		b.Provider != "" && b.Provider != sourcev1.BucketProviderGeneric &&
			b.Provider != sourcev1.BucketProviderAmazon {
		if authErr == nil {
			authErr = source.MissingSecretErr(
				manifest.DiagnosticFor(b.Named(), b),
				"",
				"provider authentication unavailable offline",
			)
		}
		return bucketAcquisition{}, authErr
	}
	tr, err := f.Network.HTTPTransport(source.HTTPTransportConfig{Material: resolved, Credentials: material})
	if err != nil {
		return bucketAcquisition{}, err
	}
	request := bucketAcquisition{
		network:  f.Network,
		bucket:   b,
		endpoint: endpointInfo.host,
		material: resolved.Identity(material),
		resolved: resolved,
		options: minio.Options{
			MaxRetries: 1,
			Creds:      creds,
			Secure:     endpointInfo.secure,
			Region:     b.Region,
			Transport:  tr,
		},
	}
	return request, authErr
}

func (r *bucketAcquisition) machineCredentials(ctx context.Context, providers []credentials.Provider) error {
	for _, provider := range providers {
		value, available, err := r.machineCredential(ctx, provider)
		if err != nil {
			return err
		}
		if !available {
			continue
		}
		if value.SignerType == credentials.SignatureAnonymous ||
			value.AccessKeyID == "" && value.SecretAccessKey == "" {
			continue
		}
		if value.AccessKeyID == "" || value.SecretAccessKey == "" {
			return manifest.NewError(manifest.ErrInput, nil, "no usable machine AWS credentials")
		}
		r.options.Creds = credentials.NewStatic(
			value.AccessKeyID,
			value.SecretAccessKey,
			value.SessionToken,
			value.SignerType,
		)
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
		return nil, errors.New("secretRef requires a SecretGetter")
	}
	sec := f.secrets(b.Namespace, b.SecretRef.Name)
	if sec == nil {
		return nil, source.MissingSecretErr(manifest.DiagnosticFor(b.Named(), b), b.SecretRef.Name, "not found")
	}
	valueResult, err := sec.Value("accesskey")
	access := valueResult.Value
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", b.Namespace, b.SecretRef.Name, err)
	}
	value, err := sec.Value("secretkey")
	if err != nil {
		return nil, fmt.Errorf("secret %s/%s: %w", b.Namespace, b.SecretRef.Name, err)
	}
	if access == "" || value.Value == "" {
		return nil, source.MissingSecretErr(
			manifest.DiagnosticFor(b.Named(), b),
			b.SecretRef.Name,
			"missing accesskey/secretkey",
		)
	}
	return credentials.NewStaticV4(access, value.Value, ""), nil
}

func machineFilesConfigured() (bool, error) {
	config, shared := os.Getenv("AWS_CONFIG_FILE"), os.Getenv("AWS_SHARED_CREDENTIALS_FILE")
	if config == "" || shared == "" {
		home, err := os.UserHomeDir()
		if err != nil {
			return false, fmt.Errorf("inspect AWS credential files: %w", err)
		}
		config, shared = cmp.Or(
			config,
			filepath.Join(home, ".aws", "config"),
		), cmp.Or(
			shared,
			filepath.Join(home, ".aws", "credentials"),
		)
	}
	configured := false
	for _, path := range []string{config, shared} {
		_, err := os.Stat( //nolint:gosec // Machine credential environment variables intentionally name caller-selected files.
			path,
		)
		if err != nil && !errors.Is(err, fs.ErrNotExist) {
			return false, fmt.Errorf("inspect AWS credential files: %w", err)
		}
		configured = configured || err == nil
	}
	return configured, nil
}

func unavailableIAMEndpoint(err error) bool {
	request, ok := errors.AsType[*url.Error](err)
	if !ok {
		return false
	}
	target, parseErr := url.Parse(request.URL)
	dial, isDial := errors.AsType[*net.OpError](request.Err)
	timeout, isNetwork := errors.AsType[net.Error](request.Err)
	return parseErr == nil && target.Scheme+"://"+target.Host == credentials.DefaultIAMRoleEndpoint &&
		(isDial && dial.Op == "dial" || isNetwork && timeout.Timeout() || errors.Is(request.Err, context.DeadlineExceeded))
}

func (r *bucketAcquisition) machineCredential(
	ctx context.Context,
	provider credentials.Provider,
) (credentials.Value, bool, error) {
	_, file := provider.(*credentials.FileAWSCredentials)
	if file {
		configured, fileErr := machineFilesConfigured()
		if fileErr != nil {
			return credentials.Value{}, false, fileErr
		}
		if !configured {
			return credentials.Value{}, false, nil
		}
	}
	value, err := r.network.AWSCredentials(ctx, provider)
	if ctx.Err() != nil {
		return credentials.Value{}, false, ctx.Err() //nolint:wrapcheck // Cancellation already identifies its cause; another operation prefix would add no resource information.
	}
	if err != nil {
		if file {
			return credentials.Value{}, false, err
		}
		if unavailableIAMEndpoint(err) {
			return credentials.Value{}, false, nil
		}
		return credentials.Value{}, false, err
	}
	return value, true, nil
}

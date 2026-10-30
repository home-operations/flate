package source

import (
	"cmp"
	"context"
	"net/http"
	"os"

	aws "github.com/minio/minio-go/v7/pkg/credentials"
	"helm.sh/helm/v4/pkg/helmpath"
	"oras.land/oras-go/v2/registry/remote/auth"
	"oras.land/oras-go/v2/registry/remote/credentials"
)

// RegistryCredentials resolves a host through the run's read-only native stores.
// Configuration MUST be identical for every borrower of the Network.
func (n *Network) RegistryCredentials(ctx context.Context, configPath, hostname string) (*auth.Credential, error) {
	n.registryOnce.Do(func() {
		opts := credentials.StoreOptions{DetectDefaultNativeStore: true}
		if configPath != "" {
			n.registryStore, n.registryErr = credentials.NewStore(configPath, opts)
			return
		}
		docker, err := credentials.NewStoreFromDocker(opts)
		if err != nil {
			n.registryErr = err
			return
		}
		helm, err := credentials.NewStore(cmp.Or(os.Getenv("HELM_REGISTRY_CONFIG"), helmpath.ConfigPath("registry/config.json")), opts)
		if err != nil {
			n.registryErr = err
			return
		}
		n.registryStore = credentials.NewStoreWithFallbacks(docker, helm)
	})
	if n.registryErr != nil {
		return nil, n.registryErr
	}
	cred, err := credentials.Credential(n.registryStore)(ctx, hostname)
	return &cred, err
}

// AWSCredentials retrieves machine credentials through the run's transport.
// IAM discovery outlives caller cancellation and MUST be joined by Close.
func (n *Network) AWSCredentials(ctx context.Context, provider aws.Provider) (aws.Value, error) {
	if err := ctx.Err(); err != nil {
		return aws.Value{}, err
	}
	retrieve := func(ctx context.Context, provider aws.Provider) (aws.Value, error) {
		return provider.RetrieveWithCredContext(&aws.CredContext{Context: ctx, Client: &http.Client{Transport: n}})
	}
	if _, iam := provider.(*aws.IAM); !iam {
		return retrieve(ctx, provider)
	}
	n.iamOnce.Do(func() {
		workerCtx, cancel := context.WithCancel(context.WithoutCancel(ctx))
		n.iamCancel = cancel
		n.iamDone = make(chan struct{})
		go func() {
			defer close(n.iamDone)
			defer cancel()
			n.iamValue, n.iamErr = retrieve(workerCtx, &aws.IAM{})
		}()
	})
	select {
	case <-ctx.Done():
		return aws.Value{}, ctx.Err()
	case <-n.iamDone:
		return n.iamValue, n.iamErr
	}
}

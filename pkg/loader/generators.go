package loader

import (
	"context"
	"encoding/base64"
	"fmt"
	"path/filepath"

	"sigs.k8s.io/kustomize/api/kv"
	kloader "sigs.k8s.io/kustomize/api/pkg/loader"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/manifest"
)

type generatorRecord struct {
	file        string
	owner       manifest.NamedResource
	namespace   string
	isConfigMap bool
	args        types.GeneratorArgs
}

func (r generatorRecord) materialize(ctx context.Context) (manifest.BaseManifest, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	root := kloader.NewFileLoaderAtRoot(filesys.MakeFsOnDisk())
	dir, err := filepath.Rel(root.Root(), filepath.Dir(r.file))
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s: %w", manifest.ErrInput, r.args.Name, err)
	}
	ldr, err := root.New(dir)
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s: %w", manifest.ErrInput, r.args.Name, err)
	}
	defer ldr.Cleanup() //nolint:errcheck // a local file loader has no temporary artifacts
	pairs, err := kv.NewLoader(ldr, provider.NewDefaultDepProvider().GetFieldValidator()).Load(r.args.KvPairSources)
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s in %s: %w", manifest.ErrInput, r.args.Name, r.file, err)
	}
	data := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		value := pair.Value
		if !r.isConfigMap {
			value = base64.StdEncoding.EncodeToString([]byte(value))
		}
		data[pair.Key] = value
	}
	if r.isConfigMap {
		return &manifest.ConfigMap{Name: r.args.Name, Namespace: r.namespace, Data: data}, nil
	}
	return &manifest.Secret{Name: r.args.Name, Namespace: r.namespace, Data: data}, nil
}

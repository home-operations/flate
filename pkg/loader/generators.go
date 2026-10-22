package loader

import (
	"context"
	"encoding/base64"
	"fmt"
	"io/fs"
	"path/filepath"

	"sigs.k8s.io/kustomize/api/ifc"
	"sigs.k8s.io/kustomize/api/kv"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/types"

	"github.com/home-operations/flate/internal/kustomizeinput"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

type generatorRecord struct {
	reader      documentReader
	root        string
	file        string
	owner       manifest.NamedResource
	namespace   string
	isConfigMap bool
	args        types.GeneratorArgs
}

var errRemoteInputs = fmt.Errorf("%w: remote generator input requires HTTPInputs", manifest.ErrInput)

func (r generatorRecord) materialize(ctx context.Context, opts Options) (manifest.BaseManifest, error) {
	var ldr ifc.Loader
	if len(r.args.EnvSources)+len(r.args.FileSources) != 0 {
		input, name := r.reader.input(r.root)
		input, err := fs.Sub(input, name)
		if err != nil {
			return nil, fmt.Errorf("%w: generator fs %s: %w", manifest.ErrInput, r.root, err)
		}
		confined := tree.NewBuilder(input)
		root, dir := r.root, filepath.Dir(r.file)
		ldr = generatorFileLoader{load: func(file string) ([]byte, error) {
			if _, _, ok := kustomizeinput.CutHTTPScheme(file); ok {
				if opts.RemoteInputs == nil {
					return nil, errRemoteInputs
				}
				acquired, err := opts.RemoteInputs.Acquire(ctx, file, opts.RemoteRetry, nil)
				if err != nil {
					return nil, err
				}
				return fs.ReadFile(acquired, "content")
			}
			if !filepath.IsAbs(file) {
				file = filepath.Join(dir, file)
			}
			name, err := filepath.Rel(root, file)
			if err != nil {
				return nil, err
			}
			return fs.ReadFile(confined, filepath.ToSlash(name))
		}}
	}
	pairs, err := kv.NewLoader(ldr, provider.NewDefaultDepProvider().GetFieldValidator()).Load(r.args.KvPairSources)
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s in %s: %w", manifest.ErrInput, r.args.Name, r.file, err)
	}
	data := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		value := pair.Value
		if opts.WipeSecrets {
			value = manifest.WipeValue(pair.Key, value, nil, false)
		}
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

// KV loading is synchronous; the adapter binds only the current materialization.
type generatorFileLoader struct {
	ifc.Loader
	load func(string) ([]byte, error)
}

func (l generatorFileLoader) Load(file string) ([]byte, error) { return l.load(file) }

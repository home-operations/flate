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
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/pkg/tree"
)

type generatorRecord struct {
	reader      documentReader
	root        string
	file        string
	owner       manifest.NamedResource
	source      manifest.NamedResource
	namespace   string
	isConfigMap bool
	args        types.GeneratorArgs
}

var errRemoteInputs = manifest.NewError(manifest.ErrInput, nil, "remote generator input requires HTTPInputs")

func (r *generatorRecord) materialize(ctx context.Context, optsArg *options) (store.Child, error) {
	opts := *optsArg
	var ldr ifc.Loader
	if len(r.args.EnvSources)+len(r.args.FileSources) != 0 {
		var err error
		ldr, err = r.inputLoader(ctx, &opts)
		if err != nil {
			return store.Child{}, err
		}
	}

	pairs, err := kv.NewLoader(ldr, provider.NewDefaultDepProvider().GetFieldValidator()).Load(r.args.KvPairSources)
	if err != nil {
		return store.Child{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("generator %s in %s: %w", r.args.Name, r.file, err),
			"",
		)
	}
	return materializeGeneratorData(r, pairs)
}

// KV loading is synchronous; the adapter binds only the current materialization.
type generatorFileLoader struct {
	ifc.Loader

	load func(string) ([]byte, error)
}

func (l generatorFileLoader) Load(file string) ([]byte, error) { return l.load(file) }

func materializeGeneratorData(r *generatorRecord, pairs []types.Pair) (store.Child, error) {
	data := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		value := pair.Value
		var encoding *base64.Encoding
		if !r.isConfigMap {
			encoding = base64.StdEncoding
			value = encoding.EncodeToString([]byte(value))
		}

		data[pair.Key] = value
	}
	kind := manifest.KindSecret
	if r.isConfigMap {
		kind = manifest.KindConfigMap
	}
	doc := map[string]any{
		"apiVersion": manifest.CoreAPIVersion,
		"kind":       kind,
		"metadata":   map[string]any{"name": r.args.Name, "namespace": r.namespace},
		"data":       data,
	}
	child, outcome, _ := Prepare(manifest.DocOccurrence{Document: doc}, manifest.ParseDocOptions{})
	return child, outcome.Err
}

//nolint:ireturn // Kustomize requires its loader interface for generator inputs.
func (r *generatorRecord) inputLoader(ctx context.Context, optsArg *options) (ifc.Loader, error) {
	opts := *optsArg
	inputFile := new(r.reader).input(r.root)
	input, err := fs.Sub(inputFile.fs, inputFile.name)
	if err != nil {
		return nil, manifest.NewError(manifest.ErrInput, fmt.Errorf("generator fs %s: %w", r.root, err), "")
	}
	confined := tree.NewBuilder(input)
	root, dir := r.root, filepath.Dir(r.file)
	return generatorFileLoader{load: func(file string) ([]byte, error) {
		if _, ok := kustomizeinput.CutHTTPScheme(file); ok {
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
			return nil, fmt.Errorf("generator path %s: %w", file, err)
		}
		return fs.ReadFile(confined, filepath.ToSlash(name))
	}}, nil
}

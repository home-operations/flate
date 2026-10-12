package loader

import (
	"encoding/base64"
	"fmt"
	"io/fs"
	"path/filepath"
	"strings"

	"sigs.k8s.io/kustomize/api/ifc"
	"sigs.k8s.io/kustomize/api/kv"
	kloader "sigs.k8s.io/kustomize/api/pkg/loader"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

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

func (r generatorRecord) materialize(wipeSecrets bool) (manifest.BaseManifest, error) {
	var ldr ifc.Loader
	if len(r.args.EnvSources)+len(r.args.FileSources) != 0 {
		input, name := r.reader.input(r.root)
		input, err := fs.Sub(input, name)
		if err != nil {
			return nil, fmt.Errorf("%w: generator fs %s: %w", manifest.ErrInput, r.root, err)
		}
		ldr = generatorFileLoader{Loader: kloader.NewFileLoaderAtRoot(filesys.MakeFsOnDisk()), fs: tree.NewBuilder(input), root: r.root, dir: filepath.Dir(r.file)}
	}
	pairs, err := kv.NewLoader(ldr, provider.NewDefaultDepProvider().GetFieldValidator()).Load(r.args.KvPairSources)
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s in %s: %w", manifest.ErrInput, r.args.Name, r.file, err)
	}
	data := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		value := pair.Value
		if wipeSecrets {
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

// Flux permits parent-relative files within the source root. URL loading stays
// upstream; local reads must pass through the source-confined filesystem.
type generatorFileLoader struct {
	ifc.Loader
	fs        fs.FS
	root, dir string
}

func (l generatorFileLoader) Load(file string) ([]byte, error) {
	if strings.HasPrefix(file, "http://") || strings.HasPrefix(file, "https://") {
		return l.Loader.Load(file)
	}
	if !filepath.IsAbs(file) {
		file = filepath.Join(l.dir, file)
	}
	name, err := filepath.Rel(l.root, file)
	if err != nil {
		return nil, err
	}
	return fs.ReadFile(l.fs, filepath.ToSlash(name))
}

package loader

import (
	"encoding/base64"
	"fmt"
	"path/filepath"
	"strings"

	fluxfilesys "github.com/fluxcd/pkg/kustomize/filesys"
	"sigs.k8s.io/kustomize/api/ifc"
	"sigs.k8s.io/kustomize/api/kv"
	kloader "sigs.k8s.io/kustomize/api/pkg/loader"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"github.com/home-operations/flate/pkg/manifest"
)

type generatorRecord struct {
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
		root := r.root
		if resolved, err := filepath.EvalSymlinks(root); err == nil {
			root = resolved
		}
		fs, err := fluxfilesys.MakeFsOnDiskSecure(root)
		if err != nil {
			return nil, fmt.Errorf("%w: secure generator fs %s: %w", manifest.ErrInput, root, err)
		}
		ldr = generatorFileLoader{Loader: kloader.NewFileLoaderAtRoot(filesys.MakeFsOnDisk()), fs: fs, dir: filepath.Dir(r.file)}
	}
	pairs, err := kv.NewLoader(ldr, provider.NewDefaultDepProvider().GetFieldValidator()).Load(r.args.KvPairSources)
	if err != nil {
		return nil, fmt.Errorf("%w: generator %s in %s: %w", manifest.ErrInput, r.args.Name, r.file, err)
	}
	data := make(map[string]any, len(pairs))
	for _, pair := range pairs {
		value := pair.Value
		if wipeSecrets && manifest.IsSopsCiphertext(value) {
			value = fmt.Sprintf(manifest.ValuePlaceholderTemplate, pair.Key)
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
	fs  filesys.FileSystem
	dir string
}

func (l generatorFileLoader) Load(file string) ([]byte, error) {
	if strings.HasPrefix(file, "http://") || strings.HasPrefix(file, "https://") {
		return l.Loader.Load(file)
	}
	if !filepath.IsAbs(file) {
		file = filepath.Join(l.dir, file)
	}
	return l.fs.ReadFile(file)
}

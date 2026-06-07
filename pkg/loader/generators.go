package loader

import (
	"cmp"
	"encoding/base64"
	"strings"

	"github.com/home-operations/flate/pkg/manifest"
)

// generatorRecord captures one configMapGenerator or secretGenerator
// entry found in a kustomization.yaml during discovery, plus enough
// context to resolve the effective namespace post-walk (kustomize
// inherits namespace from the calling Flux Kustomization when neither
// the generator nor the surrounding kustomization.yaml sets one).
//
// Tracking these records lets flate's depwait resolve dependencies
// like `substituteFrom: [ConfigMap/<ns>/cluster-settings]` even when
// the CM is produced by a configMapGenerator inside a Component —
// the CM has no on-disk YAML, so the file-walker would otherwise
// miss it.
type generatorRecord struct {
	// file is the absolute path to the kustomization.yaml that
	// declared the generator. Used by the post-walk finalizer to
	// look up the enclosing Flux Kustomization via KSPathPrefixes.
	file string
	// kustomizationNS is the kustomization.yaml's own `namespace:`
	// field. Empty means "inherit from the enclosing Flux KS."
	kustomizationNS string
	// entryNS is the generator entry's own `namespace:`. Overrides
	// kustomizationNS when set.
	entryNS string

	isConfigMap bool
	name        string
	literals    []string
}

// collectGeneratorRecords harvests every configMapGenerator and
// secretGenerator declaration in k. Files/Envs entries are NOT
// materialized (kustomize parses those at render time against the
// caller's file system); literals are enough to make depwait happy
// for the common cluster-settings pattern.
func collectGeneratorRecords(k *kustomization, file string) []generatorRecord {
	if k == nil {
		return nil
	}
	out := make([]generatorRecord, 0, len(k.ConfigMapGenerator)+len(k.SecretGenerator))
	collect := func(entries []kvPairGenerator, isConfigMap bool) {
		for _, g := range entries {
			if g.Name == "" {
				continue
			}
			out = append(out, generatorRecord{
				file: file, kustomizationNS: k.Namespace, entryNS: g.Namespace,
				isConfigMap: isConfigMap, name: g.Name, literals: g.Literals,
			})
		}
	}
	collect(k.ConfigMapGenerator, true)
	collect(k.SecretGenerator, false)
	return out
}

// materialize builds the synthesized manifest from r. parentNS is the
// namespace inherited from the enclosing Flux Kustomization; the
// caller looks it up via KSPathPrefixes against r.file. Precedence
// matches kustomize: entry.namespace > kustomization.namespace >
// parent (Flux KS) namespace.
//
// Data fidelity: literals are split on the first `=` per kustomize's
// own KvSources conventions. Secrets land in Data (base64-encoded)
// to match what a real k8s Secret carries; ConfigMaps land in Data
// as strings.
func (r generatorRecord) materialize(parentNS string) manifest.BaseManifest {
	// kustomize precedence: entry.namespace > kustomization.namespace > parent namespace.
	ns := cmp.Or(r.entryNS, r.kustomizationNS, parentNS)
	if r.isConfigMap {
		// ConfigMap data keeps the literal value as-is.
		return &manifest.ConfigMap{Name: r.name, Namespace: ns, Data: r.literalData(func(v string) any { return v })}
	}
	// Secret data is base64-encoded to match what a real k8s Secret carries.
	return &manifest.Secret{Name: r.name, Namespace: ns, Data: r.literalData(func(v string) any {
		return base64.StdEncoding.EncodeToString([]byte(v))
	})}
}

// literalData splits each `key=value` literal into a data map, passing every
// value through encode before it lands in the map. Literals without an `=`
// are skipped, per kustomize's KvSources conventions.
func (r generatorRecord) literalData(encode func(string) any) map[string]any {
	data := make(map[string]any, len(r.literals))
	for _, lit := range r.literals {
		k, v, ok := strings.Cut(lit, "=")
		if !ok {
			continue
		}
		data[k] = encode(v)
	}
	return data
}

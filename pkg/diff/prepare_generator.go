package diff

import (
	"strings"

	"sigs.k8s.io/kustomize/api/hasher"
	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/pkg/manifest"
)

func refineGenerated(idx map[pairKey]int, entries []pair, docs [2][]Doc) {
	var eligible [2]bool
	for k, entry := range idx {
		p := &entries[entry]
		if generatorOriginal(k.resource, p, docs) != nil {
			eligible[0] = eligible[0] || p.Old != nil
			eligible[1] = eligible[1] || p.New != nil
		}
	}
	if !eligible[0] || !eligible[1] {
		return
	}

	type candidates struct {
		pairs [2]*pair
		count [2]int
	}
	var groups map[pairKey]candidates
	for k, entry := range idx {
		p := &entries[entry]
		original := generatorOriginal(k.resource, p, docs)
		if original == nil {
			continue
		}
		name := verifiedGeneratorName(original, k.resource.Name)
		if name == "" {
			continue
		}
		if groups == nil {
			groups = make(map[pairKey]candidates)
		}
		k.resource.Name = name
		side := 0
		if p.Old == nil {
			side = 1
		}
		group := groups[k]
		group.count[side]++
		group.pairs[side] = p
		groups[k] = group
	}
	for k, group := range groups {
		if group.count != [2]int{1, 1} {
			continue
		}
		old, current := group.pairs[0], group.pairs[1]
		current.Old, current.OldChart, current.OldPos = old.Old, old.OldChart, old.OldPos
		current.Output[0] = old.Output[0]
		delete(idx, pairKey{parent: k.parent, resource: old.Resource})
	}
}

func generatorOriginal(resource manifest.NamedResource, p *pair, docs [2][]Doc) map[string]any {
	if (p.Old == nil) == (p.New == nil) || !resource.KindInfo().Data {
		return nil
	}
	side, pos := 0, p.OldPos
	if p.Old == nil {
		side, pos = 1, p.NewPos
	}
	original := docs[side][pos].Manifest
	// Identity normalization must remain authoritative; data normalization must not alter hash inputs.
	if manifest.DocIdentity(original) != resource {
		return nil
	}
	return original
}

func verifiedGeneratorName(original map[string]any, name string) string {
	dash := strings.LastIndexByte(name, '-')
	if dash <= 0 || len(name)-dash-1 != 10 {
		return ""
	}
	shorter, suffix := name[:dash], name[dash+1:]
	node, err := yaml.FromMap(original)
	if err != nil {
		return ""
	}
	if err = node.SetName(shorter); err != nil {
		return ""
	}
	hash, err := new(hasher.Hasher).Hash(node)
	if err != nil || hash != suffix {
		return ""
	}
	return shorter
}

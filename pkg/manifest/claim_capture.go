package manifest

import (
	"maps"
	"path/filepath"
	"slices"
)

// ClaimInput MUST use the captured source's index coordinate and native components.
type ClaimInput struct {
	Declaration BaseManifest
	Base        string
	Root        string
	IndexRoot   string
	Components  []string
	Pinned      bool
}

// CaptureClaims MUST preserve discovery facts while current declarations replace their claims.
func (i *DiscoveryIndex) CaptureClaims(inputs []ClaimInput) *DiscoveryIndex {
	if len(inputs) == 0 {
		return i
	}
	index := *i
	index.Records = maps.Clone(i.Records)
	if index.Records == nil {
		index.Records = make(map[NamedResource]DiscoveryRecord, len(inputs))
	}
	index.Claims = make(map[string][]NamedResource, len(i.Claims)+len(inputs))
	replaced := make(map[NamedResource]bool, len(inputs))
	for n := range inputs {
		input := &inputs[n]
		id := input.Declaration.Named()
		replaced[id] = true
		record := index.Records[id]
		record.Declaration, record.Pinned = input.Declaration, input.Pinned
		index.Records[id] = record
		for _, claim := range input.claims() {
			index.Claims[claim.Prefix] = appendUnique(index.Claims[claim.Prefix], id)
		}
	}
	for prefix, owners := range i.Claims {
		if len(index.Claims[prefix]) == 0 && canBorrowClaimGroup(owners, replaced) {
			index.Claims[prefix] = owners
			continue
		}
		for _, owner := range owners {
			if !replaced[owner] {
				index.Claims[prefix] = appendUnique(index.Claims[prefix], owner)
			}
		}
	}
	for _, owners := range index.Claims {
		if !slices.IsSortedFunc(owners, NamedResource.Compare) {
			slices.SortFunc(owners, NamedResource.Compare)
		}
	}
	return &index
}

func canBorrowClaimGroup(owners []NamedResource, replaced map[NamedResource]bool) bool {
	for n, owner := range owners {
		if replaced[owner] || n != 0 && owners[n-1].Compare(owner) >= 0 {
			return false
		}
	}
	return len(owners) != 0
}

func (in *ClaimInput) claims() []KSClaim {
	ks, structural := in.Declaration.(interface {
		appendClaims([]KSClaim, string, []string) []KSClaim
	})
	if !structural {
		return nil
	}
	claims := ks.appendClaims(nil, in.Base, in.Components)
	out := claims[:0]
	for _, claim := range claims {
		prefix := filepath.Clean(claim.Prefix)
		if in.IndexRoot != "" {
			var err error
			prefix, err = filepath.Rel(in.IndexRoot, filepath.Join(in.Root, claim.Prefix))
			if err != nil {
				continue
			}
		}
		prefix = filepath.ToSlash(prefix)
		if prefix == "." {
			prefix = ""
		} else {
			prefix += "/"
		}
		claim.Prefix = prefix
		out = append(out, claim)
	}
	return out
}

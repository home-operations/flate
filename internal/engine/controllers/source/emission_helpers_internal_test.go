package source

import "github.com/home-operations/flate/pkg/manifest"

func addDeclaration(index *manifest.DiscoveryIndex, id, decl manifest.NamedResource) {
	index.Records[id] = manifest.DiscoveryRecord{Productions: []manifest.Production{{Declaration: decl}}}
}

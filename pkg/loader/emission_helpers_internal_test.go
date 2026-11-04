package loader

import "github.com/home-operations/flate/pkg/manifest"

func productionForTest(recordValue *manifest.DiscoveryRecord) manifest.Production {
	r := *recordValue
	p := manifest.Production{Authored: r.Authored, Path: r.Path, File: r.File}
	if len(r.Productions) != 0 {
		p.Source = r.Productions[0].Source
	}
	return p
}

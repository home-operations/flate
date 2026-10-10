package helm

import (
	"helm.sh/helm/v4/pkg/chart/common"
	chart "helm.sh/helm/v4/pkg/chart/v2"
)

// Chart.Raw is used by Helm's show/save/create paths. Template, values and
// schema consumers use parsed fields; retaining Raw keeps their input copies.
func trimChart(ch *chart.Chart) {
	ch.Raw = nil
	for _, files := range [][]*common.File{ch.Templates, ch.Files} {
		for _, f := range files {
			if cap(f.Data) != len(f.Data) {
				data := make([]byte, len(f.Data))
				copy(data, f.Data)
				f.Data = data
			}
		}
	}
	if cap(ch.Schema) != len(ch.Schema) {
		data := make([]byte, len(ch.Schema))
		copy(data, ch.Schema)
		ch.Schema = data
	}
	for _, dep := range ch.Dependencies() {
		trimChart(dep)
	}
}

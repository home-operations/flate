package helm

import (
	"bytes"
	"context"
	"encoding/json"
	"iter"
	"path"
	"regexp"
	"strings"

	"helm.sh/helm/v4/pkg/chart/common"
	chartutil "helm.sh/helm/v4/pkg/chart/common/util"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

func (r *templateRequest) entryPrecondition(ctx context.Context, c *Client) (bool, error) {
	hr, loaded := r.release, &r.loaded
	if err := c.decodeChart(ctx, loaded, &RenderOptions{Work: r.input.Work}); err != nil {
		return true, err
	}
	if len(hr.ChartValuesFiles) > 0 && loaded.values.inputs == nil && loaded.valuesErr == nil {
		if loaded.values.names == nil {
			loaded.values, loaded.valuesErr = c.observeValuesFiles(
				ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles,
			)
		}
		if loaded.valuesErr == nil {
			loaded.valuesErr = loaded.values.observe(ctx, loaded, hr.ChartValuesFiles, hr.IgnoreMissingValuesFiles)
		}
	}
	if loaded.valuesErr != nil || r.capabilityErr != nil {
		return true, nil //nolint:nilerr // Invalid inputs MUST retain ordinary error precedence.
	}
	if hr.TracksDigest() {
		if _, err := ociChartVersion(loaded.chart.Metadata.Version, loaded.artifact.Revision); err != nil {
			return true, nil //nolint:nilerr // Rendering owns the ordered source-version diagnostic.
		}
	}
	complete := true
	_, possible := manifest.ProjectProducer(hr, entryApplicationContent(r.values, loaded, &complete))
	if !possible && complete {
		return false, nil
	}
	if r.input.EntryReach == nil || len(hr.PostRenderers) != 0 {
		return true, nil
	}
	input, err := r.chartValues(ctx, c)
	if err != nil {
		return true, nil //nolint:nilerr // Rendering owns the ordered values diagnostic.
	}
	effective, err := chartutil.CoalesceValues(loaded.chart, input)
	if err != nil {
		return true, nil //nolint:nilerr // Rendering owns the ordered values diagnostic.
	}
	objects, complete := projectedEntryObjects(loaded.chart, effective, hr.ReleaseNamespace())
	return !complete || r.input.EntryReach(objects), nil
}

var entrySpecExpression = regexp.MustCompile(
	`(?m)^spec: +\{\{[ \t]+toYaml[ \t]+\.Values\.([a-zA-Z0-9_]+(?:\.[a-zA-Z0-9_]+)*)[ \t]+\|[ \t]+nindent[ \t]+2[ \t]+\}\}[ \t]*$`,
)

// A chart with additional template actions can emit other roots.
func projectedEntryObjects(ch *chart.Chart, values map[string]any, namespace string) ([]manifest.BaseManifest, bool) {
	if len(ch.Dependencies()) != 0 || len(ch.Schema) != 0 || ch.Metadata.KubeVersion != "" ||
		len(ch.CRDObjects()) != 0 {
		return nil, false
	}
	var objects []manifest.BaseManifest
	for _, file := range ch.Templates {
		name := path.Base(file.Name)
		if name == "NOTES.txt" {
			continue
		}
		if strings.HasPrefix(name, "_") {
			return nil, false
		}
		object := projectedEntryObject(file.Data, values, namespace)
		if object == nil {
			return nil, false
		}
		objects = append(objects, object)
	}
	return objects, true
}

func projectedEntryObject(data []byte, values map[string]any, namespace string) manifest.BaseManifest {
	match := entrySpecExpression.FindSubmatchIndex(data)
	if match == nil || bytes.Count(data, []byte("{{")) != 1 {
		return nil
	}
	var spec any = values
	for key := range strings.SplitSeq(string(data[match[2]:match[3]]), ".") {
		fields, _ := spec.(map[string]any)
		spec = fields[key]
	}
	if _, ok := spec.(map[string]any); !ok {
		return nil
	}
	document := bytes.Join([][]byte{data[:match[0]], []byte("spec: {}"), data[match[1]:]}, nil)
	docs, err := manifest.DecodeDocs(bytes.NewReader(document), manifest.RequiredDocuments)
	if err != nil || len(docs) != 1 || docs[0].Err != nil {
		return nil
	}
	objectDoc := docs[0].Document
	if _, hasItems := objectDoc["items"]; hasItems {
		return nil
	}
	objectDoc["spec"] = spec
	parsed, err := manifest.ParseDoc(objectDoc, manifest.ParseDocOptions{})
	if err != nil || parsed.Object == nil {
		return nil
	}
	object := parsed.Object
	if object.Named().Namespace == "" && manifest.KindOf(object).Namespaced() {
		object = object.WithNamespace(namespace)
	}
	return object
}

func entryApplicationContent(values map[string]any, loaded *chartDescription, complete *bool) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if !yieldApplicationJSON(values, yield, complete) {
			return
		}
		for _, file := range loaded.values.inputs {
			if !yield(file.Data) {
				return
			}
			if file.Defaults != nil && !yieldApplicationJSON(file.Defaults, yield, complete) {
				return
			}
		}
		for data := range chartApplicationContent(loaded.chart, complete) {
			if !yield(data) {
				return
			}
		}
	}
}

func chartApplicationContent(ch *chart.Chart, complete *bool) iter.Seq[[]byte] {
	return func(yield func([]byte) bool) {
		if !yieldApplicationFiles(ch.Templates, yield) || !yieldApplicationFiles(ch.Files, yield) {
			return
		}
		if !yieldApplicationJSON(ch.Values, yield, complete) || !yieldApplicationJSON(ch.Metadata, yield, complete) ||
			!yieldApplicationJSON(ch.Lock, yield, complete) || !yield(ch.Schema) {
			return
		}

		for _, dependency := range ch.Dependencies() {
			for data := range chartApplicationContent(dependency, complete) {
				if !yield(data) {
					return
				}
			}
		}
	}
}

func yieldApplicationFiles(files []*common.File, yield func([]byte) bool) bool {
	for _, file := range files {
		if file != nil && !yield(file.Data) {
			return false
		}
	}
	return true
}

func yieldApplicationJSON(value any, yield func([]byte) bool, complete *bool) bool {
	data, err := json.Marshal(value)
	if err != nil {
		*complete = false
		return false
	}
	return yield(data)
}

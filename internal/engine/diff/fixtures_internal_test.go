package diff

import (
	"bytes"

	"github.com/home-operations/flate/pkg/manifest"
)

type documentProducer struct {
	manifest.RawObject

	path string
}

func (p *documentProducer) SourcePath() (manifest.NamedResource, string, bool) {
	return manifest.NamedResource{}, "./" + p.path, true
}

func documentSnapshots(left, right []Doc, opts *Options) [2]Snapshot {
	var snapshots [2]Snapshot
	for i, docs := range [2][]Doc{left, right} {
		snapshots[i].Subjects = opts.subjects[i]
		snapshots[i].ProducerKeys = opts.producerKeys[i]
		for _, doc := range docs {
			var parent manifest.BaseManifest
			if doc.Parent.NamedResource != (manifest.NamedResource{}) {
				apiVersion := "v1"
				if doc.Parent.Group != "" {
					apiVersion = doc.Parent.Group + "/v1"
				}
				parent = &documentProducer{
					Kind: doc.Parent.Kind, APIVersion: apiVersion,
					Name: doc.Parent.Name, Namespace: doc.Parent.Namespace,
					path: doc.Parent.Path,
				}
			}
			snapshots[i].Manifests = append(snapshots[i].Manifests, manifest.DocumentBatch{
				Parent: parent, Documents: []map[string]any{doc.Manifest},
			})
		}
	}
	return snapshots
}

func renderDocuments(left, right []Doc, opts *Options) ([]byte, []manifest.Warning, error) {
	snapshots := documentSnapshots(left, right, opts)
	comparison, err := Compare([]Group{{Orig: snapshots[0], Current: snapshots[1]}}, *opts)
	warnings := comparison.Groups[0].Warnings
	if err != nil {
		return nil, warnings, err
	}
	var buf bytes.Buffer
	if err := comparison.Write(&buf, nil); err != nil {
		return nil, warnings, err
	}
	return buf.Bytes(), warnings, nil
}

package kustomize

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"

	"github.com/home-operations/flate/pkg/manifest"
)

// PrepareApplication freezes the effective application before source or path decisions.
//
//nolint:gocritic // The descriptor is an immutable value snapshot.
func PrepareApplication(
	ctx context.Context,
	application manifest.EntryApplication,
	work *manifest.RenderWork,
) (manifest.EntryApplication, error) {
	if err := ctx.Err(); err != nil {
		//nolint:wrapcheck // Cancellation preserves the caller-visible sentinel identity.
		return manifest.EntryApplication{}, err
	}
	if application.Declaration == nil {
		return application, nil
	}
	data, err := json.Marshal(application.Declaration)
	if err != nil {
		return manifest.EntryApplication{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("encode application source: %w", err),
			"",
		)
	}
	var sourceSpec map[string]any
	if err = json.Unmarshal(data, &sourceSpec); err != nil {
		return manifest.EntryApplication{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("decode application source: %w", err),
			"",
		)
	}
	delete(sourceSpec, "name")
	delete(sourceSpec, "namespace")
	id := application.Declaration.Named()
	source := map[string]any{
		"apiVersion": id.KindInfo().APIVersion(), "kind": id.Kind,
		"metadata": map[string]any{"name": id.Name, "namespace": id.Namespace}, "spec": sourceSpec,
	}
	sync, err := application.Spec.RenderDocument()
	if err != nil {
		return manifest.EntryApplication{}, err
	}
	var documents bytes.Buffer
	for _, document := range []map[string]any{source, sync} {
		if documents.Len() != 0 {
			documents.WriteString("\n---\n")
		}
		data, err = json.Marshal(document)
		if err != nil {
			return manifest.EntryApplication{}, manifest.NewError(
				manifest.ErrInput,
				fmt.Errorf("encode application: %w", err),
				"",
			)
		}
		documents.Write(data)
	}
	out, err := PatchDocuments(DocumentsInput{
		Documents: documents.Bytes(), Patches: application.Patches, Transformers: application.Transformers,
		Namespace: application.Spec.Namespace, Subset: true,
		Work: work.Engine(manifest.WorkKustomize, "application"),
	})
	if err != nil {
		return manifest.EntryApplication{}, manifest.NewError(
			manifest.ErrInput,
			fmt.Errorf("prepare application: %w", err),
			"",
		)
	}
	if err = ctx.Err(); err != nil {
		//nolint:wrapcheck // Cancellation preserves the caller-visible sentinel identity.
		return manifest.EntryApplication{}, err
	}
	docs, err := manifest.DecodeDocs(bytes.NewReader(out), manifest.RequiredDocuments)
	if err != nil {
		return manifest.EntryApplication{}, err
	}
	if err := manifest.DocumentFailure(docs); err != nil {
		return manifest.EntryApplication{}, err
	}
	for _, doc := range docs {
		parsed, err := manifest.ParseDoc(doc.Document, manifest.ParseDocOptions{})
		if err != nil {
			return manifest.EntryApplication{}, err
		}
		if spec, ok := parsed.Object.(*manifest.Kustomization); ok {
			application.Spec = spec
		} else {
			application.Declaration = parsed.Object
		}
	}
	application.Source, application.Path = application.Spec.SourceID(), application.Spec.Path
	application.Patches, application.Transformers = nil, nil
	return application, nil
}

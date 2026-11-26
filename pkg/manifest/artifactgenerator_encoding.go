package manifest

import (
	"encoding/json"
	"encoding/json/jsontext"
	jsonv2 "encoding/json/v2"

	sourcewatcherv1 "github.com/fluxcd/source-watcher/api/v2/v1beta1"
)

// MarshalJSON preserves the field order and presence used by the content fingerprint.
//
//nolint:gocritic // Value snapshots must use the same encoding as pointers.
func (s ArtifactGeneratorSpec) MarshalJSON() ([]byte, error) {
	// The enclosing encoding/json encoder applies its HTML escaping setting.
	//nolint:wrapcheck // encoding/json adds the marshaler type to the error.
	return jsonv2.Marshal(s, json.DefaultOptionsV1(), jsontext.EscapeForHTML(false))
}

// MarshalJSONTo streams the fingerprint layout into the enclosing JSON value.
//
//nolint:gocritic // Value snapshots must use the same encoding as pointers.
func (s ArtifactGeneratorSpec) MarshalJSONTo(out *jsontext.Encoder) error {
	w := artifactGeneratorEncoder{out: out}
	w.token(jsontext.BeginObject)
	w.name("commonMetadata")
	w.token(jsontext.BeginObject)
	var metadata sourcewatcherv1.CommonMetadata
	if s.CommonMetadata != nil {
		metadata = *s.CommonMetadata
	}
	w.metadata("labels", metadata.Labels)
	w.metadata("annotations", metadata.Annotations)
	w.token(jsontext.EndObject)
	w.name("sources")
	if w.beginArray(s.Sources != nil) {
		for _, source := range s.Sources {
			w.token(jsontext.BeginObject)
			w.string("alias", source.Alias)
			w.string("name", source.Name)
			w.string("namespace", source.Namespace)
			w.string("kind", source.Kind)
			w.token(jsontext.EndObject)
		}
		w.token(jsontext.EndArray)
	}
	w.string("pathPattern", s.PathPattern)
	w.name("artifacts")
	if w.beginArray(s.OutputArtifacts != nil) {
		for i := range s.OutputArtifacts {
			w.artifact(&s.OutputArtifacts[i])
		}
		w.token(jsontext.EndArray)
	}
	if s.ManagedNamespaces != nil {
		w.name("managedNamespaces")
		if w.err == nil {
			w.err = jsonv2.MarshalEncode(out, s.ManagedNamespaces,
				jsonv2.Deterministic(true), jsonv2.FormatNilSliceAsNull(true), json.OmitEmptyWithLegacySemantics(true),
			)
		}
	}
	if s.ServiceAccountName != "" {
		w.string("serviceAccountName", s.ServiceAccountName)
	}
	w.token(jsontext.EndObject)
	return w.err
}

type artifactGeneratorEncoder struct {
	out *jsontext.Encoder
	err error
}

func (w *artifactGeneratorEncoder) artifact(output *sourcewatcherv1.OutputArtifact) {
	w.token(jsontext.BeginObject)
	w.string("name", output.Name)
	w.string("namespace", output.Namespace)
	w.string("inputsFrom", output.InputsFrom)
	w.string("revision", output.Revision)
	w.string("originRevision", output.OriginRevision)
	w.name("copy")
	if w.beginArray(output.Copy != nil) {
		for i := range output.Copy {
			op := &output.Copy[i]
			w.token(jsontext.BeginObject)
			w.string("from", op.From)
			w.string("to", op.To)
			w.string("strategy", op.Strategy)
			w.name("exclude")
			if w.beginArray(op.Exclude != nil) {
				for _, pattern := range op.Exclude {
					w.token(jsontext.String(pattern))
				}
				w.token(jsontext.EndArray)
			}
			w.name("optional")
			w.token(jsontext.Bool(op.Optional))
			w.token(jsontext.EndObject)
		}
		w.token(jsontext.EndArray)
	}
	w.token(jsontext.EndObject)
}

func (w *artifactGeneratorEncoder) token(token jsontext.Token) {
	if w.err == nil {
		w.err = w.out.WriteToken(token)
	}
}

func (w *artifactGeneratorEncoder) name(name string) {
	w.token(jsontext.String(name))
}

func (w *artifactGeneratorEncoder) string(name, value string) {
	w.name(name)
	w.token(jsontext.String(value))
}

func (w *artifactGeneratorEncoder) beginArray(present bool) bool {
	if present {
		w.token(jsontext.BeginArray)
	} else {
		w.token(jsontext.Null)
	}
	return present
}

func (w *artifactGeneratorEncoder) metadata(name string, values map[string]string) {
	w.name(name)
	if values == nil {
		w.token(jsontext.Null)
	} else if w.err == nil {
		w.err = jsonv2.MarshalEncode(w.out, values, jsonv2.Deterministic(true))
	}
}

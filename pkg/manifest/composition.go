package manifest

import "cmp"

// ArtifactComposition MUST preserve authored include order through final filtering.
type ArtifactComposition struct {
	Includes []ArtifactInclude
	Policy   string
	Ignore   string
}

// ArtifactInclude MUST bind its selected content through the declared Reference.
type ArtifactInclude struct {
	Reference Reference
	FromPath  string
	ToPath    string
}

// Composition MUST keep include references separate from acquisition credentials.
func (g *GitRepository) Composition() ArtifactComposition {
	result := ArtifactComposition{Policy: "source"}
	if g.Ignore != nil {
		result.Ignore = *g.Ignore
	}
	if len(g.Include) == 0 {
		return result
	}
	result.Includes = make([]ArtifactInclude, len(g.Include))
	for i := range g.Include {
		include := &g.Include[i]
		from := cmp.Or(include.GetFromPath(), ".")
		result.Includes[i] = ArtifactInclude{
			Reference: Reference{
				DependencyRef: DependencyRef{
					NamedResource: NamedResource{
						Group: SourceDomain, Kind: KindGitRepository,
						Namespace: g.Namespace, Name: include.GitRepositoryRef.Name,
					},
					Artifact: true, ArtifactOnly: true, RequiresDeclaration: true,
				},
				Use: "include", Position: i, Content: true, Gate: GateArtifacts,
				ArtifactStages: GateArtifacts, NamespaceDefault: true,
				Files: []string{from}, Subtree: true, bindsSource: true,
			},
			FromPath: from, ToPath: include.GetToPath(),
		}
	}
	return result
}

// Composition MUST remain empty because OCIRepository declares no composition inputs.
func (*OCIRepository) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition MUST remain empty because ExternalArtifact declares no composition inputs.
func (*ExternalArtifact) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition MUST remain empty because Bucket declares no composition inputs.
func (*Bucket) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition MUST remain empty because HelmRepository declares no composition inputs.
func (*HelmRepository) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition MUST remain empty because HelmChartSource declares no composition inputs.
func (*HelmChartSource) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because Kustomization declares no composition inputs.
func (*Kustomization) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because HelmRelease declares no composition inputs.
func (*HelmRelease) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because ResourceSet declares no composition inputs.
func (*ResourceSet) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because ResourceSetInputProvider declares no composition inputs.
func (*ResourceSetInputProvider) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because ConfigMap declares no composition inputs.
func (*ConfigMap) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because Secret declares no composition inputs.
func (*Secret) Composition() ArtifactComposition { return ArtifactComposition{} }

// Composition is empty because RawObject declares no composition inputs.
func (*RawObject) Composition() ArtifactComposition { return ArtifactComposition{} }

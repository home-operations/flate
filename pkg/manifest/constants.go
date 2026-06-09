package manifest

// API group prefixes. Versions are intentionally not pinned — we accept any
// version of these groups for forward compatibility with Flux upgrades.
const (
	FluxKustomizeDomain = "kustomize.toolkit.fluxcd.io"
	KustomizeDomain     = "kustomize.config.k8s.io"
	HelmReleaseDomain   = "helm.toolkit.fluxcd.io"
	SourceDomain        = "source.toolkit.fluxcd.io"
	// FluxOperatorDomain is the API group for flux-operator (controlplane.io)
	// resources — ResourceSet, ResourceSetInputProvider, FluxInstance.
	FluxOperatorDomain = "fluxcd.controlplane.io"
)

// Kubernetes kinds we recognize.
const (
	KindKustomization            = "Kustomization"
	KindHelmRelease              = "HelmRelease"
	KindHelmRepository           = "HelmRepository"
	KindHelmChart                = "HelmChart"
	KindGitRepository            = "GitRepository"
	KindOCIRepository            = "OCIRepository"
	KindExternalArtifact         = "ExternalArtifact"
	KindBucket                   = "Bucket"
	KindConfigMap                = "ConfigMap"
	KindSecret                   = "Secret"
	KindCustomResourceDefinition = "CustomResourceDefinition"
	KindResourceSet              = "ResourceSet"
	KindResourceSetInputProvider = "ResourceSetInputProvider"
)

// DefaultNamespace mirrors Flux's convention of placing top-level resources
// in `flux-system` when no namespace is declared.
const DefaultNamespace = "flux-system"

// BootstrapSourceID is the synthetic GitRepository the orchestrator
// seeds for the user's local working tree. Child Kustomizations whose
// sourceRef is patched in by a parent's render fall back to this id
// when their own SourceRef is still empty (#105 — see resolveSourceRoot).
// Exported so orchestrator and kustomization controller share one
// declaration.
var BootstrapSourceID = NamedResource{
	Kind: KindGitRepository, Namespace: DefaultNamespace, Name: DefaultNamespace,
}

// HelmRepository types as understood by Flux.
const (
	RepoTypeDefault = "default"
	RepoTypeOCI     = "oci"
)

// ValuePlaceholderTemplate is the format string used when wiping Secret
// values. The "{name}" token is replaced with the data key.
const ValuePlaceholderTemplate = "..PLACEHOLDER_%s.."

// ValuePlaceholderPrefix is the literal prefix produced by
// ValuePlaceholderTemplate; consumers checking whether a string is a
// wipe placeholder should use ContainsValuePlaceholder / IsValuePlaceholder
// rather than hard-coding the prefix.
const ValuePlaceholderPrefix = "..PLACEHOLDER_"

// KustomizeBuilderFilenames is the exact set of filenames `kustomize build`
// recognizes at any directory it builds — ordered by priority, first match
// wins. This is the upstream kustomize convention (yaml → yml → bare name).
//
// NOTE: this list intentionally differs from pkg/loader's kustomizationFileNames:
//   - it includes the bare "Kustomization" filename (no extension) which
//     kustomize build accepts as a valid root file name.
//   - it omits "kustomization.json" because kustomize build does not look for
//     a JSON-named file by default; that entry belongs only to the loader's
//     broader YAML/JSON scan pass.
//
// Multiple packages need this list (kustomize render, change-filter
// ownership, namespace inheritance) — keeping it here avoids cross-
// package coupling on a 3-element string slice.
var KustomizeBuilderFilenames = []string{
	"kustomization.yaml",
	"kustomization.yml",
	"Kustomization",
}

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

// HelmRepository types as understood by Flux.
const (
	RepoTypeDefault = "default"
	RepoTypeOCI     = "oci"
)

// ValuePlaceholderTemplate is the format string used when wiping Secret
// values. The "{name}" token is replaced with the data key.
const ValuePlaceholderTemplate = "..PLACEHOLDER_%s.."

// StripAttributes is the canonical list of metadata annotations that
// kustomize injects and which contribute only noise to diffs.
var StripAttributes = []string{
	"config.kubernetes.io/index",
	"internal.config.kubernetes.io/index",
}

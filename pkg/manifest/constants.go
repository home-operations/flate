package manifest

import "sigs.k8s.io/kustomize/api/konfig"

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
	// CoreAPIVersion is the full apiVersion of the group-less core
	// Kubernetes API, which is where ConfigMap and Secret live. A CRD
	// reusing either kind name carries a group, so the exact match keeps
	// it out of the typed parsers.
	CoreAPIVersion = "v1"
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

// ValuePlaceholderTemplate is the format string used when wiping Secret
// values. The "%s" verb is replaced with the data key.
const ValuePlaceholderTemplate = "..PLACEHOLDER_%s.."

// ValuePlaceholderPrefix is the literal prefix produced by
// ValuePlaceholderTemplate; consumers checking whether a string is a
// wipe placeholder should use ContainsValuePlaceholder / HasValuePlaceholder
// rather than hard-coding the prefix.
const ValuePlaceholderPrefix = "..PLACEHOLDER_"

// KustomizeBuilderFilenames is the upstream list of recognized root filenames.
var KustomizeBuilderFilenames = konfig.RecognizedKustomizationFileNames()

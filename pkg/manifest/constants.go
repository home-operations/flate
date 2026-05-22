package manifest

// API group prefixes. Versions are intentionally not pinned — we accept any
// version of these groups for forward compatibility with Flux upgrades.
const (
	FluxKustomizeDomain = "kustomize.toolkit.fluxcd.io"
	KustomizeDomain     = "kustomize.config.k8s.io"
	HelmReleaseDomain   = "helm.toolkit.fluxcd.io"
	SourceDomain        = "source.toolkit.fluxcd.io"
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
)

// DefaultNamespace mirrors Flux's convention of placing top-level resources
// in `flux-system` when no namespace is declared.
const DefaultNamespace = "flux-system"

// HelmRepository types as understood by Flux.
const (
	RepoTypeDefault = "default"
	RepoTypeOCI     = "oci"
)

// Bucket providers as understood by Flux. flate currently supports
// only "generic" (S3-compatible via minio-go); the others (aws, gcp,
// azure) parse correctly but fail-loud at fetch time.
const (
	BucketProviderGeneric = "generic"
	BucketProviderAmazon  = "aws"
	BucketProviderGoogle  = "gcp"
	BucketProviderAzure   = "azure"
)

// GitRepository providers as understood by Flux. flate currently
// supports only "generic" (SecretRef-based username/password / bearer
// / SSH identity); azure (Managed Identity) and github (GitHub App)
// require live cloud-provider auth flows and fail-loud at fetch time.
const (
	GitProviderGeneric = "generic"
	GitProviderAzure   = "azure"
	GitProviderGitHub  = "github"
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

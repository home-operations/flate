# flate

Render and diff Flux repositories without a cluster.

[![CI](https://github.com/home-operations/flate/actions/workflows/ci.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/ci.yaml) [![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases) [![License](https://img.shields.io/github/license/home-operations/flate)](LICENSE) [![Discord](https://img.shields.io/discord/673534664354430999?label=discord&logo=discord&logoColor=white&color=blue)](https://discord.gg/home-operations)

flate is a Go successor to [flux-local](https://github.com/allenporter/flux-local) that renders Flux manifests as one static binary.
It uses upstream libraries for Helm, Kustomize, Git and OCI, and fetches referenced sources as needed.
Native Docker credential helpers and AWS credential processes can run external programs.

## Install

```sh
brew install --cask home-operations/tap/flate
go install github.com/home-operations/flate/cmd/flate@latest
docker pull ghcr.io/home-operations/flate:latest
```

## Quick start

```sh
flate build # Render manifests
flate diff  # Compare rendered manifests with the detected Git baseline
flate test  # Report render status and exit 1 on failure
flate get   # List Kustomizations and HelmReleases in tables
```

Run from the repository root to render every detected cluster.
Use `-p` to choose a directory.
`build`, `get` and `test` render the full tree by default.
`diff` detects a Git baseline when neither `--base` nor `--path-orig` is set.

`get` prints one table per kind.
Use `-o name` for `Kind/name`, or `-o yaml` and `-o json` for objects.
`get images` lists container images under `IMAGE`.

## In CI

After checkout, run:

```yaml
- uses: home-operations/flate/action@v0.7.0
- run: flate test
```

For pre-commit, add this hook to `.pre-commit-config.yaml`:

```yaml
repos:
  - repo: https://github.com/home-operations/flate
    rev: v0.7.0
    hooks:
      - id: flate
```

## Common tasks

### Diff a pull request

```sh
flate diff --base main -o github
```

This compares rendered output with `main` and formats the diff for a GitHub comment.
`--base` and `--path-orig` are mutually exclusive.
A merge base is a shared ancestor commit.
Without either flag, `diff` tries merge bases in this order: the branch upstream, `origin/HEAD`, `origin/main`, then `origin/master`.
If shallow history prevents detection, use an explicit baseline or fetch more history.
Flate fetches explicit revisions absent from the checkout from `origin`.

Repeat `--strip-attr <key>` to ignore annotation or label keys.
The defaults are `helm.sh/chart`, `checksum/`, `app.kubernetes.io/version`, and `chart`.
The trailing slash in `checksum/` matches all keys with that prefix.
Repeat `--strip-field <path>` to ignore dotted fields. Its default list is empty.
Each flag replaces its own default list on first use.

`diff ks`, `diff hr`, and `diff all` accept `-o json`.
For one cluster present on both sides, they emit a 2-space-indented object with `changes` and `suppressed` arrays.
Empty arrays remain `[]`.
Each change contains `parent`, `apiVersion`, `kind`, `namespace`, `name`, `status`, `old`, `new`, `oldChart`, and `newChart`.
Status is `added`, `changed`, or `removed`.
The normalized `old` manifest is `null` for additions, and `new` is `null` for removals.
Chart labels are captured before stripping and remain empty strings when absent.

Parents contain `kind`, `namespace`, `name`, and `path`, which is empty for HelmReleases.
Changes sort by parent and resource identity.
Suppressions contain `parent`, `side`, `count`, and `reason`.
The `side` is `orig`, `current`, or `both`, and `count` counts withheld objects.
For multiple clusters, or an added or removed cluster, output is an array of these objects.
Each object also contains `cluster` and, when applicable, `added: true` or `removed: true`.
Render failures still write diagnostics to stderr and exit with status 1.
Other producers can still contribute changes on stdout.
`diff images` uses its own image format.

### Render one app

```sh
flate build hr plex -n media
```

This selects the `plex` HelmRelease in `media` and the inputs it needs.
Namespace selection also applies to `test` and `diff`.
Unselected sibling producers do not render or fetch their sources.

### Render changed objects

```sh
flate build --base main
```

A changed-only run renders objects that the diff reaches and the inputs they need.
The same rule applies to the root layer, which contains objects applied from the cluster entry directory.
An unchanged root layer stays outside the render unless the diff reaches it.

Changed inputs reach their consumers even when an ignore rule hides a required input.
Changes to unused substitution variables do not render unrelated output.
Use `--path-orig <directory>` to compare with another directory instead of a Git revision.

### Multiple clusters

```sh
flate get -p ./kubernetes
```

Reports for multiple clusters use `<cluster>: <name>` labels.
The `get` tables include a leading `CLUSTER` column.
Use `-p <cluster-entry>` to select one cluster.

### Supply objects from outside Git

```sh
flate build --supply ./offline-inputs.yaml
```

`--supply` accepts any Kubernetes kind that a render references, including sources and custom resources.
Each document needs `apiVersion`, `kind` and `metadata.name`.
Namespaced kinds also need `metadata.namespace`.

Repeat `--supply` for multiple YAML files, or use `--supply -` to read standard input once.
Paths are relative to the directory where you invoke flate.
Later duplicate identities replace whole supplied objects.
Both sides of a diff use the same supplied objects.

Repository objects take precedence over supplied objects.
For Secret and ConfigMap data, repository keys, including empty strings, win.
Supply fills absent keys.

Supplied objects satisfy references but never become render targets or appear as objects in output, selection or counts.
Referenced values can still appear in rendered resources.
Supplied Secret and ConfigMap data follows the same SOPS rule as repository data.
Encrypted keys stay absent, and repository plaintext keys take precedence.
Supply changes reference data only. It does not fill encrypted keys in emitted objects.

A `substituteFrom` Secret that exists only on the cluster leaves its variables unavailable.
A non-optional `substituteFrom` Secret with no readable keys produces an `UnresolvedSubstitution` warning.
Encrypted SOPS keys are absent, so their variables are unavailable.
`SOPSUnreadable` names the removed encrypted keys.
Flux substitution runs when a variable is readable or inline, or when the strategy is `Always`.
Active substitution applies defaults with or without `--strict-substitutions`.
Without `--strict-substitutions`, it replaces other unavailable `${VAR}` references with empty strings.
If no variables are readable under the default strategy, `${VAR}` remains literal.
Give those variables defaults (`${VAR:=value}`) or supply a dummy Secret with the keys.
A missing required `substituteFrom` ConfigMap still fails.

For unreadable SOPS keys, save a dummy input with the matching identity as `dummy-values.yaml`:

```yaml
apiVersion: v1
kind: Secret
metadata:
  name: cluster-secrets
  namespace: flux-system
stringData:
  DOMAIN: example.invalid
```

Use the values that your templates or chart schema need.
Then supply the file or read it through standard input:

```sh
flate build --path ./kubernetes --supply - < dummy-values.yaml
```

This supplies `DOMAIN` for references. It does not fill encrypted keys in the emitted Secret.
`--strict-substitutions` rejects undefined variables without defaults and is disabled by default.
Explicit flags replace the corresponding environment input, including `--strict-substitutions=false`.
Supply does not enable substitution without `postBuild`.
Defined empty values, `${VAR:=default}`, `${VAR:-default}`, and escaped `$${VAR}` keep their Flux behavior.
Resources with the `kustomize.toolkit.fluxcd.io/substitute: disabled` annotation or label bypass expansion.

### Private sources

flate looks for credentials in this order:

1. Real repository Secret data.
2. Supplied data for absent keys.
3. Machine credentials after no producer can provide the missing Secret.

Machine credentials include the SSH agent with local `known_hosts`, OCI registry stores, AWS environment, shared-profile and IAM credentials, and Helm repository logins.
For OCI, `--registry-config` selects one store.
Otherwise, flate tries the Docker store and then the Helm registry store.
Native stores can run configured credential helpers or the platform default helper.
Cluster Secrets never run helpers.

AWS shared profiles can run credential processes.
Profile errors fail the run, including a missing executable or SSO token.
Local Helm TLS, proxy, and forwarding configuration does not replace manifest transport or `passCredentials`.
HTTP charts can use a matching Helm `repositories.yaml` username and password.
Without `AWS_PROFILE`, shared files without a `default` profile fail before instance-role lookup.
To restore role lookup, set `AWS_PROFILE` or remove those files.
Helpers use the user's own configuration and network.

Authentication is silent on success.
Malformed explicit credentials fail instead of advancing to machine credentials.
If credentials remain unavailable, `--allow-missing-kinds Secret` skips the whole source with `SourceCredentialsUnavailable`.
Other acquisition failures, missing host verification, cancellation, and deadlines remain fatal.
When local selection yields no credential, OCI and HTTPS Git use the library's anonymous flow.
AWS Buckets resolve the machine chain, then use MinIO's anonymous signer.
Typed authentication rejection follows the same skip policy without a second anonymous acquisition.

### Cache between runs

```sh
flate build --cache-dir ~/.cache/flate
```

One cache stores fetched sources and eligible Helm and Kustomize renders across runs.
Without a cache directory, flate keeps cached content in memory.
Source freshness follows Flux intervals.

To preview removal of unused entries from that cache, run:

```sh
flate cache gc --cache-dir ~/.cache/flate --dry-run
```

`cache gc` requires an explicit `--cache-dir` or `FLATE_CACHE_DIR`.
It retires named tags by payload last-use, prunes unreachable Git generations, and delegates blob reclamation to ORAS graph GC.
Shared payloads survive while a retained tag can reach them.
`--max-age` selects the unused duration before pruning, and zero disables age-based pruning.

`--git-depth` limits Git source fetch history to one commit by default.
Set it to zero for full history.
References pinned to an explicit commit without a branch first fetch that commit at depth one, even with `--git-depth=0`, and fall back to full history if that request fails.
Commit pins that also specify a branch fetch full history.

## What flate renders

| Kind                                                            | Support                                                              |
| --------------------------------------------------------------- | -------------------------------------------------------------------- |
| Kustomization                                                   | Kustomize builds and postBuild substitution                          |
| HelmRelease                                                     | Helm charts, OCI chart references and inline OCIRepository templates |
| GitRepository, OCIRepository, HelmRepository, HelmChart, Bucket | Fetch referenced content                                             |
| ArtifactGenerator, ExternalArtifact                             | Generated artifacts                                                  |
| ResourceSet                                                     | Inline inputs and referenced input providers                         |
| ResourceSetInputProvider                                        | Defaults, exported inputs and OCI registry tags                      |

Unsupported dynamic ResourceSet input provider types with no offline inputs contribute no inputs and produce an `OfflineInputProvider` warning.
`OCIArtifactTag` can fetch registry tags without supplied `status.exportedInputs`.
Registry access still needs network access and any required credentials.

The bootstrap Git source is the repository with cluster manifests.
The working tree can serve as this source.
Missing OCIRepository, HelmRepository and Bucket references stay missing dependencies.
flate does not substitute the working tree for their content.

## Differences from a live cluster

- SOPS-encrypted keys are absent. flate keeps readable keys and removes the `sops` metadata.
  A `SOPSUnreadable` warning names the object and its unreadable keys.
  Supply a dummy Secret or ConfigMap with those keys through `--supply`.
  Re-encrypted ciphertext cannot produce a value diff.
- flate does not verify Git and OCI signatures.
- Helm renders fresh installs, without live release history.
- `--kube-version` and `--api-versions` set cluster facts for Helm.
- `--allow-missing-kinds` independently selects `Secret`, `ConfigMap` or `CustomResourceDefinition` for unavailable source credentials, Helm values or CRD dependencies.
- Required postBuild ConfigMap references and malformed real data still fail.
- flate does not perform cloud workload identity. Use repository, supplied or machine credentials.

When `--allow-missing-kinds` accepts unavailable Helm values, flate warns `OmittedValues`.
When it accepts missing CRD dependencies, flate warns `MissingCRD`.
`--allow-missing-kinds` accepts comma-separated or repeated selections and defaults to an empty list.

Helm template flags apply to every reconcile command because flate reconciles the full graph before filtering output.
`--enable-dns` enables DNS lookups in Helm templates. Source fetches still need network access when template DNS lookups are disabled.
`--disable-chart-digest-tracking` omits digest suffixes from OCI chart versions.
Install hooks, CRD handling, and validation follow `spec.install`.
Upgrade-only templates and template-path previews are unavailable because flate has no live release history.

`--restrict-egress` blocks private-address source fetches and local Git transports.
Configured HTTP proxies and SSH egress remain under operator control.
The restriction does not provide a general network sandbox.

If a producer cannot render, diffs withhold its objects and warn `DiffSuppressed`.
JSON diffs include these omissions in the `suppressed` array.

Secrets and CustomResourceDefinitions are excluded from output by default.
`--skip-kinds` replaces that exclusion list, and an empty list includes all kinds.
Build also accepts `--only-kinds` to select output kinds.
Both flags accept comma-separated or repeated selections.
`--skip-kinds Job` replaces the defaults, so rendered Secrets and CRDs become eligible for output, subject to HelmRelease CRD policy.
To exclude Job while keeping the defaults, use `--skip-kinds Secret,CustomResourceDefinition,Job`.
`--only-kinds CustomResourceDefinition` removes CRDs from the default exclusions.
An explicit `--skip-kinds` list wins over inclusion. Dependencies remain available regardless of output filtering.

## Flags

| Shared flag                       | Description                                               |
| --------------------------------- | --------------------------------------------------------- |
| `-p, --path`                      | Read Flux manifests from this directory                   |
| `-P, --path-orig`                 | Compare with a baseline directory (exclusive with --base) |
| `--base`                          | Compare with a Git revision (exclusive with --path-orig)  |
| `-n, --namespace`                 | Select resources in this namespace                        |
| `--supply`                        | Read reference-only YAML objects (repeatable flag)        |
| `--allow-missing-kinds`           | Tolerate missing input kinds (comma-separated values)     |
| `--strict-substitutions`          | Reject undefined substitutions without defaults           |
| `--skip-kinds`                    | Exclude output kinds (comma-separated values)             |
| `--registry-config`               | Read OCI credentials from a native Docker or Helm store   |
| `--cache-dir`                     | Store fetched sources and renders in this directory       |
| `--concurrency`                   | Limit parallel reconciles (0 means unbounded)             |
| `--git-depth`                     | Set Git fetch depth (0 means full history)                |
| `--restrict-egress`               | Block private-address fetches and local Git transports    |
| `--kube-version`                  | Set the cluster Kubernetes version for Helm               |
| `-a, --api-versions`              | Set Helm cluster APIs (comma-separated values)            |
| `--enable-dns`                    | Enable DNS lookups in Helm templates                      |
| `--disable-chart-digest-tracking` | Omit digest suffixes from OCI chart versions              |
| `--log-level`                     | Set log level: debug,info,warn,error                      |
| `--no-progress`                   | Hide live progress on stderr                              |

See `flate <command> --help` for defaults, environment variables, output formats and flags for each command.

## Library

Embed through `github.com/home-operations/flate/pkg/orchestrator`.
The `pkg/manifest` package provides resource types and identities.

```go
runs, err := orchestrator.RenderTrees(ctx, []orchestrator.Tree{{
    ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
    Path: ".",
}}, orchestrator.Config{Concurrency: 4})
```

## License

[AGPL-3.0](LICENSE). Behavior and fixtures also draw on flux-local (Apache-2.0).

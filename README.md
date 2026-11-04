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
flate warns `UnresolvedSubstitution` for those unavailable variables.
Flux substitution runs when a variable is readable or inline, or when the strategy is `Always`.
Active substitution applies defaults or replaces unavailable `${VAR}` references with empty strings.
If no variables are readable under the default strategy, `${VAR}` remains literal.
Give those variables defaults (`${VAR:=value}`) or supply a dummy Secret with the keys.
A missing required `substituteFrom` ConfigMap still fails.

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
Local Helm TLS and proxy settings do not replace manifest transport settings.

Authentication is silent on success.
Malformed explicit credentials fail instead of advancing to machine credentials.
If credentials remain unavailable, `--allow-missing-kinds Secret` skips the whole source with `SourceCredentialsUnavailable`.

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

## What flate renders

| Kind                                                            | Support                                                              |
| --------------------------------------------------------------- | -------------------------------------------------------------------- |
| Kustomization                                                   | Kustomize builds and postBuild substitution                          |
| HelmRelease                                                     | Helm charts, OCI chart references and inline OCIRepository templates |
| GitRepository, OCIRepository, HelmRepository, HelmChart, Bucket | Fetch referenced content                                             |
| ArtifactGenerator, ExternalArtifact                             | Generated artifacts                                                  |
| ResourceSet                                                     | Inline inputs and referenced input providers                         |
| ResourceSetInputProvider                                        | Defaults and exported inputs                                         |

Dynamic ResourceSet input providers with no offline inputs contribute no inputs and produce an `OfflineInputProvider` warning.

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

If a producer cannot render, diffs withhold its objects and warn `DiffSuppressed`.
JSON diffs include these omissions in the `suppressed` array.

Secrets and CustomResourceDefinitions are excluded from output by default.
`--skip-kinds` replaces that exclusion list, and an empty list includes all kinds.
Build also accepts `--only-kinds` to select output kinds.

## Flags

| Shared flag                       | Description                                                                |
| --------------------------------- | -------------------------------------------------------------------------- |
| `-p, --path`                      | Read Flux manifests from this directory                                    |
| `-P, --path-orig`                 | Compare with a baseline directory (exclusive with --base)                  |
| `--base`                          | Compare with a Git revision (exclusive with --path-orig)                   |
| `-n, --namespace`                 | Select resources in this namespace                                         |
| `--supply`                        | Read referenced Kubernetes objects from YAML (repeatable, `-` reads stdin) |
| `--allow-missing-kinds`           | Tolerate missing Secret, ConfigMap and CustomResourceDefinition inputs     |
| `--strict-substitutions`          | Match kustomize-controller StrictPostBuildSubstitutions                    |
| `--skip-kinds`                    | Exclude these output kinds (empty includes all)                            |
| `--registry-config`               | Read OCI credentials from one native store, including helpers              |
| `--cache-dir`                     | Store fetched sources and renders in this directory                        |
| `--concurrency`                   | Limit parallel reconciles (0 means unbounded)                              |
| `--git-depth`                     | Set Git fetch depth (0 means full history)                                 |
| `--restrict-egress`               | Block private-address fetches and local Git transports                     |
| `--kube-version`                  | Set the cluster Kubernetes version for Helm                                |
| `-a, --api-versions`              | Set cluster APIs available to Helm (comma-separated)                       |
| `--enable-dns`                    | Match helm-controller AllowDNSLookups                                      |
| `--disable-chart-digest-tracking` | Match helm-controller DisableChartDigestTracking                           |
| `--log-level`                     | Set log level: debug,info,warn,error                                       |
| `--no-progress`                   | Hide live progress on stderr                                               |

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

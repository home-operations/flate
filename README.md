# flate

Render and diff Flux repositories without a cluster.

[![CI](https://github.com/home-operations/flate/actions/workflows/ci.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/ci.yaml) [![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases) [![License](https://img.shields.io/github/license/home-operations/flate)](LICENSE) [![Discord](https://img.shields.io/discord/673534664354430999?label=discord&logo=discord&logoColor=white&color=blue)](https://discord.gg/home-operations)

flate is a Go successor to [flux-local](https://github.com/allenporter/flux-local) that renders Flux manifests as one static binary.
It uses upstream libraries for Helm, Kustomize, Git and OCI, and fetches referenced sources as needed.
Native Docker credential helpers and AWS credential processes can run external programs.

## Install

Use one of these commands:

```sh
brew install --cask home-operations/tap/flate
go install github.com/home-operations/flate/cmd/flate@latest
docker pull ghcr.io/home-operations/flate:latest
```

Homebrew installs the `flate` cask from `home-operations/tap`.
`go install` builds flate from source and needs a Go toolchain.
The Docker image is on the GitHub Container Registry.

## Quick start

```sh
flate build # Render manifests
flate diff  # Compare rendered manifests with the detected Git baseline
flate test  # Report render status and exit 1 on failure
flate get   # List Kustomizations and HelmReleases in tables
```

Run flate from the repository root to render every detected cluster.
Use `-p` to choose a directory.
`build`, `get` and `test` render the full tree by default.
The baseline is the version that `diff` compares against.
If you set neither `--base` nor `--path-orig`, `diff` detects a Git baseline.

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

CI logs are usually pipes, so flate prints plain text there.
Set `CLICOLOR_FORCE=1` to keep colors, or `NO_COLOR=1` to turn them off on a terminal.

## Common tasks

### Diff a pull request

```sh
flate diff --base main -o github
```

This command compares rendered output with `main` and formats the diff for a GitHub comment.
You cannot use `--base` and `--path-orig` together.
A merge base is a shared ancestor commit.
Without either flag, `diff` tries merge bases in this order: the branch upstream, `origin/HEAD`, `origin/main`, then `origin/master`.
A shallow clone contains only recent commits.
If a shallow clone prevents detection, use an explicit baseline or fetch more history.
If the checkout does not contain an explicit revision, flate fetches it from `origin`.

Repeat `--strip-attr <key>` to ignore annotation or label keys.
The defaults are `helm.sh/chart`, `checksum/`, `app.kubernetes.io/version`, and `chart`.
The trailing slash in `checksum/` matches all keys with that prefix.
Repeat `--strip-field <path>` to ignore fields.
A path uses dots between field names.
The default list of `--strip-field` is empty.
The first use of each flag replaces the default list of that flag.

`diff ks`, `diff hr`, and `diff all` accept `-o json`.
If one cluster is present on both sides, they write one object with `changes` and `suppressed` arrays.
The object uses an indent of 2 spaces.
An empty array stays `[]`.
Each change contains `parent`, `apiVersion`, `kind`, `namespace`, `name`, `status`, `old`, `new`, `oldChart`, and `newChart`.
The `status` is `added`, `changed`, or `removed`.
The normalized `old` manifest is `null` for additions, and `new` is `null` for removals.
flate reads the chart labels into `oldChart` and `newChart` before it removes the stripped keys.
If a chart label is absent, its field is an empty string.

Each parent contains `kind`, `namespace`, `name`, and `path`.
The `path` is empty for a HelmRelease.
flate sorts the changes by parent and by resource identity.
Each suppression contains `parent`, `side`, `count`, and `reason`.
The `side` is `orig`, `current`, or `both`.
The `count` is the number of objects that flate withheld.

If the diff has multiple clusters, or an added or removed cluster, the output is an array of these objects.
Each object also contains `cluster`.
An added cluster also has `added: true`, and a removed cluster has `removed: true`.
If a render fails, flate still writes diagnostics to stderr and exits with status 1.
A producer is an object that renders other objects, such as a Kustomization or a HelmRelease.
Other producers can still write changes to stdout.
`diff images` uses its own image format.

### Render one app

```sh
flate build hr plex -n media
```

This command selects the `plex` HelmRelease in `media` and the inputs that it needs.
The `-n` namespace selection also applies to `test` and `diff`.
Sibling producers that you do not select do not render or fetch their sources.

### Render changed objects

```sh
flate build --base main
```

A changed-only run renders the objects that the diff reaches and the inputs that they need.
The root layer is the set of objects that flate applies from the cluster entry directory.
The same rule applies to the root layer.
If the root layer is unchanged and the diff does not reach it, flate does not render it.

A changed input reaches its consumers.
If an ignore rule hides a required input, the change still reaches them.
Changes to unused substitution variables do not render unrelated output.
Use `--path-orig <directory>` to compare with another directory instead of a Git revision.

### Multiple clusters

```sh
flate get -p ./kubernetes
```

Reports for multiple clusters use `<cluster>: <name>` labels.
The `get` tables start with a `CLUSTER` column.
Use `-p <cluster-entry>` to select one cluster.

### Supply objects from outside Git

```sh
flate build --supply ./offline-inputs.yaml
```

`--supply` accepts any Kubernetes kind that a render references.
This includes sources and custom resources.
Each document needs `apiVersion`, `kind` and `metadata.name`.
Namespaced kinds also need `metadata.namespace`.

Repeat `--supply` for multiple YAML files, or use `--supply -` to read standard input once.
Paths are relative to the directory where you run flate.
If two supplied objects have the same identity, the later object replaces the earlier object.
Both sides of a diff use the same supplied objects.

Repository objects take precedence over supplied objects.
For Secret and ConfigMap data, the repository keys win.
This is also true for repository keys with empty strings.
Supplied data fills absent keys.

Supplied objects satisfy references but never become render targets or appear as objects in output, selection or counts.
Referenced values can still appear in rendered resources.
Supplied Secret and ConfigMap data follows the same SOPS rule as repository data.
Encrypted keys stay absent, and repository plaintext keys take precedence.
`--supply` changes only the reference data.
It does not fill encrypted keys in rendered objects.

Flux postBuild substitution replaces `${VAR}` references with variable values.
If a `substituteFrom` Secret exists only on the cluster, its variables are unavailable.
If a non-optional `substituteFrom` Secret has no readable keys, flate warns `UnresolvedSubstitution`.
Encrypted SOPS keys are absent, so their variables are unavailable.
The `SOPSUnreadable` warning names the removed encrypted keys.
If a variable is readable or inline, Flux substitution runs.
If the strategy is `Always`, substitution also runs.

Active substitution applies defaults with or without `--strict-substitutions`.
Without `--strict-substitutions`, it replaces other unavailable `${VAR}` references with empty strings.
If no variables are readable under the default strategy, `${VAR}` remains literal.
Give those variables defaults (`${VAR:=value}`) or supply a dummy Secret with the keys.
If a required `substituteFrom` ConfigMap is missing, the render still fails.

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

This command supplies `DOMAIN` for references.
It does not fill encrypted keys in the rendered Secret.
`--strict-substitutions` rejects undefined variables that have no default.
This flag is off by default.
An explicit flag replaces the matching environment input.
This is also true for `--strict-substitutions=false`.
`--supply` does not turn on substitution for a Kustomization without `postBuild`.
Defined empty values, `${VAR:=default}`, `${VAR:-default}`, and escaped `$${VAR}` keep their Flux behavior.
flate does not expand variables in resources that have the `kustomize.toolkit.fluxcd.io/substitute: disabled` annotation or label.

### Private sources

Machine credentials are the credentials on the machine that runs flate.
flate looks for credentials in this order:

1. Real repository Secret data.
2. Supplied data for absent keys.
3. Machine credentials, after no producer can provide the missing Secret.

Machine credentials include the SSH agent with local `known_hosts`, OCI registry stores, and Helm repository logins.
They also include AWS credentials from the environment, from shared profiles and from IAM.
For OCI, `--registry-config` selects one store.
Without `--registry-config`, flate tries the Docker store and then the Helm registry store.
Native stores can run configured credential helpers or the platform default helper.
Cluster Secrets never run helpers.

AWS shared profiles can run credential processes.
A profile error fails the run.
A missing executable or a missing SSO token is a profile error.
Local Helm configuration for TLS, proxies and forwarding does not replace manifest transport or `passCredentials`.
An HTTP chart can use the username and password of a matching entry in the Helm `repositories.yaml`.
If `AWS_PROFILE` is unset and the shared files have no `default` profile, they fail before the instance-role lookup.
To restore the role lookup, set `AWS_PROFILE` or remove those files.
Helpers use the configuration and the network of the user.

A successful authentication writes no output.
If explicit credentials are malformed, the run fails and flate does not go on to machine credentials.
If credentials stay unavailable, `--allow-missing-kinds Secret` skips the whole source and warns `SourceCredentialsUnavailable`.
Other acquisition failures, missing host verification, cancellation, and deadlines stay fatal.
If flate selects no local credential, OCI and HTTPS Git use the anonymous flow of the library.
AWS Buckets resolve the machine credential chain first, and then use the anonymous signer of MinIO.
If a server returns a typed authentication rejection, flate applies the same skip policy.
flate does not try a second anonymous acquisition.

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
It retires named tags by the last use of their payload.
It removes stored Git source copies that no tag can reach.
ORAS garbage collection then frees the unused blobs.
A shared payload stays while a retained tag can reach it.
`--max-age` sets how long an entry stays unused before flate removes it.
A zero `--max-age` turns off removal by age.

By default, `--git-depth` limits the fetch history of a Git source to one commit.
Set it to zero for full history.
A reference can pin an explicit commit without a branch.
flate first fetches that commit at depth one, also with `--git-depth=0`.
If that request fails, flate fetches the full history.
If a commit pin also names a branch, flate fetches the full history.

## What flate renders

| Kind                                                            | Support                                                              |
| --------------------------------------------------------------- | -------------------------------------------------------------------- |
| Kustomization                                                   | Kustomize builds and postBuild substitution                          |
| HelmRelease                                                     | Helm charts, OCI chart references and inline OCIRepository templates |
| GitRepository, OCIRepository, HelmRepository, HelmChart, Bucket | Fetch referenced content                                             |
| ArtifactGenerator, ExternalArtifact                             | Generated artifacts                                                  |
| ResourceSet                                                     | Inline inputs and referenced input providers                         |
| ResourceSetInputProvider                                        | Defaults, exported inputs and OCI registry tags                      |

flate does not support some dynamic input provider types for a ResourceSet.
If such a provider has no offline inputs, it contributes no inputs and flate warns `OfflineInputProvider`.
`OCIArtifactTag` can fetch registry tags without supplied `status.exportedInputs`.
Registry access still needs network access and any required credentials.

The bootstrap Git source is the repository with cluster manifests.
The working tree can be this source.
If a GitRepository URL matches a checkout remote, flate uses the local files when:

- No ref is set.
- The ref sets only `branch`.

If a source names a deployment branch, such as `main`, pull-request changes stay visible.
Each side of a diff uses its own files.
The `commit`, `tag`, `semver` and `name` ref fields keep their current behavior.
Use `ref.name: refs/heads/<branch>` to read a specific branch from the same repository.
Sources from other repositories keep their configured refs.
Sources with a ref and `recurseSubmodules: true` keep their current ref and fetch behavior.
Missing OCIRepository, HelmRepository and Bucket references stay missing dependencies.
flate does not use the working tree in place of their content.

## Differences from a live cluster

- SOPS-encrypted keys are absent. flate keeps readable keys and removes the `sops` metadata.
  A `SOPSUnreadable` warning names the object and its unreadable keys.
  Supply a dummy Secret or ConfigMap with those keys through `--supply`.
  Re-encrypted ciphertext cannot produce a value diff.
- flate ignores Git and OCI signatures.
- Helm renders each release as a fresh install, without live release history.
- `--kube-version` and `--api-versions` set cluster facts for Helm.
- `--allow-missing-kinds` independently selects `Secret`, `ConfigMap` or `CustomResourceDefinition`.
  These cover unavailable source credentials, Helm values and CustomResourceDefinition (CRD) dependencies.
- Required postBuild ConfigMap references and malformed real data still fail.
- flate does not perform cloud workload identity. Use repository, supplied or machine credentials.

When `--allow-missing-kinds` accepts unavailable Helm values, flate warns `OmittedValues`.
When it accepts missing CRD dependencies, flate warns `MissingCRD`.
`--allow-missing-kinds` accepts comma-separated or repeated selections.
Its default is an empty list.

Helm template flags apply to every reconcile command because flate reconciles the full graph before it filters output.
`--enable-dns` turns on DNS lookups in Helm templates.
If template DNS lookups are off, source fetches still need network access.
`--disable-chart-digest-tracking` omits digest suffixes from OCI chart versions.
Install hooks, CRD handling, and validation follow `spec.install`.
Upgrade-only templates and template-path previews are unavailable because flate has no live release history.

`--restrict-egress` blocks source fetches to private addresses and local Git transports.
The operator still controls configured HTTP proxies and outbound SSH traffic.
The restriction does not provide a general network sandbox.

If a producer cannot render, diffs withhold its objects and warn `DiffSuppressed`.
JSON diffs list these omissions in the `suppressed` array.

By default, flate excludes Secrets and CRDs from output.
`--skip-kinds` replaces that exclusion list.
An empty `--skip-kinds` list includes all kinds.
`build` also accepts `--only-kinds` to select output kinds.
Both flags accept comma-separated or repeated selections.

`--skip-kinds Job` replaces the defaults.
Rendered Secrets and CRDs can then appear in the output, subject to the HelmRelease CRD policy.
To exclude Job and keep the defaults, use `--skip-kinds Secret,CustomResourceDefinition,Job`.
`--only-kinds CustomResourceDefinition` removes CRDs from the default exclusions.
An explicit `--skip-kinds` list wins over inclusion.
Dependencies stay available for every output filter.

## Flags

The commands share the flags in this table:

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

Run `flate <command> --help` to see the defaults, environment variables, output formats and flags for each command.

## Library

Embed flate through `github.com/home-operations/flate/pkg/orchestrator`.
The `pkg/manifest` package provides resource types and identities.
This example renders the current directory as one cluster, with at most four parallel reconciles:

```go
runs, err := orchestrator.RenderTrees(ctx, []orchestrator.Tree{{
    ID: manifest.TreeID{Cluster: ".", Side: manifest.TreeHead},
    Path: ".",
}}, orchestrator.Config{Concurrency: 4})
```

## License

flate uses the [AGPL-3.0](LICENSE) license.
Its behavior and fixtures also draw on flux-local, which uses the Apache-2.0 license.
The [LICENSE](LICENSE) file contains the full text.

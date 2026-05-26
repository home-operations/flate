# flate

> Render and diff Flux GitOps repositories fully offline — one static binary, no cluster, no `kubectl`, no shellouts.

[![Tests](https://github.com/home-operations/flate/actions/workflows/tests.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/tests.yaml)
[![Build](https://github.com/home-operations/flate/actions/workflows/build.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/build.yaml)
[![Lint](https://github.com/home-operations/flate/actions/workflows/lint.yaml/badge.svg)](https://github.com/home-operations/flate/actions/workflows/lint.yaml)
[![Release](https://img.shields.io/github/v/release/home-operations/flate)](https://github.com/home-operations/flate/releases)
[![License](https://img.shields.io/github/license/home-operations/flate)](LICENSE)

flate is a Go rewrite of [flux-local](https://github.com/allenporter/flux-local). Helm, kustomize, go-git, and oras-go are linked as native libraries, so a `kind` cluster plus a stack of CLIs (`helm`, `kustomize`, `flux`, `kubectl`) collapse into one binary that runs in CI in seconds, not minutes. Changed-only mode reconciles just the subtree a PR touches, dropping single-file diffs to tens of milliseconds on real home-ops repos.

## Install

```bash
brew install --cask home-operations/tap/flate
go install github.com/home-operations/flate/cmd/flate@latest
docker pull ghcr.io/home-operations/flate:latest
```

…or in a GitHub Actions workflow:

```yaml
- uses: home-operations/flate/action@main
```

## Use

```bash
flate get ks       --path ./kubernetes
flate build hr     --path ./kubernetes plex
flate diff ks      --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff all     --path ./kubernetes --path-orig ../baseline/kubernetes
flate diff images  --path ./kubernetes --path-orig ../baseline/kubernetes -o json
flate test all     --path ./kubernetes
```

The `[name]` positional on `get ks/hr` and `build`/`diff`/`test ks/hr` is matched against the resource's bare name (`metadata.name`), not `namespace/name`. Use `-n / --namespace` to scope.

Every reconcile-running command takes `--path <dir>` (default `.`); `--path-orig <dir>` switches into changed-only mode. `flate <verb> --help` lists every flag. `get`, `build`, `diff`, and `test` all run the same offline reconcile pipeline before producing output, so referenced Git, OCI, Helm, Bucket, or remote kustomize sources must be reachable. Source caches respect Flux intervals: immutable pins reuse cache; mutable refs refresh when their interval expires.

| Verb | Targets | Notes |
|---|---|---|
| `get` | `ks`, `hr`, `images`, `all` | List or summarize. `-o table` / `yaml` / `json` / `name`. |
| `build` | `ks`, `hr`, `all` | Render Kustomizations and HelmReleases to YAML or JSON. |
| `diff` | `ks`, `hr`, `images`, `all` | Path-keyed diff against `--path-orig`, rendered via [dyff](https://github.com/homeport/dyff) in `--output github` mode. K8s-aware: list entries match by identifier (container name, env-var name), so a reorder shows as `⇆ order changed` instead of a wall of phantom value churn. |
| `test` | `ks`, `hr`, `all` | Pytest-style `PASS` / `FAIL` / `SKIPPED` per resource. Non-zero exit on any failure. |

`get ks` and `get hr` accept `-l/--selector key=value` for label filtering. `diff` accepts `--strip-attr <key>` (repeatable) to drop annotation/label keys before comparison; the default set covers chart-bump noise (`helm.sh/chart`, `checksum/config`, `app.kubernetes.io/version`, `chart`). Helm template flags are available on every reconcile-running subcommand because flate reconciles the full graph before filtering output. Reconcile-running subcommands accept `--allow-missing-secrets` to soft-skip source auth Secrets and omit generated HR `valuesFrom` refs that only exist in the live cluster — see [Behaviors](#behaviors).

**Default output filters.** `--skip-secrets` and `--skip-crds` both default to `true` — `build` and `diff` strip rendered `Secret` and `CustomResourceDefinition` objects from manifest output. Pass `--skip-secrets=false` / `--skip-crds=false` to include them; `--skip-kinds <kind>` (repeatable) drops additional kinds. These are output-stream filters, distinct from `--allow-missing-secrets`, which gates source auth and generated HR values Secret readiness.

## Changed-only mode

`--path-orig` flips every command into change-aware reconcile. flate diffs the two paths, walks ownership backwards (longest matching Flux KS `spec.path`, including `spec.components`), and reconciles only the touched subtree plus its content dependencies.

In the keep-set: direct file edits, chart sources, KS `sourceRef`, HR `valuesFrom`, kustomize components (touching a shared component re-renders every consumer).

Out: `dependsOn` (reconcile-ordering, not content — skipped resources still get marked `Ready` so downstream waits unblock) and meta-Kustomizations that don't claim the deeper file.

```bash
git worktree add ../baseline main
flate diff ks --path ./kubernetes --path-orig ../baseline/kubernetes
```

`--path` can point at a narrow Flux entry like `./kubernetes/flux/cluster`; flate iteratively follows each loaded KS's `spec.path` to discover the rest of the tree.

## Source kinds and auth

| Kind | Status | Auth (`spec.secretRef`) |
|---|---|---|
| `GitRepository` | full | HTTPS: `username` + `password` or `bearerToken`. SSH: `identity` (+ optional `password`, `known_hosts`). |
| `OCIRepository` | full | `.dockerconfigjson`. Falls back to `--registry-config`, then `~/.docker/config.json`. |
| `HelmRepository` | full | HTTP basic: `username` + `password`; OCI flavor routes through the OCI puller. |
| `HelmChart` | full | Inline (`HR.spec.chart`) and standalone CRD. |
| `Bucket` | `generic` only | `accesskey` + `secretkey`. `aws`/`gcp`/`azure` fail loud — use static creds. |
| `ExternalArtifact` | `file://` only | `status.artifact.url` must be a local path. |

PLACEHOLDER-wiped values (the always-on wipe of cleartext Secret data) are treated as missing — auth fails with a clear "missing username/password" instead of attempting auth with the placeholder string. See [Behaviors](#behaviors) for `--allow-missing-secrets`, which soft-skips affected sources end-to-end.

## Behaviors

**SOPS** — `spec.decryption` is not implemented. Encrypted Secret values get wiped to `..PLACEHOLDER_<key>..`, same as cleartext values under the always-on wipe. Downstream `postBuild.substituteFrom` lookups resolve to the placeholder string rather than failing.

**`spec.suspend`** — honored on every reconcilable CR. Suspended resources mark `Ready / "suspended"` and produce no rendered output.

**`--allow-missing-secrets`** — off by default. When set, a source whose auth `secretRef` is missing or PLACEHOLDER-wiped marks `Ready / "skipped: …"` instead of `Failed`, and consumers (KS `sourceRef`, HR `chartRef`) propagate the skip so `flate test` reports SKIPPED rather than a cascade of FAILED. HelmRelease `valuesFrom` Secret/ConfigMap refs that cannot materialize offline are omitted so the release can render with the remaining values. Verify, cert, and proxy `secretRef`s still fail loud, since silently dropping verification or TLS material is a security downgrade.

**`spec.dependsOn[].readyExpr` (CEL)** — evaluated against `self` and `dep` projections, matching upstream kustomize- and helm-controller binding:

```yaml
dependsOn:
- name: infra-controllers
  readyExpr: |
    dep.status.conditions.exists(c, c.type == "Healthy" && c.status == "True")
```

**Substitution opt-out** — the `kustomize.toolkit.fluxcd.io/substitute: disabled` label or annotation is honored per-resource, matching kustomize-controller. Used for ConfigMaps embedding bash array expansions envsubst can't parse.

**Signature verification** — `OCIRepository` uses cosign keyed mode (`spec.verify.secretRef` with PEM keys) verified through stdlib crypto, no sigstore dep tree. `GitRepository` uses PGP via `spec.verify.{mode,secretRef}`. Cosign keyless and `notation` are not supported (see [Limits](#limits)).

## Library use

`pkg/orchestrator` is the embed entry point.

```go
import (
    "context"
    "github.com/home-operations/flate/pkg/orchestrator"
)

o, _ := orchestrator.New(orchestrator.Config{Path: "/path/to/cluster"})
res, err := o.Render(context.Background())
// res is non-nil even when err != nil — partial output stays usable.
for id, docs := range res.Manifests {
    // rendered YAML docs for the KS / HR with this id
}
for id, info := range res.Failed {
    // structured failure list keyed by NamedResource
}
```

Other entry points worth knowing:

- `Orchestrator.WithFetcher(kind, f)` — swap any source fetcher (in-memory fakes for tests, custom kinds).
- `Store.OnObject` / `OnStatus` / `OnArtifact` — typed listeners; payloads are pre-cast.
- `helm.Prepare(hr, lookup, provider)` then `helmClient.TemplateDocs(...)` — render one HelmRelease without the orchestrator. `lookup` is a `manifest.HelmChartLookup` (`func(ns, name string) *HelmChartSource`). `kustomize.Prepare(ks, provider)` is the symmetric helper for Kustomizations.
- `discovery.Run(ctx, Config{Path, Store, WipeSecrets})` — load phase as a standalone unit.
- `change.Filter.Add(id)` — extend the changed-only-mode keep set at runtime when a custom controller emits a child that wasn't visible at filter-build time. Call BEFORE `Store.AddObject(child)` so the synchronous listener sees the extended set.
- `Store.Mutate[T]` — clone-then-AddObject helper encoding the immutability contract. See [`pkg/manifest/doc.go`](pkg/manifest/doc.go) for the full rule.

## Architecture

```
discovery → Store ⇄ events ⇄ controllers (source · kustomization · helmrelease)
```

Pipeline: bootstrap-source seed → loader pre-pass excluding `configMapGenerator`/`secretGenerator` data files → file walk → `spec.path` + ResourceSet fixed-point expansion → bootstrap-source aliasing for unresolved `GitRepository` refs → namespace inheritance → parent index → `dependsOn` cycle preflight → change-filter → controllers fire → render → render-time keep-set extension for emitted children → orphan demotion → output.

The Store is the single source of truth. Every stored manifest is immutable; mutation routes through `Store.Mutate[T]` (clone, mutate, AddObject). Helm chart loads coalesce through a per-path keylock — N parallel reconciles of the same chart issue exactly one parse.

## Limits

flate is rendering-only.

- **No SOPS decryption.** Values wiped; pre-decrypt if you need them in the diff.
- **No cosign keyless.** Keyed verification works end-to-end; keyless logs and renders unverified (no offline trust roots).
- **No notation.** Fails loud.
- **No cloud workload identity.** `spec.serviceAccountName` is a no-op; use static creds in a Secret.
- **No `healthChecks`.** flate tracks resource readiness, not status conditions of rendered objects.
- **`ResourceSetInputProvider`: `Static` only.** Dynamic providers (GitHub, GitLab, OCIArtifactTag, ExternalService) need network access and contribute zero inputs.
- **Diff output isn't a unified-diff patch.** `flate diff` emits dyff path-keyed syntax (`@@ <path> @@`); GitHub's diff lexer renders it natively but it won't apply with `patch` / `git apply` — use the rendered output of `flate build` if you need a literal patch.

## Development

```bash
go build ./cmd/flate
go test ./...
go test -race ./...
golangci-lint run ./...
```

Tool versions pin via [mise](https://mise.jdx.dev). Testdata lives in [`testdata/`](testdata/); [`test/e2e`](test/e2e/) runs the cobra command tree in-process — no fork/exec, no freshly built binary.

## License

AGPL-3.0. flate borrows behavior and test fixtures from [flux-local](https://github.com/allenporter/flux-local) (Apache-2.0).

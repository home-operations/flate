# AGENTS.md: flate

flate renders Flux (Kustomization, HelmRelease, ResourceSet, sources) offline, as one static
binary: no cluster, no kubectl, no shellouts. helm, kustomize, git and OCI run as linked
libraries. Output must match Flux bit-for-bit and be byte-identical run to run. Speed is the
point of the project: changed-only renders, a bounded parallel DAG, aggressive dedup.
`pkg/` is a public SDK (konflate embeds `pkg/orchestrator`); treat exported `pkg/` API and
`Warning.Category` codes as compatibility-sensitive.

## Working here

- The org [AI Usage Policy](https://github.com/home-operations/.github/blob/main/CONTRIBUTING.md#ai-usage-policy)
  applies. Fill the PR template's Requirements section truthfully.
- PR titles are [Conventional Commits](https://www.conventionalcommits.org/en/v1.0.0/)
  (`fix(schedule): ...`); they become the squash commit and drive release-please. Sign off
  commits (`git commit -s`). Never commit, push, or open a PR unless asked.
- Never touch secrets or gitignored files. Verify library APIs against the module cache or
  pkg.go.dev, not memory.
- Solve the stated problem with the smallest diff: no speculative abstractions, no interface
  or options struct with one caller, no new flags, no new dependencies, no drive-by refactors.
  Remove what your change orphans; leave pre-existing dead code and mention it.

## Layout

`cmd/flate` is the entrypoint; `internal/cli` owns flags, exit codes (0 or 1), signal handling
and `slog.SetDefault`. Everything else is `pkg/`. Dependencies flow one way:
`manifest` <- `store` <- {`loader`, `values`, `depwait`, `change`, `source`} <- {`kustomize`,
`helm`, `discovery`} <- `controllers/base` <- `controllers/*` <- `orchestrator` <- `internal/cli`.
Leaf packages stay leaf: `tree` (stdlib only), `manifest`, `task`, `schedule` (store and controllers only behind its
`Dispatcher` seam), `source/{atomic,cacheroot,safepath,sourceignore,ssrfguard}`,
`internal/assert`. Never add an upward import.

## Invariants

- Stored manifests are immutable: clone, mutate the copy, `AddObject` again. Never write to an
  embedded `Spec` after parse.
- Store listeners run inline and must never block on the store. Writers snapshot listeners
  with `fireUnderLock` and dispatch after unlocking. Anything locking more than one shard uses
  `lockAll`/`rLockAll`.
- The scheduler never touches the store, the pool or a dispatcher while holding its mutex;
  drop `mu` before `tasks.Go`. Termination is a structural fixpoint plus drain, never a
  timeout.
- Every krusty build holds `kustomize.BuildMutex`. It is the only global serialization point;
  do not add another on the render path.
- Changed-only mode: `KeepEmitted`/`AddEmitted` before `AddObject`.
- Only SOPS ciphertext is wiped (`..PLACEHOLDER_<key>..`); missing secrets fail loud unless a
  producer exists or `--allow-missing-secrets` is set; cert and proxy refs always fail loud.
- Fetched content goes through `safepath` and `atomic` writes; cache paths come only from
  `cacheroot.Layout`; the SSRF guard is process-global and opt-in via `RestrictEgress`.
- Output order never depends on goroutine scheduling: sort anything derived from a map.

## Performance and concurrency

- Reconcile bodies run only through `task.Service.Go`, bounded by `--concurrency`
  (default `NumCPU*4`); wrap waits on other slot-gated work in `task.YieldSlot`. Use errgroup
  only for fixed fan-out.
- No new locks, widened critical sections, channel round-trips or serialization on the
  render, store, discovery, change-detection or scheduler paths. Prefer immutable snapshots,
  existing shards, atomics, and work computed once before the parallel phase.
- Deduplicate expensive work by key (`keylock`, per-URL `sync.Once`, fingerprint dedup,
  template and disk caches) instead of re-rendering, re-fetching or re-parsing.
- Hot paths that are allocation-free stay allocation-free; allocs/op must not rise on an
  existing benchmark.
- Any PR touching a package with benchmarks pastes a `benchstat` comparison: `COUNT=10 mise run bench`
  on the base commit and on the branch, `benchstat <base>.txt <branch>.txt` (results land in
  `bench/results/`). `bench/baseline.txt` is a placeholder until it is regenerated with
  `mise run bench-baseline`; compare against it only once it holds measured numbers. A
  regression over 5% on a hot path, or any allocs/op increase, blocks review unless argued with
  numbers and accepted explicitly.
- Concurrent code gets a `-race` test at concurrency >= 2, not a serialized stand-in.

## Code style

- Go version is `go.mod`'s directive (pinned to the lowest patch; `.mise/config.toml` pins the
  toolchain that builds). Write idiomatic Go for that version: `slices`, `maps`, `cmp.Or`,
  `strings.Cut`, `for range N`, `min`/`max`, generics constrained on `manifest.BaseManifest`,
  `errors.AsType`. Run `go fix` after touching a package. No `interface{}`, naked returns,
  `ioutil`, or reflection on hot paths.
- `ctx context.Context` is the first parameter of anything that does I/O and is never stored
  in a struct. `context.Background()` belongs to `internal/cli`, except for documented shared
  work that must outlive one caller's cancellation, such as `TreeCache.FetchRemote`.
- Errors: `fmt.Errorf("<lowercase op>: %w", err)`; tag with a `manifest.Err*` sentinel as
  `fmt.Errorf("%w: detail", manifest.ErrInput)`. Every domain error wraps `manifest.ErrFlux`.
  Classify with `errors.Is`/`errors.AsType`, never by string. Panic only on impossible
  state, never on input or I/O.
- Logging is package-level `log/slog` (`slog.Debug/Warn`) with key-value attrs and a
  `"pkg: "` message prefix. No logger threading, no other logging library.
- Functions take at most four parameters (median is one); beyond that, an `Options`/`Config`
  struct. No functional options. Receivers are one or two letters, consistent per type.
- Every package has a `// Package` doc; exported identifiers are documented. Comments state
  constraints and rationale (MUST, never, invariant), not narration, and never reference past
  behavior or the current change.
- `CGO_ENABLED=0`; no cgo, ever.

## Tests

- Stdlib `testing` only; never testify or Ginkgo. Names are `TestThing_Behavior`,
  `TestE2E_*` (in-process via `cli.Run`, no build tags, no network), `BenchmarkThing_Case`
  with `b.Loop()`.
- Table-driven with `t.Run`; `t.Parallel` only where the package already uses it. Mark
  helpers with `t.Helper()`; use `t.Context()`.
- Fixtures: `t.TempDir()` plus `testutil.WriteFile`; shared corpora live in `testdata/<scenario>/`
  and are documented in `testdata/TESTDATA.md`. Assert with `internal/assert` or `cmp.Diff`
  (`-want +got`). No golden files.
- Every fix lands with a regression test for the reported symptom.

## Before a PR

```
mise run lint
go mod tidy && git diff --exit-code go.mod go.sum
mise run test        # go test -race, all packages including test/e2e
mise run vulncheck
mise run bench       # when touching a package that has benchmarks; paste benchstat vs bench/baseline.txt
```

CI runs the same plus a goreleaser snapshot and workflow lint; "Build Success" is the merge
gate behind a squash-only merge queue. `//nolint` needs a reason comment on the line.

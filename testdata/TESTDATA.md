# Testdata

This directory holds fixtures used by `pkg/` unit tests and the `test/e2e/`
integration suite. Fixtures stay here while tests run in scenario packages beneath
`test/e2e/`, using one shared `test/e2e/internal/harness` package. Run all scenarios
with `go test -race ./test/e2e/...`.

| Package | Scenarios |
| --- | --- |
| `.` | ArtifactGenerator strategies, bootstrap inputs, partial failure and emitted changed-only diff |
| `sourceref` | Source references, pinned trees and discovery |
| `scope` | Selection, ownership, orphans, diff and change scope |
| `helm` | Helm listing, builds, diagnostics and stale values |
| `crds` | ResourceSet expansion and CRD availability |
| `substitution` | Substitution producers, data reading and inherited patches |

## Layout

- `bootstrap-credentials/build-and-diff` compares complete build and diff output before and after a bootstrap source gains unavailable authentication and verification references.
  Both replays use two workers and fresh caches.

- `unclaimed-change/` - an offline Helm producer emits a Kustomization over unchanged content. A changed text file has no owner or reader. `TestE2E_UnclaimedPathWithoutHistory` requires empty changed-only output with separate cold caches.

- `simple/` - a hand-crafted minimal cluster used across the scenario packages.
  Exercises the Kustomization + HelmRelease pipeline end-to-end without any network
  access.
    - `cluster/` — Flux GitRepository, Kustomization, and HelmRelease objects.
    - `apps/` — kustomize-rendered ConfigMap + Namespace.
    - `charts/mychart/` — a tiny local Helm chart referenced via
      `sourceRef.kind: GitRepository`.

- `component-reach/` preserves shared Component changes and app-local changes through two namespaced consumers.

- `scope-orphans/` preserves namespace admission and orphan ownership.
  Its undiscovered-child case keeps one listed child and one unlisted declaration.
  Only the listed child renders.

- `resourceset-documents/` - ResourceSet JSON and YAML admission through SDK and CLI output: incomplete identities, nonrecursive list items, directives, duplicates, sanitized ciphertext with complete SOPS AES256_GCM framing, authored and explicit namespaces, and one tracked child render.

ArtifactGenerator CLI fixtures are created in temporary repositories by
`test/e2e/artifactgenerator_test.go`. `AllStrategies` combines nested directory
copying, YAML merging and a local gzip tar archive, then compares two CLI runs.
`PartialFailure` publishes outputs A and B before output C fails and checks that
their consumers still render. `BootstrapInput` selects a static generator when
only its consumer declaration changes. `BuiltBootstrapInput` patches a generator
copy input and UID in a bare Kustomize entry, then checks consumed output and
deterministic bytes. `EmittedChangedOnlyDiff` renders the generator from
a Component, changes its selected copy input, and checks both changed-only build
and forward/reverse diff with shallow and absent history on opposite sides,
without selecting an unrelated consumer. Each case runs with
two workers and compares two equal CLI runs. Consumed-input successor scheduling
remains deferred to X2 and is not asserted by these fixtures.

## Independent render model

`pkg/orchestrator/trees_model{,_render}_test.go` constructs bounded before/after
fixtures in temporary directories. `TestRenderTrees_IndependentModel` and
`FuzzRenderTrees_IndependentModel` share the byte decoder and four-side checker.
The first byte chooses a seed; the second chooses a feature variant, with its
high bit requesting directory comparison. Three-byte instructions compose
content, comment, rename, identity, namespace, path, suspension, unconsumed-file
and owner-removal edits. The `ff` prefix encodes the four metamorphic controls.
Unsupported combinations are counted. The decoder records replayable file and
producer operations, bounded to 12 producers, 48 files/documents, depth 4, three
namespaces, two sources, 4-KiB bodies and 24 operations. `0300` and `0301`
retain the optional-input directory and public committed-baseline witnesses.

Set `FLATE_E8_ARTIFACT_ROOT` to the build artifact directory to retain failing
bytes, readable worlds, operations, expected/actual identities and reproduction
argv. Diagnostic chart archives are rendered as hex. `TestRenderTrees_IndependentModel_Metamorphic`
checks disjoint growth, source renaming, reverse edits, equivalent placement and
two explicit successful replays. Fixture commits use go-git; no test subprocess
or shared generated fixture is required. The corpus includes pinned/alias and
second-source collisions with dirty-byte controls, local chart archives,
generators, components, ancestor-injected patches, owner/source/root/group moves,
ignore flips, empty output and unavailable data/CRDs. Shallow and missing-tree
controls assert baseline admission failure without claiming a completed render.
Future ResourceSet output semantics remain explicitly pending freeze.

## Upstream attribution

flate's controller architecture and behavior follow the design of
[`flux-local`](https://github.com/allenporter/flux-local). The flate
`testdata/simple` corpus is intentionally minimal and was authored from
scratch for the Go port; the larger `flux-local/tests/testdata/*` clusters
remain available in upstream for reference.

The `composed-owner-removal-orphan` input preserves native fuzz witness
`392c59303030303030`. Its ownerless package is eligible for static discovery;
a suspended or replacement owner retains ownership. Static store inventory and
selected output remain distinct, including source coordinates.
The `last-owner-local-chart` witness removes the final Kustomization while
changing a local Helm chart template; it retains the source alias and exact
HelmRelease admission expectations.

## E8 memory workloads

Routine tests are offline and run without environment overrides. Large fixtures
require `FLATE_E8_SLOW=1`; their normal skips mean `not-run`, never memory passes.
Fixtures use temporary directories and go-git. No test launches a process.
`internal/testutil/sparsegit.go` is shared fixture/counter support, not an SDK API.

| Test | Ordinary control | Explicit slow workload |
| --- | --- | --- |
| `TestRender_MemorySharedOwner/local` | Two fresh render instances, one memory cache owner, two producers/20 objects, identical output and one fill of a shared owner payload | None |
| `TestRender_MemorySharedOwner/home_plex` | Not-run | Pinned local `FLATE_E8_HOME`, `kubernetes/apps`, two fresh full SDK renders followed by public `HelmRelease media/plex` selection, one memory owner; label SDK startup/output encoding separately from the CLI row |
| `TestAcquire_MemoryRetirement` | Memory and disk backing, eight 32-KiB source files; warm reuse, refresh with retained old payload, result release/owner close, and bounded reacquisition | Same lifecycle with 64 one-MiB source files; run separately at default GOGC with `GOMEMLIMIT=128MiB` and `256MiB` |
| `TestAutoResolve_SparseGitMemory` | `/routine`: 24 committed files and a 4-KiB unchanged blob | `/exact`: 12,000 committed files and an identical unchanged 256-MiB blob, one changed consumed input, then disjoint committed growth |
| `TestSelection_SparseGitBodies` | `/routine`: calibrated comparison primitives on the same small fixture | `/exact`: same phases with exactly 12,000 files/256 MiB |
| `TestGitView_SparseBodyReads` | `/routine`: calibrated fresh pool before tree walk/shape | `/exact`: same phases with exactly 12,000 files/256 MiB |
| `TestRender_MemorySparseGit` | `/routine`: actual public AutoResolve inputs into RenderTrees, exact producer admission/terminal/output and disjoint-growth invariance | `/sparse-git`: same checks with exactly 12,000 files/256 MiB; two explicit render invocations before/after disjoint untracked growth |
| `TestAutoResolve_SparseHistoricalBodyUnavailable` | Remove the known unchanged committed blob while retaining checkout bytes; public admission and baseline reads still succeed | This fault injection is distinct from the intact 256-MiB row |
| `TestDetect_SparseDirectoryBodies` | Separate bounded directory comparison: 25 files per side, one same-size edit, 25 regular body opens per side | No large directory comparison substitutes for Git identity evidence |
| `TestRender_MemoryContained` | Not-run | Exactly 1,000 producers (500 Kustomizations and 500 HelmReleases), 10,000 ConfigMaps with 1,024-byte data payloads, shared Kustomize input/local Helm chart, concurrency 16; checks complete unique object and terminal producer membership |

The sparse fixture streams a sparse on-disk zero file through go-git's
`filesystem.Storage.LazyWriter` and `plumbing.NewHasher`, then stages that blob
identity in the Git index. It never builds a 256-MiB Go buffer. Both commits have
exactly the requested file count and the same unrelated blob identity. Fixture
construction/commit validation and Go test startup are harness overhead, outside
render metrics; a collector measuring the entire test child must label that cost
rather than subtract it or call it product-only RSS.

Reader counters wrap encoded objects, counting `Reader` opens separately for
metadata, all blobs and the known large blob. The gittree test installs a fresh
pool factory before any walk/shape operation. A small consumed blob calibrates
the actual pool path; counters reset before walk, shape/list/stat and consumed
read. The baseline test calibrates a separate test-local repository, resets for
baseEntries, candidates, committed comparison and dirty comparison, and never
passes a wrapped Storer to `selectChanges`. The public baseline inputs retain
concrete `*gittree.FS` policy dispatch. These are separate observations: public
AutoResolve admission, checkout hashing, filterChanges and render/fetch Reader
opens are not spanned by those counters and remain explicit instrumentation gaps.
Integration checks sibling exclusion and exact completed output; it does not
invent body counts for those gaps. The missing-historical-blob case supplies
independent public-route evidence without claiming intact large-row metrics.

Retention samples release render/results and close owners before explicit GC.
The cache test also reports a held-payload snapshot with `runtime.KeepAlive`.
Heap samples are process-wide observations, not attributed payload retention or
peak RSS, and have no global-equality assertion. Memory reacquires once after
close; disk source reuse restores the final payload without a new producer fill.
Source archive reuse is separate from persistent render-cache eligibility.

Focused routine proof (race instrumentation is separate from RSS collection):

```fish
go test -race ./pkg/orchestrator ./pkg/baseline ./pkg/source/gittree ./pkg/source/cache ./pkg/change -run 'Memory|Sparse|LazyBaseReads' -count=1 -v
```

Exact synthetic fixture correctness, one fixture at a time without reducing
reconcile concurrency:

```fish
env FLATE_E8_SLOW=1 go test ./pkg/baseline ./pkg/source/gittree ./pkg/orchestrator -run 'TestAutoResolve_SparseGitMemory/exact|TestSelection_SparseGitBodies/exact|TestGitView_SparseBodyReads/exact|TestRender_MemorySparseGit/sparse-git|TestRender_MemoryContained' -count=1 -v -p=1 -parallel=1
env -u GOGC FLATE_E8_SLOW=1 GOMEMLIMIT=128MiB go test ./pkg/source/cache -run '^TestAcquire_MemoryRetirement$' -count=1 -v -parallel=1
env -u GOGC FLATE_E8_SLOW=1 GOMEMLIMIT=256MiB go test ./pkg/source/cache -run '^TestAcquire_MemoryRetirement$' -count=1 -v -parallel=1
```

For the corpus SDK row the collector must supply `FLATE_E8_HOME` as an absolute
local copy at the pin below, set `FLATE_E8_SLOW=1`, and invoke
`go test ./pkg/orchestrator -run '^TestRender_MemorySharedOwner/home_plex$' -count=1 -v`.
Missing input on that explicit invocation fails. Pin/source/chart validation and
network availability are recorded by the external collector; the SDK test does
not infer corpus identity from the path. The SDK workload explicitly matches the verified CLI defaults for Git depth,
retry policy, concurrency, bundled Kubernetes version, test/CRD/Secret omission
and unreadable-key admission. It sets `Config.DisableChartDigestTracking=true`, matching
the home-ops inventory; the ordinary SDK does not read CLI environment flags.

The external row IDs below describe measurement regimes, not collected results.
The inventory source is the installed pack's `assets/corpus/corpus-invocations.json`.
Current inventory pins: home-ops `d2a8c47f99f2614003f037e455b374e7b616edd0`,
angelnu `872f8a1baebedfdcce40c580b509e038d0242ec1`,
Biohazard `4e4bb6180d6417990dc5f195852730ed77a3142f`.
E8-5 must verify these and the historical absolute-target options before verdicts.

| ID | Executable argv after the resolved binary, cwd/input and cache regime | Authority |
| --- | --- | --- |
| MEM-3-COLD | `build hr plex -n media -p kubernetes/apps --log-level debug --no-progress`; cwd pinned home-ops; fresh child, no cache-dir; `FLATE_DISABLE_CHART_DIGEST_TRACKING=true` | CPU <=2 s, peak RSS <=250 MiB after exact workload eligibility |
| MEM-3-WARM | SDK test above; separately the MEM-3-COLD argv plus `--cache-dir` and the explicit absolute persistent directory, reused across fresh children | Approximately 100 MiB objective; report distance, no hard 100-MiB cap; never label a second default-memory process warm |
| MEM-B11-ANGELNU | `build all -p clusters/prod --log-level debug --no-progress --cache-dir` plus explicit absolute persistent directory; cwd pinned angelnu, warm reused directory | <=261 MiB target requires verified exact historical pin/options |
| MEM-B11-GC128 | `TestAcquire_MemoryRetirement` slow invocation above with `GOMEMLIMIT=128MiB`, plus B11 pinned source/all-render invocation under the same pressure; default GOGC | Soft runtime budget, report RSS/held and released heap/reacquisition; synthetic retirement does not stand in for corpus RSS |
| MEM-B11-GC256 | Same as GC128 with `GOMEMLIMIT=256MiB` | Same soft-budget authority |
| MEM-ALL-HOME | `build all -p kubernetes/apps --log-level debug --no-progress`; cwd pinned home-ops, default product concurrency, explicitly record cold/reused cache; inventory chart-digest environment override | Named resource evidence; this small tests-only lane has no extra comparative performance gate |
| MEM-ALL-BIO | `build all -p kube/clusters/biohazard/flux --log-level debug --no-progress`; cwd pinned Biohazard, default product concurrency, explicit cache regime | Same named-evidence obligation |
| MEM-SPARSE | Exact sparse invocations above; measure each clean uninstrumented test child separately and retain phase assertions, dimensions and setup overhead | No unrelated historical blob body on observed commit paths, no unrelated selected producer; uncovered phases remain gaps |
| MEM-CONTAINED | Exact `TestRender_MemoryContained` child, concurrency 16; clean test binary and delegated Linux cgroup, swap off | Proposed process peak 1 GiB/cgroup 2 GiB; report process/cgroup accounting separately; do not shrink fixture or silently promote proposal to gate |

Each collector row resolves concrete binary/test-binary argv arrays, cwd,
environment overrides, cache directory/regime and clean stamped identity.
Default GOGC means remove any inherited override; pressure rows override only
GOMEMLIMIT. SDK binaries carry their own startup cost and cannot be called clean
product CLI measurements. Persistent render-cache claims require an eligible
clean stamped product binary, independently of the unstamped hash-equality proof.
The collector records CPU user/system, peak RSS bytes (normalize platform units),
wall time, exit/signal, stdout digest, available work counts and missing counts,
provenance and calibration. Linux containment records effective memory/swap
limits, cgroup peak/events/OOM and teardown; macOS cannot satisfy that row.
External collector/containment delivery is prerequisite D1, owned by the pack.
`rusage.py` alone is insufficient. This lane does not install or patch a collector;
E8-5 must record missing delivery/coverage rather than treating these invocations
or test skips as measured passes.


## E8 synchronized liveness scenarios

These fixtures are generated under `t.TempDir()` and remain offline. Routine
variants run with bounded task concurrency 2 and 16. Store listeners record
immutable observations; consumers park outside callbacks. Scheduler assertions
read terminal state only after structural completion and task drain. Barriers
hold only the first `min(concurrency, runnableRoots)` active dispatch bodies.

| Family | Routine tests and ordering assertions |
| --- | --- |
| Shared DAG | `TestScheduler_BoundedDAG`, `TestDAG_BoundedPublications`: 160 producers, ten distinct ConfigMaps each, depth 64 and a 64-way diamond. Active body counts use deferred release, reach the bound and never exceed it. Exact producer/child membership, Ready status and drain are asserted. The orchestrator renders two explicit independent outputs and compares their bytes. |
| Publication and replay | `TestStore_AddListenerNoFlush_NoMissedConcurrentWrites` distinguishes pre-registration, overlapping and promised post-registration writes. `TestStore_ListenerSnapshots` captures dispatch under the existing shard lock, unlocks, changes registration and dispatches old/fresh snapshots. `TestRecordEmitted_ParkedSnapshotReplay` holds consumers across replacement and withdrawal, checks retained old objects/conditions and fresh replay/current membership. |
| Keep before visibility | `TestEmit_OwnershipPrecedesArrivals`, `TestFilter_AddExtendsKeepSetAtRuntime`, `TestDAG_KeepBeforeLateDataPublication`: selection precedes transformed child arrivals; the child is observed Blocked before late data release; the provider is selected, the sibling stays excluded, and the child finishes with the expected data and no stale identity. |
| Shared fetch cancellation | `TestAcquire_SharedWaiterCancellation` attaches two consumers to one blocked producer, cancels a waiter and completes the survivor with one fill. `TestRemoteInput_CancelQueuedGitBase` prevents canceled queued work/reacquisition. `TestAcquire_CancelStopsContextIgnoringProducer` and `TestRenderTrees_CancellationJoinsBeforeCacheClose` hold producer cleanup, then join before closing memory/disk owners and reject canceled post-close reacquisition. |
| Failure and panic | `TestScheduler_RegisteredDependencyGraphs`, `TestScheduler_RegisteredFailureRecovery`, `TestDAGDispatcher_RecoveredAndPrimaryFailures` cover cycles, missing dependencies, failed declaration replacement, released pins and exact failure/blocker membership. `TestService_PanicCountedAndRecovered`, `TestYieldSlot_PanicRestoresSlot`, `TestScheduler_RunDispatchPanic` prove counted panic and uncanceled completion. `TestFinalize_DispatchPanic` feeds the same poisoned service into finalization and requires an error. Existing `TestE2E_MissingLocalInputDiagnostics` verifies ordinary `cli.Run` failure translation. |

Exact large variants are `TestScheduler_ScaleDAG` and
`TestDAG_ScalePublications`. Each keeps 1,000 producers, 10,000 publications,
depth 64 and 64-way diamond fan-in at both task bounds; an ordinary test skip
is not a large-campaign pass. Run the large race campaign with:

```sh
env FLATE_E8_SLOW=1 mise exec -- go test -race ./pkg/schedule ./pkg/orchestrator -run '^(TestScheduler_ScaleDAG|TestDAG_ScalePublications)$' -count=1
```

E8-4 runs the routine package race proof. E8-5 owns the exact large race
campaign, full repository gates and the final candidate commit.

## E6 synthetic reach fixtures

`e6-reach/` contains six small input graphs and one offline chart for `TestRenderTrees_Reach`.
Its README describes the eight edits, structural reach, independent output oracle, and seven fixed `NFS_MUSIC` users.
The test asserts both diff sides by identity, repeats cold requests, and bounds retained heap.
Parallel subtests share immutable fixture inputs.
The serial parent isolates process GC state and heap samples in the dedicated `test/reach` process.
U1.2 owns precision for the three recorded broad-reach rows.
Real repository inputs stay in the corpus and benchmark harness.


## Fixture scenarios

A scenario is a repository tree with declared edits and CLI invocations.
`internal/testfixture` owns all file copies, overlays, commits, inputs, and caches.
The end-to-end harness reads `scenario.json` and calls `cli.Run` for each invocation.
Tests use `harness.Open`, `Case.Want`, and two calls to `Case.Run`.
They compare each complete ordered result, including stdout, stderr, and the exit code.

`scenario.json` rejects unknown fields.
Its `source` records the repository, full revision, original paths, and reductions.
`repositories` names independent Git stores and their relative disk directories.
`commits` lists fixed authors, timestamps, parents, overlays, and refs in order.
`overlays` supplies complete replacement files and declared delete, rename, or mode operations.
`inputs` names supplied material outside the rendered repository.
`tokens` declares exact file sentinels that need runtime references before commit creation.
`files` assigns every file to named cases and states its purpose.
The schema guard rejects missing expectations, undeclared files, and empty successful Kustomize packages.

Each case declares initial `setup`, an optional working directory, environment values, a cache policy, and ordered `steps`.
Each step has a unique `id` and exactly one `edit` or `invoke`.
An edit names an overlay and never commits implicitly.
An invocation declares `argv`, optional environment values, optional cache selection, both expectation paths, and an integer exit code.
Priming commands have the same complete expectations as other invocations.
A failed invocation still returns a result and does not suppress later steps.

The default cache and all named caches belong to the case and live outside its repositories.
`invoke.cache.name` selects a named cache for that invocation only.
`invoke.cache.reset` empties only the selected cache before the invocation.
No invocation gains `--cache-dir` implicitly.
`case.cache.repeat` must be `reset` or `reuse`.
Both policies restore repositories, inputs, refs, remotes, and dirty setup at the same paths before every replay.
`reset` also empties all case caches and the isolated home directory.
`reuse` retains them after the first replay.
Two complete replays must produce identical results.

Runtime tokens are `@@repo@@`, `@@baseline@@`, `@@inputs@@`, `@@cache@@`, and `@@commit:name@@`.
Expectations resolve tokens against the same reserved paths and declared commits.
Actual output keeps its absolute paths.
The materializer never expands `${VAR}` or Helm templates.

### Expected output and runtime token grammar

Reviewed `expect/*.stdout` and `expect/*.stderr` files define complete bytes, including final newlines.
No tool creates, regenerates, or accepts expectations from candidate output.
Derive manifests from Flux behavior and the authored inputs.
Derive ordering and diagnostics from the flate encoder contract.
Review replacement expectations as source changes.

Only actual streams from `test` invocations receive the following duration substitution.
This includes bare `test`, `test all`, `test ks`, and `test hr`.
Apply the grammar to stdout and stderr without moving bytes between them.
The source is `.gc/pack/assets/corpus/corpus-parity.sh`, function `normalize`, expression `durations`:

```text
^([[:space:]]*(✓|✗) [0-9]+ passed( · [0-9]+ (skipped|failed|blocked))*[[:space:]]+)([0-9]+h)?([0-9]+m)?[0-9]+(\.[0-9]+)?(ns|µs|us|ms|s|m|h)$
```

Match each line without consuming its newline.
Replace only the duration with `<DUR>` through the Go replacement `${1}<DUR>`.
Preserve indentation, counts, separators, glyphs, and final newlines.
Malformed durations and summaries without durations remain unchanged.
Build, diff, get, and help output remains unchanged.
Diagnostics, resource values, paths, timestamps, and retry-after values remain unchanged.
Expected files already contain `<DUR>` and receive no normalization.

The expectation resolver replaces `<CONCURRENCY_DEFAULT>` with `runtime.NumCPU()*4`, which matches `internal/cli/bindCommon`.
This token represents the host-dependent default in command help.
Actual help output remains unchanged.
The resolver preserves unknown angle-bracket tokens.
The harness tests this token beside the `<DUR>` grammar.

### Isolation and transport

The harness clears inherited `FLATE_*`, proxy, and credential variables.
Each case owns its home directory, credentials, explicit caches, and OS cache.
Cleanup restores the environment, working directory, logging, and transports.
Declared HTTPS responses use `net.Pipe` without sockets or live endpoints.
The host list is closed, and cleanup joins connection workers.
The harness routes plain HTTP through a proxy whose name its resolver rejects.
Named hosts, non-loopback literal IP addresses, and HTTP redirects cannot reach a live endpoint off the machine.
Plain HTTP to localhost or a loopback IP remains outside this guarantee because upstream bypasses environment proxies for those addresses.
The loopback proof requires a failed connection without a fixture response.
Plain HTTP, DNS, credential helpers, AWS credential processes, and low-level transport proofs remain in package tests.
Native credential files contain synthetic values.
No end-to-end fixture launches a helper or a subprocess.

### U0 catalog

| Scenario / case | Behavior and commands | Cache regime |
| --- | --- | --- |
| `entry-root/literal-app` | `build ks app`: one complete ConfigMap with Flux ownership labels. | No disk persistence; reset on replay. |
| `entry-applicator/entry-application-rendered-fluxinstance` | FluxInstance application: prime twice, workload diff twice, warm interval diff twice, isolated interval diff twice, then path invalidation. | Default cache within a sequence; named `interval-cold`; reset on replay. |
| `entry-applicator/entry-application-rendered-kustomization` | Kustomization application: the same three edits and nine invocations. | Default cache within a sequence; named `interval-cold`; reset on replay. |

Both scenarios are authored reductions of `buroa/home-ops` revision `380ca7bc59d0e58e8d85bfcaa54d8bb7057e5f70`.
The observed paths are `kubernetes/apps/media/plex/ks.yaml` and `kubernetes/apps/flux-system/flux-instance/app/helmrelease.yaml`.
The fixtures retain source references and application paths, with local charts and ConfigMaps replacing application payloads.
The unapplied sibling is an authored exclusion control with a real ConfigMap.
The alternate application is an authored invalidation control with a distinct ConfigMap.
FluxInstance Git sync uses the full ref `refs/heads/main`, as required by Flux's source reference contract.

The approved plan's literal stdout example omitted Flux ownership labels.
The owner ruling corrects that example: `metadata.labels` includes `kustomize.toolkit.fluxcd.io/name: app` and `kustomize.toolkit.fluxcd.io/namespace: default`.
The encoder sorts those keys before `metadata.name` and `metadata.namespace`.
The expectations include both labels and derive from Flux application behavior.

`TestRenderTrees_EntryApplicationIntervalReceipt` retains interval, workload, receipt reuse, admission, and exclusion proofs for both application forms.
Its chart invalidation case counts the named generator through rendering.
The implicit fixture places its exclusion control outside the entry subtree because Flux includes nested packages during implicit generation.
Both disk scenarios use explicit entries.
The application entry lists `source.yaml`, `generator.yaml`, `workload.yaml`, and `cm.yaml`, and excludes `unapplied`.
Path invalidation preserves these declarations and the literal layer.
`TestRenderTrees_ExplicitEntryIntervalSelection` uses this disk fixture for complete output, work counts, and two-run determinism.

The FluxInstance end-to-end sequence remains not-run because its alternate application path renders no ConfigMap at the new path.
Discovery owns that defect.
Its alternate ConfigMap expectation remains present.
The implicit Kustomization receipt case remains not-run because warm admissions lose the application receipt.
Orchestrator receipts own that defect.
The explicit Kustomization warm selection case stays active with zero templates and one generator admission per side.
All held bodies and expectations remain present.
Every other assertion stays active, including every U2 witness.
`TestRender_EntryDeclarationQualifiesOpaqueApplier` retains qualification and receipt validity proofs.
Warm receipt evidence requires a clean VCS-stamped test build.
An unavailable persistence witness is not-run and does not satisfy the warm acceptance bar.

U1 through U9 retain their existing fixture helpers and unmigrated files until their assigned migrations.
The explicit allowlist lives in `test/e2e/internal/harness/migration_internal_test.go` and shrinks with each unit.
U0 does not replace other scenario data or package fixtures.

### U3 Helm catalog

The Helm render cases use authored reductions of the pinned toboshii HelmRelease topology.
The other scenarios record their observed repository, full revision, source paths, and authored reductions in `scenario.json`.
Expected manifests include Flux ownership labels.
Package OCI witnesses use `helm-oci/chart` and `helm-oci/repo`.
They retain selected and irrelevant layers, tag and digest tracking, credentials, and copy and extract operations.
The `simple` data remain for `scope/selection_test.go`, which U1 owns.
The stale-value and dependency cases use their shared scenarios after their last old callers migrate.

The three source-root values cases and both malformed-chart producer cases invoke the same build twice per sequence.
The second invocation reuses the populated default cache and compares the same complete expectations.
Both sequence replays start with empty caches.

| Scenario / case | Behavior | Cache regime |
| --- | --- | --- |
| `helm-render/buildhr-secret-excluded` | buildhr secret excluded. | Reuse within sequence; reset on replay. |
| `helm-render/buildhr-secret-included` | buildhr secret included. | Reuse within sequence; reset on replay. |
| `helm-render/malformed-chartcrd-omitted` | malformed chartcrd omitted. | Reuse within sequence; reset on replay. |
| `helm-render/malformed-chartcrd-included` | malformed chartcrd included. | Reuse within sequence; reset on replay. |
| `helm-render/malformed-chart-fails-producer-duplicate-keys` | malformed chart fails producer duplicate keys. | Reuse within sequence; reset on replay. |
| `helm-render/malformed-chart-fails-producer-typed-child` | malformed chart fails producer typed child. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-reported-strict` | helm source root values files reported strict. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-reported-ignore-missing` | helm source root values files reported ignore missing. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-outside-chart` | helm source root values files outside chart. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-template-edit` | helm source root values files changed only template edit. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-template-add` | helm source root values files changed only template add. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-template-delete` | helm source root values files changed only template delete. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-template-strict-delete` | helm source root values files changed only template strict delete. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-chartref-edit` | helm source root values files changed only chartref edit. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-chartref-add` | helm source root values files changed only chartref add. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-chartref-delete` | helm source root values files changed only chartref delete. | Reuse within sequence; reset on replay. |
| `chart-inputs/helm-source-root-values-files-changed-only-chartref-strict-delete` | helm source root values files changed only chartref strict delete. | Reuse within sequence; reset on replay. |
| `helm-identity/gethr-yaml-exposes-projected-fields-target-namespace` | gethr yaml exposes projected fields target namespace. | Reuse within sequence; reset on replay. |
| `helm-identity/gethr-yaml-exposes-projected-fields-explicit-override` | gethr yaml exposes projected fields explicit override. | Reuse within sequence; reset on replay. |
| `helm-identity/helm-release-target-namespace-identity` | helm release target namespace identity. | Reuse within sequence; reset on replay. |
| `helm-documents/invalid-helm-list-included` | invalid helm list included. | Reuse within sequence; reset on replay. |
| `helm-documents/invalid-helm-list-excluded` | invalid helm list excluded. | Reuse within sequence; reset on replay. |
| `helm-documents/helm-list-member-diff-added` | helm list member diff added. | Reuse within sequence; reset on replay. |
| `helm-documents/helm-list-member-diff-removed` | helm list member diff removed. | Reuse within sequence; reset on replay. |
| `helm-values/unreadable-parent-does-not-fill-required-values-full` | unreadable parent does not fill required values full. | Reuse within sequence; reset on replay. |
| `helm-values/unreadable-parent-does-not-fill-required-values-changed-only` | unreadable parent does not fill required values changed only. | Reuse within sequence; reset on replay. |
| `helm-values/unreadable-parent-does-not-fill-required-values-diff-current` | unreadable parent does not fill required values diff current. | Reuse within sequence; reset on replay. |
| `helm-values/unreadable-parent-does-not-fill-required-values-diff-orig` | unreadable parent does not fill required values diff orig. | Reuse within sequence; reset on replay. |
| `helm-values/stale-values-warning` | stale values warning. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-onlyhr-depends-on-unchanged-dep` | changed onlyhr depends on unchanged dep. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-only-emitted-kustomization-path-templated` | changed only emitted kustomization path templated. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-only-emitted-kustomization-path-templated-root` | changed only emitted kustomization path templated root. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-only-emitted-kustomization-path-literal` | changed only emitted kustomization path literal. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-only-emitted-kustomization-path-literal-root` | changed only emitted kustomization path literal root. | Reuse within sequence; reset on replay. |
| `helm-dependencies/changed-only-render-emitted-dependency` | changed only render emitted dependency. | Reuse within sequence; reset on replay. |
| `helm-dependencies/ready-expr--helm-release` | ready expr  helm release. | Reuse within sequence; reset on replay. |
| `helm-oci/helm-missingoci-source` | helm missingoci source. | Reuse within sequence; reset on replay. |

### U6 ArtifactGenerator catalog

| Scenario / case | Behavior and commands | Cache regime |
| --- | --- | --- |
| `artifact-copy/artifact-generator--all-strategies` | `build all`: directory copy, YAML merge with a retained key, and archive extraction overwrite the copied payloads. | Two invocations share the default cache. Replays retain caches. |
| `artifact-copy/artifact-generator--partial-failure` | `build all`: outputs a and b publish once. Missing input c fails with exit 1 and blocks its consumer. | Two invocations share the default cache. Replays retain caches. |
| `artifact-reach/artifact-generator--bootstrap-input` | `build all`: a missing source aliases the working tree. Both repositories pin identical copied inputs. A consumer-only edit still selects its static generator. | Full and changed-only builds share the default cache. Replays retain caches. |
| `artifact-reach/artifact-generator--emitted-changed-only-diff` | Changed-only `build all` and both directions of `diff all`: a component emits the generator. Its changed copy input reaches the consumer and excludes the unrelated workload. | Each invocation shares the default cache. Replays retain caches. |
| `artifact-reach/artifact-generator--input-source-changed-only` | Full builds resolve both pinned inputs. Changed-only builds and both diff directions cover independent directory baselines and committed baselines. | Each invocation shares the default cache. Replays retain caches. |
| `artifact-reach/artifact-generator--input-source-changed-only-reverse-committed` | `diff all`: the before checkout owns both commits and compares with the after commit. | The default cache survives both replays. |
| `artifact-convergence/artifact-generator--consumed-input-convergence` | `build all`: entry patches replace the raw generator input and UID. The final consumer and ExternalArtifact appear once with the current metadata. | Two invocations share the default cache. Replays retain caches. |

These scenarios are authored reductions of `xunholy/k8s-gitops` revision `2f2e6edc730457a3f45d7a73aad536afc683b5eb`.
The reference path is `kubernetes/apps/base/flux-system/artifact-generator/artifactgenerator.yaml`.
The reduction keeps the source, copy, artifact, and consumer relationships and replaces application payloads with local ConfigMaps.
Merge, extract, missing input, bootstrap, pinned history, and convergence are authored variants of the existing flate regression tests.
The unrelated workload is an exclusion control with a real ConfigMap.
The copied archive payload and merge target also exist before generation, so each source package builds independently.
The generator overwrites those payloads with the independently specified final values.
The component uses the standard `Kustomization` filename and renders through its owner package.

Flux source-watcher `v2.3.1` supplies the behavior authority through `ArtifactGeneratorReconciler.reconcile`, `fetchSources`, and `reconcileExternalArtifact`.
Its `mergeYAML` and `extractTarball` define the merge and extraction results.
Flux kustomize-controller `v1.10.1` supplies transformations and ownership labels through `KustomizationReconciler.build` and `apply`.
Flate supplies the offline aliases, changed-only selection, diff presentation, and failure diagnostics.
Expectation files specify complete bytes and exit codes independently of candidate output.

Every U6 sequence runs twice with concurrency 2.
The pinned-input case keeps separate Git stores for directory comparisons and selects both historical commits through the declared fixture transport.
Its committed reverse diff uses a separate declared checkout in the repository that owns both historical commits.
The emitted-generator case keeps a declared shallow boundary on the baseline repository.
The package tests under `pkg/controllers/artifactgenerator` retain parser, archive, partial publication, and synchronized convergence proofs.
U6 removes both ArtifactGenerator files from the migration allowlist.
The shared CLI and fixture helpers remain for the unmigrated U1 through U5 and U7 consumers until U9 removes their last callers.

### ResourceSet and CRD scenarios

`resourceset` follows `angelnu/k8s-gitops` at `872f8a1baebedfdcce40c580b509e038d0242ec1`.
The source paths are `apps/paperless/app/resourceSet.yaml`.
Authored static tenant expansion and generated-data controls. Keep the static input/template topology; replace workloads with ConfigMaps.

`resourceset/resource-set-expands-into-child-kustomizations` covers resource set expands into child kustomizations.
Its declared commands are `get ks -o`, `build all -o`, `test all --path`.
`resourceset/resource-set-generated-data-all` covers resource set generated data all.
Its declared commands are `build ks app`.
`resourceset/resource-set-generated-data-only-explicit-skips` covers resource set generated data only explicit skips.
Its declared commands are `build ks app`.
`resourceset/resource-set-generated-data-only-implicit-skips` covers resource set generated data only implicit skips.
Its declared commands are `build ks app`.
`resourceset/resource-set-generated-data-defaults` covers resource set generated data defaults.
Its declared commands are `build ks app`.
`resourceset/resource-set-generated-data-skip-data` covers resource set generated data skip data.
Its declared commands are `build ks app`.
`resourceset/resource-set-generated-data-encrypted` covers resource set generated data encrypted.
Its declared commands are `build ks app`.

`resourceset-documents` follows `billimek/k8s-gitops` at `127e039d8c739d748e20415dd3fdcaff91b4cb2d`.
The source paths are `kubernetes/kube-system/nfs-scalers/resourceset-nfs-scalers.yaml`.
Copy the static app input and templated output shape with two ConfigMaps in place of HPAs. Document, metadata and malformed-member variants are authored controls, not copied upstream content.

`resourceset-documents/resource-set-document-boundary` covers resource set document boundary.
Its declared commands are `build all -o`, `diff all -o`.
`resourceset-documents/controller-metadata-ownership` covers controller metadata ownership.
Its declared commands are `build all -o`.
`resourceset-documents/billimek-scalers` covers billimek scalers.
Its declared commands are `build all -o`.

`resourceset-provider` follows `xunholy/k8s-gitops` at `2f2e6edc730457a3f45d7a73aad536afc683b5eb`.
The source paths are `kubernetes/apps/base/preview-system/app/resourcesetinputprovider.yaml`.
Authored offline provider variant of the static fan-out. Keep the provider reference and replace the remote payload with two static tenant inputs.

`resourceset-provider/provider-warnings` covers provider warnings.
Its declared commands are `get ks -o`, `build all -o`, `test all --path`.

`crd-admission` follows `toboshii/home-ops` at `b16119e828316df78f73d378bf7c386bad9dac12`.
The source paths are `cluster/apps/database/cloudnative-pg/helm-release.yaml`, `cluster/apps/default/hajimari/helm-release.yaml`.
Authored CRD admission controls around the existing flux-operator ResourceSet. Retain suspended chart/source children. No upstream chart or CRD distribution is copied.

`crd-admission/missing-crds-strict-issue` covers missing crds strict issue.
Its declared commands are `test all --path`.
`crd-admission/resource-set-runtime-class-dependency` covers resource set runtime class dependency.
Its declared commands are `test all --path`.
`crd-admission/policy-reference-crd` covers policy reference crd.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-reference-crd-opt-in` covers policy reference crd opt in.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-strict-absent` covers policy strict absent.
Its declared commands are `test all --path`.
`crd-admission/policy-allowed-absent` covers policy allowed absent.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-supplied-crd` covers policy supplied crd.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-supplied-crd-opt-in` covers policy supplied crd opt in.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-strict-absent-explicit-crd` covers policy strict absent explicit crd.
Its declared commands are `test all --path`.
`crd-admission/policy-allowed-absent-explicit-crd` covers policy allowed absent explicit crd.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/policy-missing-configmap` covers policy missing configmap.
Its declared commands are `test all --path`.
`crd-admission/policy-missing-secret` covers policy missing secret.
Its declared commands are `test all --path`.
`crd-admission/policy-missing-widget` covers policy missing widget.
Its declared commands are `test all --path`.
`crd-admission/policy-supplied-configmap` covers policy supplied configmap.
Its declared commands are `test all --path`, `get hr -o`, `build all -o`.
`crd-admission/missing-crds-environment` covers missing crds environment.
Its declared commands are `test all --path`.
`crd-admission/missing-crds-deterministic-warnings` covers missing crds deterministic warnings.
Its declared commands are `test all --path`, `build all -o`, `get hr -o`.
`crd-admission/missing-crds-diff-sides` covers missing crds diff sides.
Its declared commands are `diff all -o`.
`crd-admission/selection-strict-absent` covers selection strict absent.
Its declared commands are `get hr -o`.
`crd-admission/selection-allowed-absent` covers selection allowed absent.
Its declared commands are `get hr -o`, `build all -o`.
`crd-admission/selection-present` covers selection present.
Its declared commands are `get hr -o`, `build all -o`.
`crd-admission/selection-foreign-group` covers selection foreign group.
Its declared commands are `get hr -o`.

Each sequence runs twice at concurrency two or greater.
Cases restore their inputs and reset their isolated caches before each replay.
These cases omit `--cache-dir` and claim no disk render persistence.
Expected manifests follow Flux ownership and namespace rules.
Diagnostics and command selection follow flate policy.
The document sequence retains malformed lists, duplicate identities, child suppression, and both diff sides.
The orchestrator package proof reads the same materialized document scenario and retains its effective identity assertions.

`resourceset-provider/static-provider` covers static provider.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/selector-provider` covers selector provider.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/flatten-provider` covers flatten provider.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/permute-provider` covers permute provider.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/supplied-provider` covers supplied provider.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/oci-tags` covers oci tags.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/invalid-template` covers invalid template.
Its declared command is `build ks cluster-tenants`.

`resourceset-provider/invalid-selector` covers invalid selector.
Its declared command is `build ks cluster-tenants`.

## Substitution and dependency corpus

The U5 cases preserve substitution, supplied inputs, and dependencies. Expected documents follow Flux rendering and ownership rules. Selection and unreadable-data behavior follow flate policy. Each case compares complete streams and exit codes on two replays.

| Case | Preserved behavior |
| --- | --- |
| `substitute-cycles/substitution-producer-cycle-concurrency-1` | `TestE2E_SubstitutionProducerCycle/concurrency_1` |
| `substitute-cycles/substitution-producer-cycle-concurrency-2` | `TestE2E_SubstitutionProducerCycle/concurrency_2` |
| `substitute-cycles/substitution-producer-cycle-concurrency-8` | `TestE2E_SubstitutionProducerCycle/concurrency_8` |
| `substitute-cycles/substitution-cycle-downstream-concurrency-2` | `TestE2E_SubstitutionCycleDownstream/concurrency_2` |
| `substitute-cycles/substitution-cycle-downstream-concurrency-8` | `TestE2E_SubstitutionCycleDownstream/concurrency_8` |
| `substitute-cycles/dependency-cycle-warnings-build` | `TestE2E_DependencyCycleWarnings/build` |
| `substitute-cycles/dependency-cycle-warnings-test` | `TestE2E_DependencyCycleWarnings/test` |
| `dependency-readiness/depends-on-defaults--supplied-values-prefix-config` | `TestE2E_DependsOnDefaults_SuppliedValues/${PREFIX:-}config` |
| `dependency-readiness/depends-on-defaults--supplied-values-prefix-dev-config` | `TestE2E_DependsOnDefaults_SuppliedValues/${PREFIX:-dev-}config` |
| `dependency-readiness/depends-on-defaults--changed-only-build` | `TestE2E_DependsOnDefaults_ChangedOnly/build` |
| `dependency-readiness/depends-on-defaults--changed-only-diff` | `TestE2E_DependsOnDefaults_ChangedOnly/diff` |
| `dependency-readiness/depends-on-defaults--emitted-child-default-only` | `TestE2E_DependsOnDefaults_EmittedChild/default-only` |
| `dependency-readiness/depends-on-defaults--emitted-child-discovered-supplier` | `TestE2E_DependsOnDefaults_EmittedChild/discovered-supplier` |
| `dependency-readiness/depends-on-defaults--empty-name-fails` | `TestE2E_DependsOnDefaults_EmptyNameFails` |
| `dependency-readiness/ready-expr--kustomization` | `TestE2E_ReadyExpr_Kustomization` |
| `dependency-readiness/ready-expr--kustomization-build` | `TestE2E_ReadyExpr_Kustomization/build` |
| `dependency-readiness/ready-expr--kustomization-diff` | `TestE2E_ReadyExpr_Kustomization/diff` |
| `dependency-readiness/ready-expr--kustomization-test` | `TestE2E_ReadyExpr_Kustomization/test` |
| `dependency-readiness/ready-expr--emitted-indexed-false` | `TestE2E_ReadyExpr_EmittedIndexed/false` |
| `dependency-readiness/ready-expr--emitted-indexed-true` | `TestE2E_ReadyExpr_EmittedIndexed/true` |
| `dependency-readiness/depends-on-produced-source-interval` | `TestE2E_DependsOnProducedSource/interval` |
| `dependency-readiness/depends-on-produced-source-interval-build` | `TestE2E_DependsOnProducedSource/interval/build` |
| `dependency-readiness/depends-on-produced-source-interval-diff` | `TestE2E_DependsOnProducedSource/interval/diff` |
| `dependency-readiness/depends-on-produced-source-substitution` | `TestE2E_DependsOnProducedSource/substitution` |
| `dependency-readiness/depends-on-produced-source-substitution-build` | `TestE2E_DependsOnProducedSource/substitution/build` |
| `dependency-readiness/depends-on-produced-source-substitution-diff` | `TestE2E_DependsOnProducedSource/substitution/diff` |
| `dependency-readiness/depends-on-produced-target-interval` | `TestE2E_DependsOnProducedTarget/interval` |
| `dependency-readiness/depends-on-produced-target-interval-build` | `TestE2E_DependsOnProducedTarget/interval/build` |
| `dependency-readiness/depends-on-produced-target-interval-diff` | `TestE2E_DependsOnProducedTarget/interval/diff` |
| `dependency-readiness/depends-on-produced-target-substitution` | `TestE2E_DependsOnProducedTarget/substitution` |
| `dependency-readiness/depends-on-produced-target-substitution-build` | `TestE2E_DependsOnProducedTarget/substitution/build` |
| `dependency-readiness/depends-on-produced-target-substitution-diff` | `TestE2E_DependsOnProducedTarget/substitution/diff` |
| `supplied-inputs/data-reader--suppliedssh-on-both-diff-sides` | `TestE2E_DataReader_SuppliedSSHOnBothDiffSides` |
| `supplied-inputs/data-reader--string-data-precedence` | `TestE2E_DataReader_StringDataPrecedence` |
| `supplied-inputs/data-reader--supplied-source-produces-credential` | `TestE2E_DataReader_SuppliedSourceProducesCredential` |
| `supplied-inputs/data-reader--cluster-advisories-on-both-base-sides` | `TestE2E_DataReader_ClusterAdvisoriesOnBothBaseSides` |
| `substitute-values/parent-patches-propagate-to-children` | `TestE2E_ParentPatchesPropagateToChildren` |
| `substitute-values/substitute-disabled-annotation` | `TestE2E_SubstituteDisabledAnnotation` |
| `substitute-values/substitution-precedence-build` | `TestE2E_SubstitutionPrecedence/build` |
| `substitute-values/substitution-precedence-test` | `TestE2E_SubstitutionPrecedence/test` |
| `substitute-values/unapplied-package-patches-unapplied` | `TestE2E_UnappliedPackagePatches/unapplied` |
| `substitute-values/unapplied-package-patches-applied` | `TestE2E_UnappliedPackagePatches/applied` |
| `substitute-values/literal-kustomization-path-full-build` | `TestE2E_LiteralKustomizationPath/full-build` |
| `substitute-values/literal-kustomization-path-own-postbuild` | `TestE2E_LiteralKustomizationPath/own-postBuild` |
| `substitute-values/literal-kustomization-path-changed-only` | `TestE2E_LiteralKustomizationPath/changed-only` |
| `substitute-values/literal-kustomization-path-paired-diff` | `TestE2E_LiteralKustomizationPath/paired-diff` |
| `substitute-values/literal-kustomization-path-reverse-diff` | `TestE2E_LiteralKustomizationPath/reverse-diff` |
| `substitute-producers/bootstrap-generator-env-and-file-substitution` | `TestE2E_BootstrapGeneratorEnvAndFileSubstitution` |
| `substitute-producers/substitution-producer-rendering-configmap-full` | `TestE2E_SubstitutionProducerRendering/ConfigMap/full` |
| `substitute-producers/substitution-producer-rendering-configmap-direct` | `TestE2E_SubstitutionProducerRendering/ConfigMap/direct` |
| `substitute-producers/substitution-producer-rendering-configmap-inherited` | `TestE2E_SubstitutionProducerRendering/ConfigMap/inherited` |
| `substitute-producers/substitution-producer-rendering-configmap-env` | `TestE2E_SubstitutionProducerRendering/ConfigMap/env` |
| `substitute-producers/substitution-producer-rendering-configmap-path-orig` | `TestE2E_SubstitutionProducerRendering/ConfigMap/path-orig` |
| `substitute-producers/substitution-producer-rendering-configmap-diff` | `TestE2E_SubstitutionProducerRendering/ConfigMap/diff` |
| `substitute-producers/substitution-producer-rendering-secret-full` | `TestE2E_SubstitutionProducerRendering/Secret/full` |
| `substitute-producers/substitution-producer-rendering-secret-direct` | `TestE2E_SubstitutionProducerRendering/Secret/direct` |
| `substitute-producers/substitution-producer-rendering-secret-inherited` | `TestE2E_SubstitutionProducerRendering/Secret/inherited` |
| `substitute-producers/substitution-producer-transformed-namespace-configmap-full` | `TestE2E_SubstitutionProducerTransformedNamespace/ConfigMap/full` |
| `substitute-producers/substitution-producer-transformed-namespace-configmap-changed` | `TestE2E_SubstitutionProducerTransformedNamespace/ConfigMap/changed` |
| `substitute-producers/substitution-producer-transformed-namespace-secret-full` | `TestE2E_SubstitutionProducerTransformedNamespace/Secret/full` |
| `substitute-producers/substitution-producer-transformed-namespace-secret-changed` | `TestE2E_SubstitutionProducerTransformedNamespace/Secret/changed` |
| `substitute-sops/data-reader--generator-embedded-ciphertext-secretgenerator` | `TestE2E_DataReader_GeneratorEmbeddedCiphertext/secretGenerator` |
| `substitute-sops/data-reader--generator-embedded-ciphertext-configmapgenerator` | `TestE2E_DataReader_GeneratorEmbeddedCiphertext/configMapGenerator` |
| `substitute-sops/sops-unreadable-active-substitution` | `TestE2E_SOPSUnreadableActiveSubstitution` |
| `substitute-sops/sops-unreadable-empty-variable-set-default-inactive` | `TestE2E_SOPSUnreadableEmptyVariableSet/default_inactive` |
| `substitute-sops/sops-unreadable-empty-variable-set-default-skipped` | `TestE2E_SOPSUnreadableEmptyVariableSet/default_skipped` |
| `substitute-sops/sops-unreadable-empty-variable-set-active-inline` | `TestE2E_SOPSUnreadableEmptyVariableSet/active_inline` |
| `substitute-sops/sops-unreadable-empty-variable-set-active-readable` | `TestE2E_SOPSUnreadableEmptyVariableSet/active_readable` |
| `substitute-sops/sops-unreadable-empty-variable-set-always-empty` | `TestE2E_SOPSUnreadableEmptyVariableSet/always_empty` |
| `substitute-sops/sops-unreadable-empty-variable-set-strict-active` | `TestE2E_SOPSUnreadableEmptyVariableSet/strict_active` |
| `substitute-sops/sops-unreadable-empty-variable-set-resource-disabled` | `TestE2E_SOPSUnreadableEmptyVariableSet/resource_disabled` |
| `substitute-sops/sops-unreadable-empty-variable-set-dummy-supply` | `TestE2E_SOPSUnreadableEmptyVariableSet/dummy_supply` |
| `substitute-sops/sops-unreadable-empty-variable-set-mixed-emitted` | `TestE2E_SOPSUnreadableEmptyVariableSet/mixed_emitted` |
| `substitute-sops/sops-unreadable-empty-variable-set-mixed-skipped` | `TestE2E_SOPSUnreadableEmptyVariableSet/mixed_skipped` |
| `substitute-sops/sops-unreadable-empty-variable-set-no-postbuild` | `TestE2E_SOPSUnreadableEmptyVariableSet/no_postBuild` |
| `substitute-sops/sops-unreadable-diff-both-directions-ciphertext` | `TestE2E_SOPSUnreadableDiffBothDirections/ciphertext` |
| `substitute-sops/sops-unreadable-diff-both-directions-plaintext` | `TestE2E_SOPSUnreadableDiffBothDirections/plaintext` |
| `substitute-sops/sops-unreadable-diff-both-directions-metadata` | `TestE2E_SOPSUnreadableDiffBothDirections/metadata` |

`helm-dependencies/changed-onlyhr-depends-on-unchanged-dep` preserves the shared Helm dependency consumer, merged in U3. Its unchanged dependency producer remains skipped.

The supplied SSH corpus case compares both full builds and the supplied paired diff. The absent-agent branch stays in `internal/cli/TestRun_SuppliedSSHSnapshotOnBothDiffSides` with every original assertion. `pkg/source/git/TestFetcher_SSHAcquisitionsOnBothSides` also requires zero authenticated acquisitions for an absent agent. The native-agent proof uses real package-test sockets. The corpus uses only declared Git URLs and generated test keys.

The ancestor-patch cases follow `bjw-s-labs/home-ops` at `656b3515052fcdb0450ca56151809852e383200d`, `kubernetes/flux/cluster/ks.yaml`. They retain the parent patch and child ownership structure. Local ConfigMaps replace unrelated workloads. The dependency cases follow `truxnell/home-cluster` at `0f7b47a9fec9419a4c5d6b5c4a4ae219ad342c1c`, `kubernetes/hegira/apps/cert-manager/cert-manager/ks.yaml`. The readiness expressions, emitted sources, and negative variants are authored reductions.

Expected manifests use Flux `Generator.WriteFile` and `SecureBuild` from `github.com/fluxcd/pkg/kustomize`. Flux removes newlines from substitution values before expansion. Expected labels identify the applying Kustomization. Source admission, output selection, ciphertext removal, and diagnostic routing use flate policy. All expectations are source changes, never accepted CLI output.

## Command, format, and flag corpus

These authored controls follow the app and source shape in `bjw-s-labs/home-ops` at `656b3515052fcdb0450ca56151809852e383200d`.
The source path is `kubernetes/flux/cluster/ks.yaml`.
Local ConfigMaps and a tiny chart replace the workloads.
The image case adds tagged, digest, bare, and init-container references.
The Secret contains synthetic test data.

Expected objects follow Flux Kustomization ownership labels and Helm template behavior.
Expected presentation follows `internal/output`, the command definitions in `internal/cli`, and Cobra help.
The cases compare complete streams and integer exit codes twice.
Each render uses concurrency 2 except the explicit zero and negative controls.
The default working-directory case omits `--path`.
Cases use isolated memory caches unless an invocation explicitly names a disk cache.
No case claims persisted render reuse.

The 17 filter cases replace `TestE2E_SkipKindsAppliesToKustomizationOutput` and delete its last fixture consumer.
The CLI cases replace pure help, environment precedence, and output-rejection assertions from `internal/cli`.
Private flag state, debug timestamps, TTY progress, DNS, profiles, and low-level transport proofs remain in their package tests.
The harness keeps its documented loopback exclusion.

| Case | Declared commands |
| --- | --- |
| `output-formats/build-commands` | `build -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build all -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build all -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build ks -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build hr -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build hr -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`build helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`build helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o json` |
| `output-formats/get-commands` | `get -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get all -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get all -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get all -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get all -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get hr -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get hr -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get hr -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get hr -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o name`<br>`get images -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get images -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get images -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get images -p @@repo@@/cluster --concurrency 2 --no-progress -o name` |
| `output-formats/test-commands` | `test -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test all -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test ks -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test hr -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test hr app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/diff-commands` | `diff -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff hr -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff kustomization app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff kustomizations app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff helmrelease app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o human --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o github --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o diff --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o html --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o brief --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitlab --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o gitea --base base`<br>`diff helmreleases app -p @@repo@@/cluster --concurrency 2 --no-progress -o json --base base` |
| `output-formats/diff-all-changes` | `diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o human`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o github`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o diff`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o html`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o brief`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitlab`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitea`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/diff-ks-changes` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o human`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o github`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o diff`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o html`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o brief`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitlab`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitea`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/diff-hr-changes` | `diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o human`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o github`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o diff`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o html`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o brief`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitlab`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o gitea`<br>`diff hr app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/filter-defaults` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/filter-empty-skip` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds ` |
| `output-formats/filter-replacement-skip` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds ConfigMap` |
| `output-formats/filter-only-crds` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds CustomResourceDefinition` |
| `output-formats/filter-only-secret` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds Secret` |
| `output-formats/filter-only-multiple` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds ConfigMap,Secret` |
| `output-formats/filter-empty-only` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds ` |
| `output-formats/filter-exact-kind` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds secret` |
| `output-formats/filter-exact-exclusion` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds secret,customresourcedefinition` |
| `output-formats/filter-env-empty-only` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/filter-argv-overrides-empty-env-skip` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds Secret,CustomResourceDefinition` |
| `output-formats/filter-explicit-skip-wins` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds Secret,CustomResourceDefinition --only-kinds CustomResourceDefinition` |
| `output-formats/filter-env-skip-wins` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds CustomResourceDefinition` |
| `output-formats/filter-env-only` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/filter-env-empty-skip` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/filter-argv-overrides-env-skip` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --skip-kinds ` |
| `output-formats/filter-argv-empty-only-overrides-env` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --only-kinds ` |
| `output-formats/diff-images` | `diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o name`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o yaml`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o name --include-removed`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o yaml --include-removed`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json --include-removed` |
| `output-formats/env-include-removed` | `diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base` |
| `output-formats/include-removed-precedence` | `diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base --include-removed=false` |
| `output-formats/changed-only-build` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base` |
| `output-formats/member-added` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o diff`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/member-removed` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base added -o diff`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base added -o json` |
| `output-formats/suspended-current` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o human`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/suspended-orig` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base suspended -o human`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base suspended -o json` |
| `output-formats/partial-failure` | `build all -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test all -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `output-formats/failed-current` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `output-formats/failed-orig` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base failed -o json` |
| `cli-flags/render-flags-1` | `build ks app --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --concurrency 0`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --concurrency -1`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --log-level error`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --log-level info`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --log-level warn`<br>`build ks app -p @@repo@@/cluster --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --git-depth 0`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --restrict-egress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --strict-substitutions`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress --enable-dns`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --disable-chart-digest-tracking`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --allow-missing-kinds Secret,ConfigMap`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --registry-config @@inputs@@/registry.json`<br>`build ks app --path @@repo@@/cluster --namespace apps --output yaml --concurrency 2 --no-progress` |
| `cli-flags/environment-bindings-2` | `build ks app --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `cli-flags/argv-precedence-3` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress -n apps`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --git-depth 0`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --restrict-egress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --strict-substitutions`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress --enable-dns`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --disable-chart-digest-tracking`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --allow-missing-kinds Secret,ConfigMap`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --registry-config @@inputs@@/registry.json`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --log-level warn`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml` |
| `cli-flags/parser-errors-4` | `test -p @@repo@@/missing -o json`<br>`test all -p @@repo@@/missing -o json`<br>`test ks -p @@repo@@/missing -o json`<br>`test hr -p @@repo@@/missing -o json`<br>`build frobnicate`<br>`build ks app extra`<br>`get frobnicate`<br>`get ks app extra`<br>`test frobnicate`<br>`test ks app extra`<br>`diff frobnicate`<br>`diff ks app extra`<br>`frobnicate`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --log-level invalid`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/missing --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/app/configmap.yaml --concurrency 2 --no-progress`<br>`build ks app -p  --concurrency 2 --no-progress`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress --base base -P @@repo@@/cluster`<br>`build all -o unsupported`<br>`build all -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`get all -o unsupported`<br>`get all -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`diff all -o unsupported`<br>`diff all -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2`<br>`build ks app -p @@repo@@/cluster --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`diff images -p @@repo@@/cluster --concurrency 2 --no-progress --base base`<br>`get ks missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`get hr missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build hr missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test ks missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`test hr missing -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`get all -l app=selected`<br>`list`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --allow-missing-kinds RuntimeClass`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2`<br>`build ks app -p @@repo@@/cluster --concurrency 2` |
| `cli-flags/help-and-version` | `--help`<br>`build --help`<br>`build all --help`<br>`build ks --help`<br>`build hr --help`<br>`get --help`<br>`get all --help`<br>`get ks --help`<br>`get hr --help`<br>`get images --help`<br>`test --help`<br>`test all --help`<br>`test ks --help`<br>`test hr --help`<br>`diff --help`<br>`diff all --help`<br>`diff ks --help`<br>`diff hr --help`<br>`diff images --help`<br>`cache --help`<br>`cache gc --help`<br>`-h`<br>`--version`<br>`-v`<br>`completion --help`<br>`completion bash --help`<br>`completion fish --help`<br>`completion powershell --help`<br>`completion zsh --help`<br>`help --help` |
| `cli-flags/render-flags-5` | `get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json -l app=selected`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json --selector app=missing`<br>`get ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o json -l app=missing`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -P @@baseline@@/cluster`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress --kube-version 1.31.2`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress -a fixture.example/v1` |
| `cli-flags/environment-bindings-6` | `get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress`<br>`cache gc` |
| `cli-flags/argv-precedence-7` | `get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json -l app=selected`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --path-orig @@baseline@@/cluster`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress --kube-version 1.31.2`<br>`build hr app -p @@repo@@/cluster --concurrency 2 --no-progress --api-versions fixture.example/v1`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --cache-dir ` |
| `cli-flags/parser-errors-8` | `diff ks missing -p @@repo@@/cluster --concurrency 2 --no-progress --base base`<br>`diff hr missing -p @@repo@@/cluster --concurrency 2 --no-progress --base base` |
| `cli-flags/environment-bindings-9` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress -o json` |
| `cli-flags/argv-precedence-10` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base base -o json` |
| `cli-flags/render-flags-11` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json --strip-attr fixture.example/change`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json --strip-field spec.replicas` |
| `cli-flags/environment-bindings-12` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json` |
| `cli-flags/argv-precedence-13` | `diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json --strip-attr fixture.example/change`<br>`diff ks app -p @@repo@@/cluster --concurrency 2 --no-progress --base strip-base -o json --strip-field spec.replicas` |
| `cli-flags/render-flags-14` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --supply @@inputs@@/first.yaml --supply @@inputs@@/second.yaml`<br>`build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --supply @@inputs@@/first.yaml --supply @@inputs@@/second.yaml --supply @@inputs@@/duplicate.yaml` |
| `cli-flags/environment-bindings-15` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress` |
| `cli-flags/argv-precedence-16` | `build ks app -p @@repo@@/cluster --concurrency 2 --no-progress --supply @@inputs@@/first.yaml --supply @@inputs@@/second.yaml` |
| `cli-flags/omitted-kustomization-path` | `get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o table`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o yaml`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o json`<br>`get ks -p @@repo@@/cluster --concurrency 2 --no-progress -o name` |
| `cli-flags/restricted-local-source` | `build ks app -p @@repo@@/cluster --concurrency 2 --restrict-egress` |
| `cache-lifecycle/empty-cache` | `cache gc --cache-dir @@cache@@`<br>`cache gc --dry-run --max-age 0 --cache-dir @@cache@@`<br>`cache gc --cache-dir @@cache@@/absent`<br>`cache gc`<br>`cache gc --cache-dir @@cache@@ --max-age 0 --dry-run=false` |
| `cache-lifecycle/negative-age` | `cache gc --max-age=-1s --cache-dir @@cache@@` |
| `cache-lifecycle/missing-cache` | `cache gc` |
| `cache-lifecycle/invalid-env-age` | `cache gc` |
| `cache-lifecycle/invalid-env-dry` | `cache gc` |
| `cache-lifecycle/cache-command` | `cache` |

`get all` and bare `get` reject label selectors on this revision. The selector cases use `get ks`.
`--allow-missing-kinds` accepts Secret, ConfigMap, and CustomResourceDefinition. RuntimeClass remains an explicit rejection case.
The CLI has no `list` command. The corpus retains its exact rejection.

`internal/cli/TestRun_Get_Formats` stays in its package because `runClusterInputs` supplies SDK trees directly.
Its cluster-qualified streams and original assertions remain intact.
The CLI corpus claims only the single-tree forms accepted by `cli.Run`.
The private `decodeGetDocuments` helper remains for changed-only, partial-failure, empty-kind, and cluster tests.
`writeGetRelease`, `writeFixture`, and the old harness entry points retain other consumers until their owning units migrate.

## Scope migration

`unclaimed-change/unclaimed-path-without-history` retains the authored fixture from flate revision `cf0821e638c7bcb146d6e5b784bef2509c1a1331`.
The fixture changes one unowned text file and keeps the chart and its emitted child unchanged.
Both invocations use `diff all --base HEAD -p @@repo@@/flux -o diff --no-progress --concurrency 2 --cache-dir @@cache@@`.
Each invocation owns a separate cold cache.
Both streams are empty, and the exit code is zero.
Two complete replays require the same result sequence.

An overlay operation can declare `symlink` with a `path` and a relative `target`.
The target stays inside a declared fixture tree and can be absent.
The materializer creates the link after restoration on every replay.
Disk fixture trees still reject undeclared symlinks.

An invocation can declare `transport` responses with the same host, method, and path fields as the scenario response table.
A response replaces that request's prior response from that invocation onward.
Each replay restores the scenario response table before its first invocation.
A response with `"error": "connection-reset"` returns a classified connection reset instead of an HTTP response.
Error responses omit the status, body, and headers.

`namespace-scope` preserves `TestE2E_NamespaceDiff` from flate revision `cf0821e638c7bcb146d6e5b784bef2509c1a1331`.
Each case keeps independent Git stores and changes the monitoring peer as an exclusion control.
The expected changes follow the selected namespace and both authored ConfigMaps.
The parent identity and labels follow the Flux Kustomization in `flux/selected.yaml`.
Every invocation uses two workers and a cache that resets between complete replays.

`namespace-scope/namespace-diff-changed` compares complete JSON changes, empty stderr, and exit zero.
Its invocation sequence is declared in `namespace-scope/scenario.json`.

`namespace-scope/namespace-diff-added` compares complete JSON changes, empty stderr, and exit zero.
Its invocation sequence is declared in `namespace-scope/scenario.json`.

`namespace-scope/namespace-diff-removed` compares complete JSON changes, empty stderr, and exit zero.
Its invocation sequence is declared in `namespace-scope/scenario.json`.

`namespace-scope/namespace-diff-renamed` compares complete JSON changes, empty stderr, and exit zero.
Its invocation sequence is declared in `namespace-scope/scenario.json`.

`namespace-scope/namespace-diff-empty` compares complete JSON changes, empty stderr, and exit zero.
Its invocation sequence is declared in `namespace-scope/scenario.json`.

`component-reach/component-change-propagates-to-all-consumers` retains the authored fixture from flate revision `cf0821e638c7bcb146d6e5b784bef2509c1a1331`.
Its declared command compares independent current and baseline trees with two workers.
Both replays reset the owned cache and compare both full streams with exit zero.

`component-reach/non-shared-change-does-not-propagate` retains the authored fixture from flate revision `cf0821e638c7bcb146d6e5b784bef2509c1a1331`.
Its declared command compares independent current and baseline trees with two workers.
Both replays reset the owned cache and compare both full streams with exit zero.
The expected dyff paths, identities, values, and whitespace follow the reviewed `output-formats` presentation contract.

`scope-authority` preserves the authoritative parsing cases from flate revision `cf0821e638c7bcb146d6e5b784bef2509c1a1331`.
The expected objects come from the authored documents and the pinned Kustomize renderer.
Typed failure causes come from the pinned Flux API JSON decoder.
Every case keeps the healthy sibling and compares two full sequences with two workers and replay-reset caches.

`scope-authority/typed-release` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/typed-kustomization` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/generated-release` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/patch-created-release` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/required-syntax` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/repaired-raw-release` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

`scope-authority/incidental-malformed-and-scalar` declares complete build output, diagnostics, and exit status in `scope-authority/scenario.json`.

A repository with `directory: ""` starts empty.
Declared overlays and commits supply its files through the existing materializer.
It needs no placeholder file.

The `cacheFailure` operation records attempted chart failures through the source cache API.
It declares `cache`, `repository`, `chart`, `version`, and `attempts`.
The attempt count must be between one and twelve.
The operation derives the same transport identity as the chart fetcher.
Synthetic timer receives advance the cache backoff without a real sleep.
The cache closes before the next CLI invocation.
CLI calls and transport workers stay outside the synthetic clock.
The terminal replay case keeps its primed chart and requires twelve attempted failures.
Its package proof reopens the CLI cache and requires a classified failure without another attempt.

The `noGit` setup operation names one declared repository.
The materializer removes only that repository's Git metadata after it restores the declared tree.
Replays repeat this operation and retain the repository files.

The catalog builds package states after setup and declared edits.
A successful package must emit at least one object.
A file use can declare `package: failure` for an intentional build failure with a named failing invocation.
A file use can declare `package: excluded` for an intentionally broken package that a named successful invocation excludes.
A Component declares `package: component` and its materialized `consumer` directory.
The catalog builds that consumer and requires it to read the declared Component file.
Historical source packages keep the same nonempty output requirement.
Partial overlays must pass in a declared materialized state.
Failure roles use only invocation results between adjacent edits.
Named symlink operations supply their synthetic paths to the catalog without requiring regular files at those paths.

### entry-membership

`entry-membership/classification-yaml`: classification yaml.

`entry-membership/classification-yml`: classification yml.

`entry-membership/classification-capitalized`: classification capitalized.

`entry-membership/classification-plain`: classification plain.

`entry-membership/classification-malformed`: classification malformed.

`entry-membership/classification-multiple`: classification multiple.

`entry-membership/classification-component`: classification component.

`entry-membership/selected-root`: selected root.

`entry-membership/selected-root-nested-owner`: selected root nested owner.

`entry-membership/selected-diff-referenced-patch`: selected diff referenced patch.

`entry-membership/selected-diff-resource-removed`: selected diff resource removed.

`entry-membership/selected-diff-resource-added`: selected diff resource added.

`entry-membership/selected-diff-list-member-added`: selected diff list member added.

`entry-membership/selected-diff-list-member-removed`: selected diff list member removed.

`entry-membership/selected-diff-declaration-removed`: selected diff declaration removed.

`entry-membership/selected-diff-declaration-added`: selected diff declaration added.

`entry-membership/selected-diff-excluded-sibling`: selected diff excluded sibling.

`entry-membership/selected-diff-plain-dependency`: selected diff plain dependency.

`entry-membership/selected-diff-owner-added-forward`: selected diff owner added forward.

`entry-membership/selected-diff-owner-added-reverse`: selected diff owner added reverse.

`entry-membership/selected-diff-owner-removed-forward`: selected diff owner removed forward.

`entry-membership/selected-diff-owner-removed-reverse`: selected diff owner removed reverse.

`entry-membership/selected-diff-owner-renamed-forward`: selected diff owner renamed forward.

`entry-membership/selected-diff-owner-renamed-reverse`: selected diff owner renamed reverse.

`entry-membership/selected-diff-owner-winner-changed-forward`: selected diff owner winner changed forward.

`entry-membership/selected-diff-owner-winner-changed-reverse`: selected diff owner winner changed reverse.

`entry-membership/named-missing-child-owner-added-forward`: named missing child owner added forward.

`entry-membership/named-missing-child-owner-added-reverse`: named missing child owner added reverse.

`entry-membership/named-missing-child-owner-removed-forward`: named missing child owner removed forward.

`entry-membership/named-missing-child-owner-removed-reverse`: named missing child owner removed reverse.

`entry-membership/named-missing-child-owner-renamed-forward`: named missing child owner renamed forward.

`entry-membership/named-missing-child-owner-renamed-reverse`: named missing child owner renamed reverse.

`entry-membership/named-missing-child-owner-renamed-nested-forward`: named missing child owner renamed nested forward.

`entry-membership/named-missing-child-owner-renamed-nested-reverse`: named missing child owner renamed nested reverse.

`entry-membership/disabled-nested-enabled`: disabled nested enabled.

`entry-membership/disabled-nested-absent`: disabled nested absent.

`entry-membership/disabled-nested-commented`: disabled nested commented.

`entry-membership/disabled-invalid-api-version`: disabled invalid api version.

`entry-membership/disabled-invalid-yaml-syntax`: disabled invalid yaml syntax.

`entry-membership/disabled-shared-active-target`: disabled shared active target.

`entry-membership/disabled-cross-package-inclusion`: disabled cross package inclusion.

`entry-membership/disabled-single-root`: disabled single root.

`entry-membership/disabled-single-scoped`: disabled single scoped.

`entry-membership/disabled-multi-root`: disabled multi root.

`entry-membership/disabled-multi-scoped`: disabled multi scoped.

`entry-membership/entry-self-ownership-selected-package`: entry self ownership selected package.

`entry-membership/entry-self-ownership-plain-entry-ancestor`: entry self ownership plain entry ancestor.

`entry-membership/entry-self-ownership-disabled-foreign-owner`: entry self ownership disabled foreign owner.

`entry-membership/disabled-diff-nested-remove`: disabled diff nested remove.

`entry-membership/disabled-diff-nested-restore`: disabled diff nested restore.

`entry-membership/disabled-diff-nested-payload`: disabled diff nested payload.

`entry-membership/disabled-diff-remove`: disabled diff remove.

`entry-membership/disabled-diff-restore`: disabled diff restore.

`entry-membership/membership-namespace-payload`: membership namespace payload.

`entry-membership/membership-namespace-namespace-forward`: membership namespace namespace forward.

`entry-membership/membership-namespace-namespace-reverse`: membership namespace namespace reverse.


### entry-packages

`entry-packages-entry-package-json-build/entry-package-json-build`: entry package json build.

`entry-packages-entry-package-json-diff/entry-package-json-diff`: entry package json diff.

`entry-packages-entry-package-failure-test/entry-package-failure-test`: entry package failure test.

`entry-packages-entry-package-failure-diff/entry-package-failure-diff`: entry package failure diff.

`entry-packages-entry-package-failure-diff-json/entry-package-failure-diff-json`: entry package failure diff json.

`entry-packages-discovery-package-one/discovery-package-one`: discovery package one.

`entry-packages-discovery-package-two/discovery-package-two`: discovery package two.

`entry-packages-root-layer-declarations/root-layer-declarations`: root layer declarations.

`entry-packages-literal-root-overlay-collision/literal-root-overlay-collision`: literal root overlay collision.

`entry-packages-literal-root-failure-build-all/literal-root-failure-build-all`: literal root failure build all.

`entry-packages-literal-root-failure-build-ks/literal-root-failure-build-ks`: literal root failure build ks.

`entry-packages-literal-root-failure-test-all/literal-root-failure-test-all`: literal root failure test all.

`entry-packages-literal-root-failure-test-ks/literal-root-failure-test-ks`: literal root failure test ks.

`entry-packages-entry-prerequisite-forward/entry-prerequisite-forward`: entry prerequisite forward.

`entry-packages-entry-prerequisite-reverse/entry-prerequisite-reverse`: entry prerequisite reverse.


### scope-namespaces

`scope-namespaces/child-bare`: child bare

`scope-namespaces/child-target`: child target

`scope-namespaces/child-authored`: child authored

`scope-namespaces/child-authored-insert01-remove0`: child authored insert01 remove0

`scope-namespaces/child-authored-insert01-remove1`: child authored insert01 remove1

`scope-namespaces/child-authored-insert10-remove0`: child authored insert10 remove0

`scope-namespaces/child-authored-insert10-remove1`: child authored insert10 remove1

`scope-namespaces/runtime-omitted`: runtime omitted

`scope-namespaces/runtime-explicit`: runtime explicit

`scope-namespaces/runtime-chart-value-forward`: runtime chart value forward

`scope-namespaces/runtime-chart-value-reverse`: runtime chart value reverse

`scope-namespaces/runtime-authored-namespace-forward`: runtime authored namespace forward

`scope-namespaces/runtime-authored-namespace-reverse`: runtime authored namespace reverse

`scope-namespaces/bootstrap-sibling-projection`: bootstrap sibling projection


### scope-orphans

`scope-orphans/namespace-sibling`: namespace sibling

`scope-orphans/namespace-explicit`: namespace explicit

`scope-orphans/namespace-standalone`: namespace standalone

`scope-orphans/namespace-shared`: namespace shared

`scope-orphans/namespace-terminal-replay`: namespace terminal replay

`scope-orphans/namespace-failing-owner-2`: namespace failing owner 2

`scope-orphans/namespace-failing-owner-4`: namespace failing owner 4

`scope-orphans/parent-local`: parent local

`scope-orphans/parent-external`: parent external

`scope-orphans/excluded-siblings`: excluded siblings

`scope-orphans/undiscovered-kustomization`: undiscovered kustomization


### scope-malformed

`scope-malformed/owned-graph`: owned graph

`scope-malformed/shared-graph`: shared graph

`scope-malformed/owned-ad-hoc`: owned ad hoc

`scope-malformed/owned-ad-hoc-wide`: owned ad hoc wide

`scope-malformed/suspended-owner`: suspended owner

`scope-malformed/changed-unselected-owner`: changed unselected owner

`scope-malformed/generated-owner`: generated owner

`scope-malformed/suspended-generated-owner`: suspended generated owner

`scope-malformed/external-source-owner`: external source owner

`scope-malformed/unowned-graph`: unowned graph

`scope-malformed/unowned-ad-hoc`: unowned ad hoc


### scope-malformed-transitions

`scope-malformed-transitions/malformed-to-valid`: malformed to valid

`scope-malformed-transitions/valid-to-malformed`: valid to malformed


### scope-unowned

`scope-unowned/malformed`: malformed

`scope-unowned/unreadable`: unreadable


### scope-unreadable

`scope-unreadable/owned-independent-sibling`: owned independent sibling

`scope-unreadable/unowned-nested-census`: unowned nested census


### scope-inputs

`scope-inputs-escaped-implicit-shallow/escaped-implicit-shallow`: escaped implicit shallow.

`scope-inputs-escaped-implicit-deep/escaped-implicit-deep`: escaped implicit deep.

`scope-inputs-escaped-mixed/escaped-mixed`: escaped mixed.

`scope-inputs-escaped-explicit/escaped-explicit`: escaped explicit.

`scope-inputs-escaped-patch-only/escaped-patch-only`: escaped patch only.

`scope-inputs-input-lifecycle-add/input-lifecycle-add`: input lifecycle add.

`scope-inputs-input-lifecycle-delete/input-lifecycle-delete`: input lifecycle delete.

`scope-inputs-input-lifecycle-dangling/input-lifecycle-dangling`: input lifecycle dangling.

`scope-inputs-selected-input-builder/selected-input-builder`: selected input builder.

`scope-inputs-selected-input-patch/selected-input-patch`: selected input patch.

`scope-inputs-selected-input-unrelated/selected-input-unrelated`: selected input unrelated.

`scope-inputs-chart-input-template-committed/chart-input-template-committed`: chart input template committed.

`scope-inputs-chart-input-template-directories/chart-input-template-directories`: chart input template directories.

`scope-inputs-chart-input-template-added-directories/chart-input-template-added-directories`: chart input template added directories.

`scope-inputs-chart-input-template-added-committed/chart-input-template-added-committed`: chart input template added committed.

`scope-inputs-chart-input-template-deleted-directories/chart-input-template-deleted-directories`: chart input template deleted directories.

`scope-inputs-chart-input-template-deleted-committed/chart-input-template-deleted-committed`: chart input template deleted committed.

`scope-inputs-chart-input-default-values/chart-input-default-values`: chart input default values.

`scope-inputs-chart-input-packaged-subchart/chart-input-packaged-subchart`: chart input packaged subchart.

`scope-inputs-chart-input-root-chart/chart-input-root-chart`: chart input root chart.

`scope-inputs-chart-input-policy-admits-template/chart-input-policy-admits-template`: chart input policy admits template.

`scope-inputs-chart-input-policy-removes-template/chart-input-policy-removes-template`: chart input policy removes template.

`scope-inputs-chart-input-policy-created/chart-input-policy-created`: chart input policy created.

`scope-inputs-chart-input-policy-deleted/chart-input-policy-deleted`: chart input policy deleted.

`scope-inputs-chart-input-excluded-payload/chart-input-excluded-payload`: chart input excluded payload.

`scope-inputs-chart-input-unchanged/chart-input-unchanged`: chart input unchanged.

`scope-inputs-chart-input-remote-chart-excludes-local-payload/chart-input-remote-chart-excludes-local-payload`: chart input remote chart excludes local payload.

`scope-inputs-chart-changed-directories/chart-changed-directories`: chart changed directories.

`scope-inputs-chart-changed-directories-invalid/chart-changed-directories-invalid`: chart changed directories invalid.

`scope-inputs-chart-changed-committed/chart-changed-committed`: chart changed committed.

`scope-inputs-chart-changed-committed-invalid/chart-changed-committed-invalid`: chart changed committed invalid.

`scope-inputs-optional-changed-directory/optional-changed-directory`: optional changed directory.

`scope-inputs-optional-changed-committed/optional-changed-committed`: optional changed committed.

`scope-inputs-optional-sides-appear/optional-sides-appear`: optional sides appear.

`scope-inputs-optional-sides-remove/optional-sides-remove`: optional sides remove.

`scope-inputs-optional-sides-projected-appear/optional-sides-projected-appear`: optional sides projected appear.

`scope-inputs-optional-sides-projected-remove/optional-sides-projected-remove`: optional sides projected remove.

`scope-inputs-optional-sides-required-removal/optional-sides-required-removal`: optional sides required removal.

`scope-inputs-cross-source-optional-removal/cross-source-optional-removal`: Identical bootstrap trees consume different selected supplier histories.

`scope-inputs-ignored-required-all/ignored-required-all`: ignored required all.

`scope-inputs-ignored-required-all-reverse/ignored-required-all-reverse`: ignored required all reverse.

`scope-inputs-ignored-required-named/ignored-required-named`: ignored required named.

`scope-inputs-ignored-required-named-reverse/ignored-required-named-reverse`: ignored required named reverse.


### scope-selection

`scope-selection-cache-implicit-unchanged-external/cache-implicit-unchanged-external`: cache implicit unchanged external.

`scope-selection-cache-implicit-changed-external/cache-implicit-changed-external`: cache implicit changed external.

`scope-selection-cache-implicit-suspended-current-external/cache-implicit-suspended-current-external`: cache implicit suspended current external.

`scope-selection-cache-implicit-suspended-orig-external/cache-implicit-suspended-orig-external`: cache implicit suspended orig external.

`scope-selection-cache-self-unchanged-external/cache-self-unchanged-external`: cache self unchanged external.

`scope-selection-cache-self-changed-external/cache-self-changed-external`: cache self changed external.

`scope-selection-cache-self-changed-relative/cache-self-changed-relative`: cache self changed relative.

`scope-selection-cache-self-changed-absolute/cache-self-changed-absolute`: cache self changed absolute.

`scope-selection-cache-self-suspended-current-external/cache-self-suspended-current-external`: cache self suspended current external.

`scope-selection-cache-self-suspended-orig-external/cache-self-suspended-orig-external`: cache self suspended orig external.

`scope-selection-baseline-explicit-base/baseline-explicit-base`: baseline explicit base.

`scope-selection-baseline-self-ref/baseline-self-ref`: baseline self ref.

`scope-selection-committed-membership-delete/committed-membership-delete`: committed membership delete.

`scope-selection-committed-membership-rename/committed-membership-rename`: committed membership rename.

`scope-selection-images-no-git-baseline/images-no-git-baseline`: images no git baseline.

`scope-selection-get-projected-kustomization/get-projected-kustomization`: get projected kustomization.

`scope-selection-missing-local-resource/missing-local-resource`: missing local resource.

`scope-selection-missing-local-chart/missing-local-chart`: missing local chart.

`scope-selection-auxiliary-supplied/auxiliary-supplied`: auxiliary supplied.

`scope-selection-auxiliary-allow-configmap/auxiliary-allow-configmap`: auxiliary allow configmap.

`scope-selection-auxiliary-allow-secret/auxiliary-allow-secret`: auxiliary allow secret.

`scope-selection-same-directory-failure/same-directory-failure`: same directory failure.

`scope-selection-ownership-cycle/ownership-cycle`: ownership cycle.

`scope-selection-bootstrap-template-error/bootstrap-template-error`: bootstrap template error.

`scope-selection-external-wide-source/external-wide-source`: external wide source.

`scope-selection-secret-material-username-required/secret-material-username-required`: secret material username required.

`scope-selection-secret-material-username-allowed/secret-material-username-allowed`: secret material username allowed.

`scope-selection-secret-material-password-required/secret-material-password-required`: secret material password required.

`scope-selection-secret-material-password-allowed/secret-material-password-allowed`: secret material password allowed.


### multi-cluster

`multi-cluster/root-partition`: Repository-root discovery retains three colliding Flux and workload identities, exact cluster framing, and named selection; each selected path has its own complete output.

`multi-cluster/one-sided-diff`: The removed utility cluster has an unsuppressed removal, main has only its own change, and test stays unchanged; independent selected entries establish the corresponding payloads.


## Source-backed scope layout companions

These positive cases record the external repository layouts required by U1.
The other migrated regressions retain their `home-operations/flate` behavior lineage.
Each scenario records its source revision, original paths, reductions, and authored additions.
Fetched source bodies and hashes remain build artifacts.

`entry-membership-layout/pinned-disabled-declaration` uses `tyzbit/fleet-infra` at `87c199e34e96df4e475c5bb1a29b9aff035730e3`.
It retains `clusters/corpus/_disabled/project-zomboid/ks.yaml` and its application target.
A Namespace controls entry admission while the application is excluded.
The declared sequence includes, excludes, and restores the application.
Each invocation compares complete JSON output, diagnostics, and exit zero.

`orphan-membership/pinned-namespaced-static-root` uses the same source declaration and target paths.
Its selected directory gives the namespaced declaration no owning Kustomize package.
This authored offline variant requires the complete ConfigMap and its original owner identity.
Stderr is empty, and the exit code is zero.

`namespace-scope-layout/pinned-application-namespaces` uses `JJGadgets/Biohazard` at `4e4bb6180d6417990dc5f195852730ed77a3142f`.
It retains the actual and atuin declarations under `kube/deploy/apps`.
Their application paths, namespaces, target namespaces, and common labels remain.
Each namespace selection compares its complete ConfigMap, empty stderr, and exit zero.

`input-boundary/pinned-runner-escaped-input` uses `jfroy/flatops` at `a2e1cbf6cae6742b624ea66db20ce064f9a2f014`.
It retains both producers in `kubernetes/apps/actions-runner-system/arc/ks.yaml` and their dependency.
An authored ancestor generator makes a consumed input change visible.
The complete JSON diff changes only runner settings and retains the unchanged controller as an exclusion control.
Stderr is empty, and the exit code is zero.

`component-reach-layout/pinned-backup-component` uses `bjw-s-labs/home-ops` at `656b3515052fcdb0450ca56151809852e383200d`.
It retains the cluster-to-apps relationship and the Hermes Component edge, `../../../../components/kopiur/backup`.
The referenced source file declares a Component.
An authored peer consumes the same Component in `ai-peer`.
One Component edit produces exactly two JSON changes and leaves both application controls unchanged.
Stderr is empty, and the exit code is zero.

Every case uses two workers, declared caches, and two complete replays through `testScopeCase`.
Each reduction lists the omitted dependencies and authored variants in its source metadata.

## Source and credential corpus

The source cases retain tag, commit, semver, mutable HEAD, and dirty-file controls. Each case compares complete CLI results twice.

The six late-source cases compare both build and diff with two and eight workers. The four followed-source cases compare one, two, and eight workers. Each worker count uses the same complete expectations.

The source layout follows `wrmilling/k3s-gitops` at `dca29749e04b073f6463fdda490a74822594ed15`, with `flux-system/gotk-sync.yaml` as its topology reference. The regression manifests are authored reductions.

The declared `submodule` operation adds a gitlink at a root path in a named commit. The operation changes that commit token. A following `ref` operation selects the changed commit without changing the worktree.

The Git package retains HEAD, index, status, and file-byte observations. Low-level transport, authentication, helper processes, and cache identity remain package-level proofs. The identical-spec source-store case stays skipped because changed-only selection ignores divergent revisions with identical authored files.

| Case | Behavior | Cache policy |
| --- | --- | --- |
| `source-pins/nonhead-renders-preserve-checkout-tag` | nonhead renders preserve checkout tag | Reset on replay. |
| `source-pins/nonhead-renders-preserve-checkout-semver` | nonhead renders preserve checkout semver | Reset on replay. |
| `source-pins/nonhead-renders-preserve-checkout-head` | nonhead renders preserve checkout head | Reset on replay. |
| `source-pins/nonhead-renders-preserve-checkout-no-ref` | nonhead renders preserve checkout no ref | Reset on replay. |
| `source-pins/nonhead-renders-preserve-checkout-commit` | nonhead renders preserve checkout commit | Reset on replay. |
| `source-pins/stable-pin-ignores-dirty-content` | stable pin ignores dirty content | Reset on replay. |
| `source-pins/source-ref--nonhead-tag` | source ref  nonhead tag | Reset on replay. |
| `source-pins/pinned-diff--base-and-current-refs` | pinned diff  base and current refs | Reset on replay. |
| `source-pins/source-ref--pinned-followed-source-inherits-namespace-clusters-explicit` | source ref  pinned followed source inherits namespace clusters explicit | Reset on replay. |
| `source-pins/source-ref--pinned-followed-source-inherits-namespace-clusters-omitted` | source ref  pinned followed source inherits namespace clusters omitted | Reset on replay. |
| `source-pins/source-ref--pinned-followed-source-inherits-namespace-sub-explicit` | source ref  pinned followed source inherits namespace sub explicit | Reset on replay. |
| `source-pins/source-ref--pinned-followed-source-inherits-namespace-sub-omitted` | source ref  pinned followed source inherits namespace sub omitted | Reset on replay. |
| `source-pins/source-ref--pinned-tree-replaces-held-consumer-removed` | source ref  pinned tree replaces held consumer removed | Reset on replay. |
| `source-pins/source-ref--pinned-tree-replaces-held-consumer-replaced` | source ref  pinned tree replaces held consumer replaced | Reset on replay. |
| `source-pins/source-ref--pinned-tree-replaces-held-consumer-different-name` | source ref  pinned tree replaces held consumer different name | Reset on replay. |
| `source-pins/source-ref--pinned-tree-replaces-held-consumer-different-path` | source ref  pinned tree replaces held consumer different path | Reset on replay. |
| `source-pins/source-ref--pinned-tree-replaces-held-consumer-different-source-ref` | source ref  pinned tree replaces held consumer different source ref | Reset on replay. |
| `source-pins/source-ref--pinned-root-preserves-source` | source ref  pinned root preserves source | Reset on replay. |
| `source-pins/source-ref--pinned-path-renders-deleted-kustomization` | source ref  pinned path renders deleted kustomization | Reset on replay. |
| `source-pins/source-ref--patched-stub-kustomization-keeps-parent-path` | source ref  patched stub kustomization keeps parent path | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-apps-first-flux-system` | source ref  pinned source discovered after consumer apps first flux system | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-apps-first-cluster` | source ref  pinned source discovered after consumer apps first cluster | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-sources-first-flux-system` | source ref  pinned source discovered after consumer sources first flux system | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-sources-first-cluster` | source ref  pinned source discovered after consumer sources first cluster | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-unpinned-no-ref` | source ref  pinned source discovered after consumer unpinned no ref | Reset on replay. |
| `source-pins/source-ref--pinned-source-discovered-after-consumer-unpinned-non-matching` | source ref  pinned source discovered after consumer unpinned non matching | Reset on replay. |
| `source-pins/source-ref--pinned-path-owns-working-tree-releases` | source ref  pinned path owns working tree releases | Reset on replay. |
| `source-pins/source-ref--pinned-path-ignores-added-discovery-objects` | source ref  pinned path ignores added discovery objects | Reset on replay. |
| `source-bootstrap/source-ref--empty-path-matches-root-explicit` | source ref  empty path matches root explicit | Reset on replay. |
| `source-bootstrap/source-ref--empty-path-matches-root-omitted` | source ref  empty path matches root omitted | Reset on replay. |
| `source-bootstrap/source-ref--missing-non-git-ocirepository` | source ref  missing non git ocirepository | Reset on replay. |
| `source-bootstrap/source-ref--missing-non-git-helmrepository` | source ref  missing non git helmrepository | Reset on replay. |
| `source-bootstrap/source-ref--missing-non-git-bucket` | source ref  missing non git bucket | Reset on replay. |
| `source-ignore/source-ref--ignore-negation-omitted` | source ref  ignore negation omitted | Reset on replay. |
| `source-ignore/source-ref--ignore-negation-explicit` | source ref  ignore negation explicit | Reset on replay. |
| `source-ignore/source-ref--ignore-negation-excluded` | source ref  ignore negation excluded | Reset on replay. |
| `source-warnings/source-warnings-aliases-build` | source warnings aliases build | Reset on replay. |
| `source-warnings/source-warnings-aliases-test` | source warnings aliases test | Reset on replay. |
| `source-warnings/source-warnings-aliases-get` | source warnings aliases get | Reset on replay. |
| `source-warnings/source-warnings--bootstrap-submodule-aliases` | source warnings  bootstrap submodule aliases | Reset on replay. |
| `source-warnings/source-warnings-unavailable-ref-build` | source warnings unavailable ref build | Reset on replay. |
| `source-warnings/source-warnings-unavailable-ref-test` | source warnings unavailable ref test | Reset on replay. |
| `source-warnings/source-warnings-unavailable-ref-get` | source warnings unavailable ref get | Reset on replay. |
| `source-diff-pins/source-ref--path-orig-uses-own-object-store-different-spec` | source ref  path orig uses own object store different spec | Reset on replay. |
| `source-diff-pins/source-ref--path-orig-uses-own-object-store-identical-spec` | source ref  path orig uses own object store identical spec | Reset on replay. |
| `source-diff-pins/pinned-diff--substitute-from-producer` | pinned diff  substitute from producer | Reset on replay. |
| `source-diff-pins/pinned-diff--deleted-substitute-from-producer` | pinned diff  deleted substitute from producer | Reset on replay. |
| `source-diff-pins/pinned-diff--dirty-transitive-substitute-from` | pinned diff  dirty transitive substitute from | Reset on replay. |
| `source-diff-pins/pinned-diff--generator-inputs-secret-env` | pinned diff  generator inputs secret env | Reset on replay. |
| `source-diff-pins/pinned-diff--generator-inputs-configmap-file` | pinned diff  generator inputs configmap file | Reset on replay. |
| `source-diff-pins/pinned-diff--escaped-patch-input` | pinned diff  escaped patch input | Reset on replay. |
| `source-diff-pins/supplied-git-repository` | supplied git repository | Reset on replay. |
| `source-diff-pins/pinned-diff--root-pin` | pinned diff  root pin | Reset on replay. |
| `substitute-sops/bootstrap-generator-sops-substitution` | bootstrap generator sops substitution | Reset on replay. |
| `substitute-sops/generator-sops-document-hash` | generator sops document hash | Reset on replay. |
| `source-membership/discovery-index--scoped-raw-file-fallback` | discovery index  scoped raw file fallback | Reset on replay. |
| `source-membership/discovery-index--changed-only-file-backed-producers-plain` | discovery index  changed only file backed producers plain | Reset on replay. |
| `source-membership/discovery-index--changed-only-file-backed-producers-list` | discovery index  changed only file backed producers list | Reset on replay. |
| `artifact-reach/discovery-index--changed-only-generator-inputs-secret-env` | discovery index  changed only generator inputs secret env | Reset on replay. |
| `artifact-reach/discovery-index--changed-only-generator-inputs-configmap-file` | discovery index  changed only generator inputs configmap file | Reset on replay. |
| `manifest-failures/loader-incidental-parse-failure-sibling` | loader incidental parse failure sibling | Reset on replay. |
| `manifest-failures/loader-incidental-parse-failure-component` | loader incidental parse failure component | Reset on replay. |
| `manifest-failures/kustomization-parse-failure` | kustomization parse failure | Reset on replay. |
| `artifact-copy/bootstrap-generator-failure-isolation-missing` | bootstrap generator failure isolation missing | Reset on replay. |
| `artifact-copy/bootstrap-generator-failure-isolation-outside` | bootstrap generator failure isolation outside | Reset on replay. |
| `chart-inputs/source-ref--last-owner-local-chart` | source ref  last owner local chart | Reset on replay. |
| `source-errors/source-ref--held-error-across-namespaces-external-source` | source ref  held error across namespaces external source | Reset on replay. |
| `source-errors/source-ref--held-error-across-namespaces-working-tree-source` | source ref  held error across namespaces working tree source | Reset on replay. |
| `source-errors/source-ref--held-error-survives-namespace-inheritance-explicit-source-namespace` | source ref  held error survives namespace inheritance explicit source namespace | Reset on replay. |
| `source-errors/source-ref--held-error-survives-namespace-inheritance-implicit-source-namespace` | source ref  held error survives namespace inheritance implicit source namespace | Reset on replay. |
| `source-errors/source-ref--held-error-survives-namespace-inheritance-pinned-inherited-namespace` | source ref  held error survives namespace inheritance pinned inherited namespace | Reset on replay. |
| `source-errors/source-ref--held-error-survives-namespace-inheritance-external-source-in-other-inherited-namespace` | source ref  held error survives namespace inheritance external source in other inherited namespace | Reset on replay. |
| `source-errors/source-ref--held-error-survives-namespace-inheritance-pinned-namespace-free-source` | source ref  held error survives namespace inheritance pinned namespace free source | Reset on replay. |
| `source-errors/source-ref--held-error-retained-by-empty-path` | source ref  held error retained by empty path | Reset on replay. |
| `source-errors/source-ref--held-error-scope-oci` | source ref  held error scope oci | Reset on replay. |
| `source-errors/source-ref--held-error-scope-bootstrap` | source ref  held error scope bootstrap | Reset on replay. |
| `source-errors/source-ref--held-error-scope-resolved-unavailable-tag` | source ref  held error scope resolved unavailable tag | Reset on replay. |
| `source-errors/source-ref--held-error-scope-non-matching-url` | source ref  held error scope non matching url | Reset on replay. |
| `source-errors/source-ref--held-error-scope-two-held` | source ref  held error scope two held | Reset on replay. |
| `source-errors/source-ref--held-error-scope-two-held-path-order` | source ref  held error scope two held path order | Reset on replay. |
| `machine-auth/docker-native-store` | docker native store | Reset on replay. |
| `machine-auth/helm-native-store` | helm native store | Reset on replay. |
| `machine-auth/anonymous-registry` | anonymous registry | Reset on replay. |
| `machine-auth/malformed-native-registry-store` | malformed native registry store | Reset on replay. |
| `machine-auth/aws-native-profile` | aws native profile | Reset on replay. |
| `machine-auth/missing-certificate` | missing certificate | Reset on replay. |
| `machine-auth/missing-proxy` | missing proxy | Reset on replay. |
| `remote-inputs/git-source` | git source | Reset on replay. |
| `cache-lifecycle/source-prime-and-warm` | Prime a source cache, then reuse the same cache for the warm render and replay. | Reuse on replay. |
| `source-errors/source-ref--held-error-scope-non-decode-error` | The loader package distinguishes a read error from a decode error. Unsupported readers remain not-run. | Package-only permission proof. |

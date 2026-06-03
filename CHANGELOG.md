# Changelog

## [0.2.11](https://github.com/home-operations/flate/compare/0.2.10...0.2.11) (2026-06-03)


### Features

* **diff:** expandable full-file context for `-o html` ([#561](https://github.com/home-operations/flate/issues/561)) ([68664b3](https://github.com/home-operations/flate/commit/68664b3bc1ae410032d0ede94699312b58729e49))


### Miscellaneous Chores

* move mise to mise folder ([7a06078](https://github.com/home-operations/flate/commit/7a060786b71ff2f689d3f12a4003f11d8e8098c0))

## [0.2.10](https://github.com/home-operations/flate/compare/0.2.9...0.2.10) (2026-06-03)


### Features

* **diff:** word-level highlighting, filterable tree, keyboard nav for `-o html` ([#558](https://github.com/home-operations/flate/issues/558)) ([eeff31f](https://github.com/home-operations/flate/commit/eeff31fef4cdc818bdf067a88ba219f3fe8b6759))

## [0.2.9](https://github.com/home-operations/flate/compare/0.2.8...0.2.9) (2026-06-03)


### Bug Fixes

* **manifest:** flatten Kubernetes List wrappers in rendered output ([#556](https://github.com/home-operations/flate/issues/556)) ([f75c617](https://github.com/home-operations/flate/commit/f75c6175c4d83012e94f4d9c22422f73260a9dc5))

## [0.2.8](https://github.com/home-operations/flate/compare/0.2.7...0.2.8) (2026-06-03)


### Bug Fixes

* **diff:** emit nothing for `-o html` when there is no diff ([#554](https://github.com/home-operations/flate/issues/554)) ([489e214](https://github.com/home-operations/flate/commit/489e2144ae04f20d8d079deda3ed3cbb8fd722af))

## [0.2.7](https://github.com/home-operations/flate/compare/0.2.6...0.2.7) (2026-06-03)


### Bug Fixes

* **helm:** support external $ref in values.schema.json ([#552](https://github.com/home-operations/flate/issues/552)) ([c16c9ad](https://github.com/home-operations/flate/commit/c16c9ad70c0c345581a10797a555d41fd3f85e84))

## [0.2.6](https://github.com/home-operations/flate/compare/0.2.5...0.2.6) (2026-06-03)


### Code Refactoring

* **diff:** tidy `-o html` viewer internals and template ([#549](https://github.com/home-operations/flate/issues/549)) ([638a294](https://github.com/home-operations/flate/commit/638a294c179ee3421a8e4e1a1cbcf3fb750920fc))

## [0.2.5](https://github.com/home-operations/flate/compare/0.2.4...0.2.5) (2026-06-03)


### Features

* **diff:** add `-o html` — self-contained HTML diff viewer ([#546](https://github.com/home-operations/flate/issues/546)) ([ca5add0](https://github.com/home-operations/flate/commit/ca5add03328b7cf459af45dfa8ac92c61c85489f))


### Bug Fixes

* **loader:** discover bootstrap-style Flux Kustomization siblings ([#547](https://github.com/home-operations/flate/issues/547)) ([a9edc45](https://github.com/home-operations/flate/commit/a9edc45b24b05c091b6851ea48e20e4c13ac8b80))

## [0.2.4](https://github.com/home-operations/flate/compare/0.2.3...0.2.4) (2026-06-02)


### Bug Fixes

* **change:** re-render consumers when a referenced source changes ([#545](https://github.com/home-operations/flate/issues/545)) ([45a7414](https://github.com/home-operations/flate/commit/45a7414397c3a46d9d769b17a1fcf04f3c19e095))
* **kustomize:** recursive working-tree fingerprint for stage cache ([#544](https://github.com/home-operations/flate/issues/544)) ([13ad3bf](https://github.com/home-operations/flate/commit/13ad3bfb89e72ed855374f23580e04c151c73b2a))
* **mise:** update tool go (1.26.3 → 1.26.4) ([0eab7de](https://github.com/home-operations/flate/commit/0eab7deb41043358f1bbaabaaa94d9c140f9f037))

## [0.2.3](https://github.com/home-operations/flate/compare/0.2.2...0.2.3) (2026-06-02)


### Bug Fixes

* **diff:** blank line between docs in unified output ([#541](https://github.com/home-operations/flate/issues/541)) ([a2f2d97](https://github.com/home-operations/flate/commit/a2f2d9731f1ae7ae0d7037f15b5b42926f1d5d51))

## [0.2.2](https://github.com/home-operations/flate/compare/0.2.1...0.2.2) (2026-06-02)


### Features

* **cli:** add -p/-P shorthands for --path/--path-orig ([#537](https://github.com/home-operations/flate/issues/537)) ([b29856b](https://github.com/home-operations/flate/commit/b29856be1e380e9047bb1c9e4c3fe9bb120325f4))

## [0.2.1](https://github.com/home-operations/flate/compare/0.2.0...0.2.1) (2026-06-02)


### Miscellaneous Chores

* **discovery:** demote self-referential alias log to Debug ([#535](https://github.com/home-operations/flate/issues/535)) ([fb9c101](https://github.com/home-operations/flate/commit/fb9c101c28b11ef4f1d8e463e86e1ad2a4af3594))

## [0.2.0](https://github.com/home-operations/flate/compare/0.1.38...0.2.0) (2026-06-02)


### ⚠ BREAKING CHANGES

* **diff:** simplify output formats, default to human ([#532](https://github.com/home-operations/flate/issues/532))

### Bug Fixes

* **diff:** resolve self-referential GitRepository on the baseline side ([#534](https://github.com/home-operations/flate/issues/534)) ([29ee977](https://github.com/home-operations/flate/commit/29ee9772deecb3b59dcd8a57effe7778b34431db))


### Code Refactoring

* **diff:** simplify output formats, default to human ([#532](https://github.com/home-operations/flate/issues/532)) ([ca33395](https://github.com/home-operations/flate/commit/ca3339527f2f79fc8a15df5a3a88d3375a5028bd))

## [0.1.38](https://github.com/home-operations/flate/compare/0.1.37...0.1.38) (2026-06-02)


### Features

* **diff:** dyff output styles + native rendering, and pkg/diff cleanup ([#530](https://github.com/home-operations/flate/issues/530)) ([fa0ab26](https://github.com/home-operations/flate/commit/fa0ab26a36400b8b2089f46bb433805b37ec0fd9))
* **mise:** update tool oxfmt (0.52.0 → 0.53.0) ([cf23e71](https://github.com/home-operations/flate/commit/cf23e71afe34f29c15aa33150c6aaccf798b9130))
* stamp build version into the container image ([#527](https://github.com/home-operations/flate/issues/527)) ([949bd91](https://github.com/home-operations/flate/commit/949bd91a80851675e406e53558b832ad7b651f3f))


### Bug Fixes

* **configmap:** wipe SOPS-encrypted values to placeholders ([#523](https://github.com/home-operations/flate/issues/523)) ([84c3337](https://github.com/home-operations/flate/commit/84c333794306c3ba627bd4dbff344ce0494417d5))
* **loader:** resolve NamespaceTransformer targetNamespace at load time ([#529](https://github.com/home-operations/flate/issues/529)) ([32e9154](https://github.com/home-operations/flate/commit/32e91544d7cd3a3f04b7c12e6706d2591430b26f))
* **oci:** skip provenance layer when selecting the chart layer ([#522](https://github.com/home-operations/flate/issues/522)) ([996f9d1](https://github.com/home-operations/flate/commit/996f9d17b5c2c58c46105d3c676dde032e5cd088))


### Miscellaneous Chores

* add discord badge ([4e5628d](https://github.com/home-operations/flate/commit/4e5628d02af9609e477279432774e5bd452156ad))
* add discord badge ([00e959d](https://github.com/home-operations/flate/commit/00e959d9fdf6c8da22cbae889cae97696f816701))
* mise lock ([c0928e7](https://github.com/home-operations/flate/commit/c0928e7941a79c7fbd4fbd96b03ba99947100d6d))

## [0.1.37](https://github.com/home-operations/flate/compare/0.1.36...0.1.37) (2026-06-01)


### Bug Fixes

* **action:** ignore forge token for github.com release lookups ([#520](https://github.com/home-operations/flate/issues/520)) ([21e117b](https://github.com/home-operations/flate/commit/21e117b990f1bb469b568a05b6fe295c17f16a2d))

## [0.1.36](https://github.com/home-operations/flate/compare/0.1.35...0.1.36) (2026-06-01)


### Bug Fixes

* **helmrelease:** prune unchanged dependsOn targets in changed-only mode ([#518](https://github.com/home-operations/flate/issues/518)) ([e51b770](https://github.com/home-operations/flate/commit/e51b7709b1fb438cff90d92492b923d28960680c))
* **mise:** update tool lefthook (2.1.8 → 2.1.9) ([1c0fcaf](https://github.com/home-operations/flate/commit/1c0fcaf5e7ff4b52aba4a74f1b1d7f46a2f4af8f))


### Miscellaneous Chores

* implement oxfmt ([2236e28](https://github.com/home-operations/flate/commit/2236e28705171695c9c9b283852334d5a94c2d39))
* upgrade deps ([1f8a09f](https://github.com/home-operations/flate/commit/1f8a09fe409d421ebc5308068132c8a690b1f9de))

## [0.1.35](https://github.com/home-operations/flate/compare/0.1.34...0.1.35) (2026-05-30)


### Performance Improvements

* **cold-start:** cache helm schema compile + raise GC target ([#514](https://github.com/home-operations/flate/issues/514)) ([3392823](https://github.com/home-operations/flate/commit/3392823b44e17c28b54d7bfa651923a08ce2c72b))

## [0.1.34](https://github.com/home-operations/flate/compare/0.1.33...0.1.34) (2026-05-30)


### Documentation

* **readme:** editorial pass — fix stale claims, add ToC, reorder ([#513](https://github.com/home-operations/flate/issues/513)) ([e683754](https://github.com/home-operations/flate/commit/e6837543ee6724e84e871444aadbbe391c6ea8bf))


### Code Refactoring

* **controllers:** drop test-only resolveSourceRoot wrapper ([#499](https://github.com/home-operations/flate/issues/499)) ([8df2c3f](https://github.com/home-operations/flate/commit/8df2c3f73a87906933c78a688fdf92f304b374cd))
* drop test-only AllReady, trim stale comments ([#501](https://github.com/home-operations/flate/issues/501)) ([9f5b0b1](https://github.com/home-operations/flate/commit/9f5b0b14033e89606dbfcb18b92e0a0f086c26fe))
* **helm:** extract newInstallAction, move test-only hooks, fix name collision ([#502](https://github.com/home-operations/flate/issues/502)) ([3419b9a](https://github.com/home-operations/flate/commit/3419b9ac28810ec7e77e0851d7082cd2e988d944))
* **kustomize:** flatten copyFile, fix preflight comment, split stage.go ([#506](https://github.com/home-operations/flate/issues/506)) ([1e22e8a](https://github.com/home-operations/flate/commit/1e22e8ad17a309de99b14636cc6c0d0b5091b51f))
* **orchestrator:** remove dead cycle code, split Run, drop test-only MarkRendered ([#498](https://github.com/home-operations/flate/issues/498)) ([4c12d1e](https://github.com/home-operations/flate/commit/4c12d1e2cc5edabff2cd2a0e30eb5c294aa2ea6a))
* **source:** dedup cacheKeyHash/allocStaging, strip stale tag ([#500](https://github.com/home-operations/flate/issues/500)) ([951f23b](https://github.com/home-operations/flate/commit/951f23ba112e9fa1f49dc7a52f3c3dd706654ef5))
* unify disk-cache sweep + atomic CAS staging (internal/diskcache, internal/cas) ([#504](https://github.com/home-operations/flate/issues/504)) ([7f6a618](https://github.com/home-operations/flate/commit/7f6a6180c527d8d9741e28060f7a8e747fe0144f))

## [0.1.33](https://github.com/home-operations/flate/compare/0.1.32...0.1.33) (2026-05-29)


### Bug Fixes

* **diff:** redact ConfigMap binaryData ([#496](https://github.com/home-operations/flate/issues/496)) ([065df63](https://github.com/home-operations/flate/commit/065df632770a7e939f93a8653cde808a59be7952))

## [0.1.32](https://github.com/home-operations/flate/compare/0.1.31...0.1.32) (2026-05-29)


### Miscellaneous Chores

* default base ([91f8f0d](https://github.com/home-operations/flate/commit/91f8f0d93dccffcc8c1ac69c56685e49424632ab))
* default base ([cb4c9f1](https://github.com/home-operations/flate/commit/cb4c9f1a45e584f539ecc5b80a778a6b359b5b4b))

## [0.1.31](https://github.com/home-operations/flate/compare/0.1.30...0.1.31) (2026-05-29)


### Bug Fixes

* **diff:** empty markdown is empty, drop KS path from header ([#493](https://github.com/home-operations/flate/issues/493)) ([987ee91](https://github.com/home-operations/flate/commit/987ee91e40e97ab23fbe5b12e7ca59febe501cbb))

## [0.1.30](https://github.com/home-operations/flate/compare/0.1.29...0.1.30) (2026-05-29)


### Features

* update action to use new cache dir ([e56f782](https://github.com/home-operations/flate/commit/e56f782c062d9af992231ad057086aed1b3a03c5))

## [0.1.29](https://github.com/home-operations/flate/compare/0.1.28...0.1.29) (2026-05-29)


### Features

* --output markdown for build, diff, test, get ([#491](https://github.com/home-operations/flate/issues/491)) ([7e90725](https://github.com/home-operations/flate/commit/7e90725374c0e0f1d8dbdb903b51215fabb84fe3))
* **cli:** expose --cache-dir on build/diff/test/get ([#490](https://github.com/home-operations/flate/issues/490)) ([8d1e540](https://github.com/home-operations/flate/commit/8d1e540d75de2516201f0d67ff67b483e836dca5))


### Bug Fixes

* idempotency bugs + Go 1.22-1.26 modernization + lint config ([#489](https://github.com/home-operations/flate/issues/489)) ([e276e08](https://github.com/home-operations/flate/commit/e276e08bf267b542523d117b6923c3643a93a5b0))
* keep substituteFrom producer Kustomizations in changed-only mode ([#487](https://github.com/home-operations/flate/issues/487)) ([7e58a4d](https://github.com/home-operations/flate/commit/7e58a4d7933cb47a901cbff639b1c674f45cd693))


### Performance Improvements

* phases 0–3 (benchmarks, caches, sharding) — 34% faster warm on real repo ([#488](https://github.com/home-operations/flate/issues/488)) ([29de74b](https://github.com/home-operations/flate/commit/29de74ba270b0a1cd404a92cb55b19f8d6af255f))


### Miscellaneous Chores

* make action baby ([8ed64e5](https://github.com/home-operations/flate/commit/8ed64e52c1dc658002f1406d46edfe799489a291))

## [0.1.28](https://github.com/home-operations/flate/compare/0.1.27...0.1.28) (2026-05-28)


### Miscellaneous Chores

* **deps:** update go modules ([#484](https://github.com/home-operations/flate/issues/484)) ([767a65d](https://github.com/home-operations/flate/commit/767a65da430a7b422b39f4fd218b240364a298ae))

## [0.1.27](https://github.com/home-operations/flate/compare/0.1.26...0.1.27) (2026-05-27)


### Features

* **cli:** bind every flag to a FLATE_* env var ([#482](https://github.com/home-operations/flate/issues/482)) ([10ef361](https://github.com/home-operations/flate/commit/10ef36177e8d1434eb65388a7e0602e23d3becb9))

## [0.1.26](https://github.com/home-operations/flate/compare/0.1.25...0.1.26) (2026-05-27)


### Features

* **action:** default version to the action's pinned ref ([#481](https://github.com/home-operations/flate/issues/481)) ([29e373d](https://github.com/home-operations/flate/commit/29e373dbf09e6b766108590878be38093b63fa37))
* **action:** install via mise + optional XDG cache between runs ([#480](https://github.com/home-operations/flate/issues/480)) ([8d0204c](https://github.com/home-operations/flate/commit/8d0204c7eda3d6aeb91714a056f18d30bce6bb12))


### Code Refactoring

* **source:** drop dead MutableCacheKey fallback + clarify commitRefresh unique-name pattern ([#478](https://github.com/home-operations/flate/issues/478)) ([ca71637](https://github.com/home-operations/flate/commit/ca71637a508a52fcc4bcd60ff960e43333fd3c6c))

## [0.1.25](https://github.com/home-operations/flate/compare/0.1.24...0.1.25) (2026-05-27)


### Bug Fixes

* **controllers/helmrelease:** wire keepEmitted+markRendered in emitRenderedChildren ([#476](https://github.com/home-operations/flate/issues/476)) ([c648833](https://github.com/home-operations/flate/commit/c6488333b7bd1a56718782e3ed25741e14b0d885))
* **discovery:** normalize slog attribute keys to snake_case ([#459](https://github.com/home-operations/flate/issues/459)) ([644da89](https://github.com/home-operations/flate/commit/644da8962ab0c578023231df7c5aa9b933f28171))
* **kustomize:** isHTTPClientError 4xx detection + idiom cleanups ([#451](https://github.com/home-operations/flate/issues/451)) ([f5f0b1f](https://github.com/home-operations/flate/commit/f5f0b1f2a07b3c48dc1d1d395e724979c7e7c1bd))
* **kustomize:** typed httpStatusError sentinel survives error wrapping ([#467](https://github.com/home-operations/flate/issues/467)) ([9020cc1](https://github.com/home-operations/flate/commit/9020cc1df189cf919d48e8fc8c5cbc4b0f879df9))
* **loader:** .krmignore ** patterns now match (was silently no-op) + move NormalizePrefix ([#462](https://github.com/home-operations/flate/issues/462)) ([993d687](https://github.com/home-operations/flate/commit/993d687c084bcc343ceb140a603ead8a6e18c026))
* **loader:** propagate isDir to gitignore matcher (trailing-slash patterns now prune dirs) ([#474](https://github.com/home-operations/flate/issues/474)) ([3fa4c50](https://github.com/home-operations/flate/commit/3fa4c50aa0e7d521f3c11a2fa64c7fbe9e19a1ba))
* **manifest,selector:** apply the changes that iter-10 PR [#460](https://github.com/home-operations/flate/issues/460) promised but didn't ship ([#472](https://github.com/home-operations/flate/issues/472)) ([d12a17f](https://github.com/home-operations/flate/commit/d12a17f6d05d4881c3499c65b578b923beb4fe08))
* **manifest:** parseSecret in-place mutation + dead-code sweep + add GetLabels for selector dedupe ([#460](https://github.com/home-operations/flate/issues/460)) ([8bce374](https://github.com/home-operations/flate/commit/8bce37445989216a32d67a95d7b7eb669a5eb854))
* **mirror:** remove deadlocking InstallHTTPS call inside OpenOrFetch ([#450](https://github.com/home-operations/flate/issues/450)) ([cdd8927](https://github.com/home-operations/flate/commit/cdd89272b068ce7f9a52c1f537afa32b4e2a84ed))
* **selector:** correct package doc to list all four supported types ([#427](https://github.com/home-operations/flate/issues/427)) ([4c857d6](https://github.com/home-operations/flate/commit/4c857d62cc2f81e22fd12be9227546bac800ab46))
* **source/cache:** drop duplicate Slot.Refresh — fixes broken build ([#471](https://github.com/home-operations/flate/issues/471)) ([32e045e](https://github.com/home-operations/flate/commit/32e045e0acf22c638e990702cee9173503326f86))


### Performance Improvements

* **cacheroot:** drop redundant filepath.Clean from every path call ([#432](https://github.com/home-operations/flate/issues/432)) ([fc872cb](https://github.com/home-operations/flate/commit/fc872cbaac4d4a954409065a545a2f9d0377e4db))
* **change:** drop NUL-parse subslices, errors.AsType, capacity hints ([#441](https://github.com/home-operations/flate/issues/441)) ([fb18894](https://github.com/home-operations/flate/commit/fb18894591f9c8811d6ab4f1f0fba1c4c8bcf9f4))
* **controllers/helmrelease:** O(1) raw-producer index removes full-store scan from --allow-missing-secrets path ([#477](https://github.com/home-operations/flate/issues/477)) ([0b8937d](https://github.com/home-operations/flate/commit/0b8937d5f9c226069547e7f9569daf27009645ec))
* **git/verify:** strings.Builder in buildPGPKeyring ([#435](https://github.com/home-operations/flate/issues/435)) ([7ec04f3](https://github.com/home-operations/flate/commit/7ec04f3dc1df5608503267936171b8bb9c444aaf))
* **helm:** yield worker-pool slot during locateOCIChart puller.Fetch ([#473](https://github.com/home-operations/flate/issues/473)) ([fb2946f](https://github.com/home-operations/flate/commit/fb2946f56e785fb313d25c0dbb90513763f369b7))
* **internal/format:** drop per-call allocations on table render ([#425](https://github.com/home-operations/flate/issues/425)) ([f43d27b](https://github.com/home-operations/flate/commit/f43d27b5d3acbc3f3838a6538212345459bd63db))
* **manifest:** preallocate dependsOn slices and input decode maps ([#444](https://github.com/home-operations/flate/issues/444)) ([508e398](https://github.com/home-operations/flate/commit/508e398c84de981d00bd896581d15ad7ac57864a))
* **resourceset:** parse-once template hot path, O(1) cluster-scope, alloc reduction ([#443](https://github.com/home-operations/flate/issues/443)) ([45cb07e](https://github.com/home-operations/flate/commit/45cb07e11484c5e59108b8455b977dc33ac72d94))
* **source/bucket:** buffered IO + preallocated keys + close-error propagation ([#434](https://github.com/home-operations/flate/issues/434)) ([6eed82e](https://github.com/home-operations/flate/commit/6eed82e74a28ef819c72c8746b4ed0e657644dd2))
* **store:** fixed-size listener array, inline dispatch, condition-first scan ([#440](https://github.com/home-operations/flate/issues/440)) ([6276c1f](https://github.com/home-operations/flate/commit/6276c1f988c65768ba307e62c45a3070db47bd67))


### Documentation

* **gittransport:** tighten doc comments to why-only ([#436](https://github.com/home-operations/flate/issues/436)) ([558374e](https://github.com/home-operations/flate/commit/558374e1a90875baa29f5c6a05bf3dcebe906dcd))
* **loader,manifest:** clarify why kustomization filename lists differ ([#455](https://github.com/home-operations/flate/issues/455)) ([ead2bd6](https://github.com/home-operations/flate/commit/ead2bd69c0992d8eb743a31405b1a7a3dc52c84b))
* **source/atomic:** replace what-comments with POSIX-semantics rationale ([#430](https://github.com/home-operations/flate/issues/430)) ([03952c4](https://github.com/home-operations/flate/commit/03952c46d71885b4da088f4bcf99b028c46d02a3))


### Miscellaneous Chores

* post-merge cleanup pass on PRs [#413](https://github.com/home-operations/flate/issues/413)-[#420](https://github.com/home-operations/flate/issues/420) ([#421](https://github.com/home-operations/flate/issues/421)) ([12acb14](https://github.com/home-operations/flate/commit/12acb14755752da23d7747bf79c9301619996cd1))


### Code Refactoring

* **cli:** move labels out of commonFlags, drop dup output validate, split cache gc flags ([#469](https://github.com/home-operations/flate/issues/469)) ([608d8bf](https://github.com/home-operations/flate/commit/608d8bff79d5914d582d5e6ff97d49a3ff38dfcf))
* **cli:** snake_case slog, remove dead kind guard, fix names alloc ([#452](https://github.com/home-operations/flate/issues/452)) ([4293cf0](https://github.com/home-operations/flate/commit/4293cf0ceaae1e49a5daab63641a2dcf7c159bc0))
* consolidate resolveInputProvider into pkg/resourceset.StoreResolver ([#454](https://github.com/home-operations/flate/issues/454)) ([dd52572](https://github.com/home-operations/flate/commit/dd52572457b87a2760aa6d660e481c97fb49f2bb))
* **controllers:** consolidate preflight/waiter/parent into base.Controller ([#453](https://github.com/home-operations/flate/issues/453)) ([559d7e8](https://github.com/home-operations/flate/commit/559d7e8001ac469bd3cfe91615cc213b04c22328))
* **controllers:** WaitForStatus → testutil, split HR valuesfrom into own file ([#468](https://github.com/home-operations/flate/issues/468)) ([7e3f10c](https://github.com/home-operations/flate/commit/7e3f10c201021860244875b0046f8d5d3f12778f))
* **depwait:** map lookup for apiVersion, Go 1.22 loop vars, WaitGroup.Go ([#439](https://github.com/home-operations/flate/issues/439)) ([ce4339d](https://github.com/home-operations/flate/commit/ce4339dc32bcf7edfbab4d079d43f9683b6cd235))
* **discovery:** collapse redundant maps, CutPrefix, range-over-int ([#442](https://github.com/home-operations/flate/issues/442)) ([1653805](https://github.com/home-operations/flate/commit/165380551c6eae39924f1262bd0352b980f759f0))
* **format:** drop dead ParseOutput and ValidOutputs ([#475](https://github.com/home-operations/flate/issues/475)) ([c528190](https://github.com/home-operations/flate/commit/c528190f75de5c2c63acf33da5bf6b3526988fdd))
* **helm:** move chartCacheLocks onto Client, drop cloneValuesMap dup ([#461](https://github.com/home-operations/flate/issues/461)) ([c8a018c](https://github.com/home-operations/flate/commit/c8a018c619977e991e4202cd72dd8add5ec3aa0a))
* **keylock:** drop heap alloc on cancelled Acquire ([#428](https://github.com/home-operations/flate/issues/428)) ([8e8c676](https://github.com/home-operations/flate/commit/8e8c6762640cc39060cbdb57f01b6461df602f8b))
* **loader:** generic applyDefaultNS, cmp.Or for namespace precedence ([#446](https://github.com/home-operations/flate/issues/446)) ([1008655](https://github.com/home-operations/flate/commit/1008655f1768234ba80aa5cfb63f878c4232d03a))
* **mirror:** drop unused tlsCfg param from OpenOrFetch ([#457](https://github.com/home-operations/flate/issues/457)) ([d1bb504](https://github.com/home-operations/flate/commit/d1bb50453291aa430f9f7421a04f7d6fcfe4f2d2))
* **orchestrator:** collapse cycleMu+preflightMu, honor Concurrency in RS expansion ([#463](https://github.com/home-operations/flate/issues/463)) ([11fae8d](https://github.com/home-operations/flate/commit/11fae8dc51fa6bd345ffaf3f533586f33699f1a5))
* **orchestrator:** snake_case slog, slices/maps idioms, dead helper inline ([#447](https://github.com/home-operations/flate/issues/447)) ([e1f2680](https://github.com/home-operations/flate/commit/e1f2680db0c7b2238828dcad9c92e368e0c280f3))
* **pkg/baseline:** collapse dual-mode helper, dedupe materialize ([#423](https://github.com/home-operations/flate/issues/423)) ([72ac309](https://github.com/home-operations/flate/commit/72ac30976cd3824275045e4d176765956b7686b6))
* **pkg/diff:** preallocate Header parts, fix stale doc claim ([#422](https://github.com/home-operations/flate/issues/422)) ([4cc0d39](https://github.com/home-operations/flate/commit/4cc0d39203ec93606d4cfc8f8a27b11159712d6a))
* **pkg/helm:** inline writeAtomic shim, RWMutex for chart cache reads ([#445](https://github.com/home-operations/flate/issues/445)) ([c5bea76](https://github.com/home-operations/flate/commit/c5bea7659a8ca75f930aa1b0b7e0bfdd491a5a81))
* **pkg/image:** unexport unused IsImageRef, prune what-comments ([#424](https://github.com/home-operations/flate/issues/424)) ([04cc123](https://github.com/home-operations/flate/commit/04cc123b0ad882836b934a9c7d723ee5a60df031))
* **pkg/task:** drop fmt import, trim what-comments, fix stale doc ([#437](https://github.com/home-operations/flate/issues/437)) ([4888c68](https://github.com/home-operations/flate/commit/4888c681cab56593ced2e9cdd92282e4b5401dee))
* **pkg/values:** dedupe resource lookup, errors.AsType, simplify in-place merges ([#438](https://github.com/home-operations/flate/issues/438)) ([518f174](https://github.com/home-operations/flate/commit/518f1742aa26e77d4ce18e499aec4962a2bec74e))
* **source,blob:** WithSweepLock encapsulates GC mutex, Slot.Refresh consolidates Reset+Stage ([#465](https://github.com/home-operations/flate/issues/465)) ([6c4faf3](https://github.com/home-operations/flate/commit/6c4faf380a47016ec082ab83b7b089bc3971baa3))
* **source/blob:** drop dead PutReader, tighten lock scopes ([#433](https://github.com/home-operations/flate/issues/433)) ([26bbf95](https://github.com/home-operations/flate/commit/26bbf9559214d2ee48102b17a1b5f752a39a6b90))
* **source/external:** tighten docs, inline CutPrefix check ([#431](https://github.com/home-operations/flate/issues/431)) ([581936e](https://github.com/home-operations/flate/commit/581936ea7d3860f8b4a2d11097ccff0c1fa0085d))
* **source/git:** decouple verify from manifest type, drop ssh known_hosts tempfile ([#464](https://github.com/home-operations/flate/issues/464)) ([ea6aabc](https://github.com/home-operations/flate/commit/ea6aabcfc0a9e2e5764d004ea8772863b421135b))
* **source/git:** fix known-hosts temp leak, single-open marker, dedupe ignore+mark ([#449](https://github.com/home-operations/flate/issues/449)) ([5e50b52](https://github.com/home-operations/flate/commit/5e50b520dd30658d60082aea7b824f5a04289522))
* **source/oci:** slices.MaxFunc, single open for stat+read, pre-alloc ([#448](https://github.com/home-operations/flate/issues/448)) ([ba24794](https://github.com/home-operations/flate/commit/ba2479478b67eecc00fdbe018898bea79e1a7a67))
* **source:** extract SafeJoin to shared pkg/source/safepath ([#470](https://github.com/home-operations/flate/issues/470)) ([0056fd2](https://github.com/home-operations/flate/commit/0056fd226277229f2926cc4ea6166aa33525adfc))
* **store,depwait:** remove dead OtherActive, RLock AddListener, sort ListObjects ([#466](https://github.com/home-operations/flate/issues/466)) ([06fa4e1](https://github.com/home-operations/flate/commit/06fa4e19d53129580949be32f9b545cf50ce7226))
* **testutil:** consolidate mapLister test fixture, replace m[k]=v with maps.Copy ([#456](https://github.com/home-operations/flate/issues/456)) ([7b94c5a](https://github.com/home-operations/flate/commit/7b94c5a6e1536fdaec6d90037189d3041571de03))
* **testutil:** RSA-2048 → ECDSA P-256 for test certs ([#429](https://github.com/home-operations/flate/issues/429)) ([dc6d853](https://github.com/home-operations/flate/commit/dc6d8533608e9bb2fa5366f78e26c8fd08744e2b))
* WaitGroup.Go sweep across pkg/{blob,helm,store} tests ([#458](https://github.com/home-operations/flate/issues/458)) ([dc272ab](https://github.com/home-operations/flate/commit/dc272abff5a4eb121deec298b57680deb88d19b5))

## [0.1.24](https://github.com/home-operations/flate/compare/0.1.23...0.1.24) (2026-05-26)


### Bug Fixes

* **orchestrator:** cascade parent failures to render-emitted children ([#414](https://github.com/home-operations/flate/issues/414)) ([cc051d7](https://github.com/home-operations/flate/commit/cc051d7317b5dfd732612cb807e4dd67c70ca973))
* **source:** close mark-sweep race between gc.Sweep and Refs.Put ([#405](https://github.com/home-operations/flate/issues/405)) ([a045111](https://github.com/home-operations/flate/commit/a0451114381386bea0e4d318fa8bf1e48b61213a))
* **store:** stop the post-Failed grace timer on early return ([#400](https://github.com/home-operations/flate/issues/400)) ([bf6f8be](https://github.com/home-operations/flate/commit/bf6f8bec818eb5a280ba6f6b8d69669dee275989))


### Performance Improvements

* **depwait:** skip MissingGrace when Existence index is wired ([#412](https://github.com/home-operations/flate/issues/412)) ([f06c774](https://github.com/home-operations/flate/commit/f06c7742026233d82a3021233f27e6ecfe64e5fa))
* drop unnecessary syscalls and narrow the AddObject lock ([#404](https://github.com/home-operations/flate/issues/404)) ([75331cb](https://github.com/home-operations/flate/commit/75331cb7307ee0cf22ed6eb1507cf1bdad63e72c))
* **task:** YieldQuiescent decrements active for depwait yields ([#413](https://github.com/home-operations/flate/issues/413)) ([80781aa](https://github.com/home-operations/flate/commit/80781aaa75506b8676d8e01c089923dabe77a70e))


### Miscellaneous Chores

* no need for draft PR ([fc4522b](https://github.com/home-operations/flate/commit/fc4522b6090449fa99c5f4fcdd6e7053e85dfeb7))


### Code Refactoring

* **cli, controllers:** naming + file organization ([#403](https://github.com/home-operations/flate/issues/403)) ([090d075](https://github.com/home-operations/flate/commit/090d0750fe89570b699e5e82b978839e271997cf))
* **manifest:** dedup readKustomizeComponents ([#401](https://github.com/home-operations/flate/issues/401)) ([ad8de44](https://github.com/home-operations/flate/commit/ad8de440cd38ea14d5a7525142e490170f0e2b45))
* **source/git:** split verify and mirror into subpackages ([#408](https://github.com/home-operations/flate/issues/408)) ([23e764b](https://github.com/home-operations/flate/commit/23e764b78bc4ea9d801482b1862734601438cc3a))
* **source:** collapse fetcher authIdentity helpers into AuthIdentityFromRefs ([#406](https://github.com/home-operations/flate/issues/406)) ([75799fe](https://github.com/home-operations/flate/commit/75799fe01d150f1ab06b958763ae4de705ce5e3f))
* **source:** consolidate bucket subpackage files and fetcher boilerplate ([#407](https://github.com/home-operations/flate/issues/407)) ([afb252c](https://github.com/home-operations/flate/commit/afb252c4457951f0e7819bbb53892453f411dcf0))
* **source:** give every fetcher subpackage a controller-shaped layout ([#409](https://github.com/home-operations/flate/issues/409)) ([d0e7fb1](https://github.com/home-operations/flate/commit/d0e7fb183e41098346a20e79fdd7bae4f931d012))
* **source:** shared NewHTTPTransport factory ([#402](https://github.com/home-operations/flate/issues/402)) ([e92181d](https://github.com/home-operations/flate/commit/e92181d67ec338cacba2e443078e77c8a5f31864))

## [0.1.23](https://github.com/home-operations/flate/compare/0.1.22...0.1.23) (2026-05-26)


### Bug Fixes

* **baseline:** --base main falls back to origin/main on CI checkouts ([#397](https://github.com/home-operations/flate/issues/397)) ([53ccce0](https://github.com/home-operations/flate/commit/53ccce0aa739ebc2d9b11b5c96b34ce394102af2))
* **loader:** materialize Component configMapGenerator/secretGenerator ([#399](https://github.com/home-operations/flate/issues/399)) ([48e53b2](https://github.com/home-operations/flate/commit/48e53b2774fc10599971c982e70b3a349fe159bc))

## [0.1.22](https://github.com/home-operations/flate/compare/0.1.21...0.1.22) (2026-05-26)


### Features

* **baseline:** content-addressed slot at &lt;cache&gt;/baselines/&lt;sha&gt;/ ([#379](https://github.com/home-operations/flate/issues/379)) ([22da538](https://github.com/home-operations/flate/commit/22da5381e60caea7dfe66ce91a1f737dba2bf60a))
* **cli:** --base opts into changed-only mode on build/get/test too ([#363](https://github.com/home-operations/flate/issues/363)) ([6d7b3cb](https://github.com/home-operations/flate/commit/6d7b3cb65127e9a1c63dc5f22b449c91e7d06868))
* **helm:** in-process HelmRepository index cache ([#380](https://github.com/home-operations/flate/issues/380)) ([0eb44d8](https://github.com/home-operations/flate/commit/0eb44d8a8ad2ce282dfac5f9b05c796880c5be75))
* **helm:** OCI pulls yield the worker-pool slot ([#366](https://github.com/home-operations/flate/issues/366)) ([cdb8d2b](https://github.com/home-operations/flate/commit/cdb8d2b7bdfb2238175a97215f36c2b371da9513))
* **kustomize:** StagingCache sweeps stale flate-stage-* leftovers on open ([#367](https://github.com/home-operations/flate/issues/367)) ([bf3d79a](https://github.com/home-operations/flate/commit/bf3d79a06e562d1aa727ba1d1ff0eda93cc42ad2))
* **orchestrator:** incremental cycle detection on render-emit ([#365](https://github.com/home-operations/flate/issues/365)) ([b29a9fc](https://github.com/home-operations/flate/commit/b29a9fc0d716ad075aab94c1ea0e6a962be954cf))
* **orchestrator:** XDG-compliant default cache root ([#376](https://github.com/home-operations/flate/issues/376)) ([2f1aa46](https://github.com/home-operations/flate/commit/2f1aa461a37da09649ce9ed52c11596c8d44e2a8))
* **source,cli:** cache GC + flate cache gc command ([#383](https://github.com/home-operations/flate/issues/383)) ([07a570b](https://github.com/home-operations/flate/commit/07a570bdbf1d7c5b68168747870763b60e920bc0))
* **source/blob:** CAS store + refs; migrate helm tarball cache ([#382](https://github.com/home-operations/flate/issues/382)) ([96ed561](https://github.com/home-operations/flate/commit/96ed56163f8f32a0b6ba67b416bf66096557c42e))
* **source/git:** bare-mirror + tree-walk worktree materialization ([#381](https://github.com/home-operations/flate/issues/381)) ([7fa2999](https://github.com/home-operations/flate/commit/7fa2999e320e425b496c93ffe5bcdb711e0412de))
* **source:** auth identity in cache slot key ([#377](https://github.com/home-operations/flate/issues/377)) ([f11db0e](https://github.com/home-operations/flate/commit/f11db0ef41cbf47e48044fcfd98d40a464ab3c2f))


### Bug Fixes

* **change:** chartRef→HelmChart BFS + git/walker symlink parity ([#356](https://github.com/home-operations/flate/issues/356)) ([937ffe2](https://github.com/home-operations/flate/commit/937ffe2319bb924f8f9cc5fed40395a8f17e4957))
* **cli:** get -o yaml/json output now deterministic ([#375](https://github.com/home-operations/flate/issues/375)) ([6bbfd7f](https://github.com/home-operations/flate/commit/6bbfd7fb03d95621fc532968930688e0b7a262b7))
* **controllers:** fingerprint dedup replays emit side-effects (KS + HR) ([#361](https://github.com/home-operations/flate/issues/361)) ([58aa822](https://github.com/home-operations/flate/commit/58aa822bef1ccfc33783676fc000c75c8404e66b))
* **controllers:** Recover re-raises + Refire type-miss writes terminal + depwait cap sentinel ([#359](https://github.com/home-operations/flate/issues/359)) ([4bb2ccb](https://github.com/home-operations/flate/commit/4bb2ccb3d4e6183588f3a2feaf15343084d44406))
* **depwait:** CEL eval errors are transient, not terminal ([#374](https://github.com/home-operations/flate/issues/374)) ([f534ef3](https://github.com/home-operations/flate/commit/f534ef3646d586315ea30a214de66d529150104e))
* **diff:** pair key includes Parent.Path to disambiguate same-named KSes ([#353](https://github.com/home-operations/flate/issues/353)) ([7c6bca8](https://github.com/home-operations/flate/commit/7c6bca85e5cc3f52c40ef5ce01a0557937e0a11e))
* **discovery:** repo-root threading + bootstrap GR survival + worktree detect + OCI alias ns ([#358](https://github.com/home-operations/flate/issues/358)) ([77c63f4](https://github.com/home-operations/flate/commit/77c63f489713712a7ac82eea07ee5027bf3b3fa7))
* **helm:** chart cache returns per-render clone to avoid ProcessDependencies race ([#351](https://github.com/home-operations/flate/issues/351)) ([729bd7c](https://github.com/home-operations/flate/commit/729bd7cf19748c115820e5cabea5fd5b9cc861fb))
* **helm:** OCI fallback cache key includes tag/digest ([#384](https://github.com/home-operations/flate/issues/384)) ([42fe304](https://github.com/home-operations/flate/commit/42fe3045252f048c5099328102ff2f7a1c6383d7))
* **helmrelease:** await chartRef-HelmChart + non-optional valuesFrom before Prepare ([#360](https://github.com/home-operations/flate/issues/360)) ([809af62](https://github.com/home-operations/flate/commit/809af62bae10317735bd04eb47ae268b79ad6b8d))
* **loader:** scan-root ignore + relax kustomization stat + apiVersion gate on Component ([#357](https://github.com/home-operations/flate/issues/357)) ([773dbf6](https://github.com/home-operations/flate/commit/773dbf69d5de4c071fe36797071510680626f5ee))
* **manifest:** Clone deep-copies embedded helm/kustomize spec ([#350](https://github.com/home-operations/flate/issues/350)) ([b158ae2](https://github.com/home-operations/flate/commit/b158ae2aa88a1d5234ea6caf49197af54ec1b695))
* **orchestrator,depwait:** wave-3 leftover audit findings ([#364](https://github.com/home-operations/flate/issues/364)) ([06228c3](https://github.com/home-operations/flate/commit/06228c349be008281298acf6213c7fc9709b6352))
* **source:** wrap missing-secret errors + don't silence cosign keyless warning ([#352](https://github.com/home-operations/flate/issues/352)) ([b0f3e8d](https://github.com/home-operations/flate/commit/b0f3e8dd8ec942c981a90d37dcd9aded9816576b))
* **store:** AddListener flush=false serializes via s.mu to avoid missed events ([#354](https://github.com/home-operations/flate/issues/354)) ([d4cb788](https://github.com/home-operations/flate/commit/d4cb78857e968fdfaa5b48de0e750c38fbae9492))
* third-pass batch 2 — helm/loader/manifest correctness ([#369](https://github.com/home-operations/flate/issues/369)) ([b04b005](https://github.com/home-operations/flate/commit/b04b005e0f40233a140d0f82b30d0dccf5585a70))
* third-pass batch 3 — store/values/baseline correctness ([#370](https://github.com/home-operations/flate/issues/370)) ([4d162bc](https://github.com/home-operations/flate/commit/4d162bc1de3735374c4d1ac7c1838d70b3a552d1))
* third-pass batch 4 — discovery loadAt + RS warn + kustomize transient retry ([#371](https://github.com/home-operations/flate/issues/371)) ([7d90b2c](https://github.com/home-operations/flate/commit/7d90b2c7a6c0f7ab6768f455d3a1ef1ae263b82e))
* third-pass regressions from PR [#349](https://github.com/home-operations/flate/issues/349) / [#361](https://github.com/home-operations/flate/issues/361) / [#365](https://github.com/home-operations/flate/issues/365) ([#368](https://github.com/home-operations/flate/issues/368)) ([a8396d9](https://github.com/home-operations/flate/commit/a8396d9e94e0e92a958111d18a40f2b6e8e788dc))


### Performance Improvements

* **kustomize:** hardlink stage files instead of byte-copying ([#385](https://github.com/home-operations/flate/issues/385)) ([d69651e](https://github.com/home-operations/flate/commit/d69651eb5cb34386fed54247055941d373a9e1dc))
* **kustomize:** parallel stage copy via worker pool ([#395](https://github.com/home-operations/flate/issues/395)) ([0bc780b](https://github.com/home-operations/flate/commit/0bc780b50b97c50db0a1c2f8e486285491c0d3be))
* **source/gittree:** parallel + unified tree-walk materialization ([#393](https://github.com/home-operations/flate/issues/393)) ([cdf5913](https://github.com/home-operations/flate/commit/cdf5913a86487252341e37e41c05950964bd3aee))


### Code Refactoring

* **source/atomic:** one helper, four file-write sites ([#389](https://github.com/home-operations/flate/issues/389)) ([54a58fe](https://github.com/home-operations/flate/commit/54a58fefbd3d842b0a2d1a5f91215979ed1bb6f3))
* **source/gc:** mark-sweep — live refs preserve their blobs ([#391](https://github.com/home-operations/flate/issues/391)) ([404e76d](https://github.com/home-operations/flate/commit/404e76db39e6b3825b1d952c9f8cf2d36ab37e09))
* **source:** cacheroot.Layout owns all cache paths ([#386](https://github.com/home-operations/flate/issues/386)) ([1c167ae](https://github.com/home-operations/flate/commit/1c167ae72090e3a088c56d5157586500873ad205))
* **source:** final cache polish ([#390](https://github.com/home-operations/flate/issues/390)) ([0db7dfc](https://github.com/home-operations/flate/commit/0db7dfcde75435062c1707c7c4e013c8e5275316))
* **source:** unify per-key locks via internal/keylock ([#392](https://github.com/home-operations/flate/issues/392)) ([eb1f6d0](https://github.com/home-operations/flate/commit/eb1f6d06efa439c0e59887b78ccb2ce50e259939))

## [0.1.21](https://github.com/home-operations/flate/compare/0.1.20...0.1.21) (2026-05-25)


### Features

* **diff:** auto-detect baseline via git so --path-orig is optional ([#349](https://github.com/home-operations/flate/issues/349)) ([9f9cef6](https://github.com/home-operations/flate/commit/9f9cef6d3a45918a2b093c5804cb6ac34376f85d))
* **orchestrator,helm:** widen and surface the EnableOCI=false warnings ([#327](https://github.com/home-operations/flate/issues/327)) ([2e4fe7b](https://github.com/home-operations/flate/commit/2e4fe7b0c6cb1080af43634faaf12192e63ea91a))
* **orchestrator:** warn at bootstrap on OCI HelmRepository + SecretRef ([#329](https://github.com/home-operations/flate/issues/329)) ([2500e18](https://github.com/home-operations/flate/commit/2500e188b8c049e5cdb9f1fa9a3b302fe5bb53b6))


### Bug Fixes

* **cleanup:** bucket parallel + replaceValueAtPath panic + small fixes ([#340](https://github.com/home-operations/flate/issues/340)) ([cde049e](https://github.com/home-operations/flate/commit/cde049e4f049be3f7111cad4858eac7e9c3c75d0))
* **deferred:** six small bugs + fragility traps from the audit ([#341](https://github.com/home-operations/flate/issues/341)) ([9723540](https://github.com/home-operations/flate/commit/97235408f8748c8555b1091db0828140cb0e66cd))
* **depwait,task:** event-driven quiescence + missed-wake + cache + stack ([#333](https://github.com/home-operations/flate/issues/333)) ([2d59c7c](https://github.com/home-operations/flate/commit/2d59c7c59451924366e3f1f43ab39e844f46a91f))
* **kustomize:** yaml.Node edit + stage-root lock + cancel-safe FetchRemote ([#334](https://github.com/home-operations/flate/issues/334)) ([60044f4](https://github.com/home-operations/flate/commit/60044f4cf4fbaf7e36b6edd14b4e35bb129f9f07))
* **loader:** graph-driven walk skips orphan subtrees, not just same-dir orphans ([#347](https://github.com/home-operations/flate/issues/347)) ([9314106](https://github.com/home-operations/flate/commit/93141067db765733a810624ed91c11129a5335ed))
* **loader:** skip orphan YAMLs not referenced by their parent kustomization.yaml ([#346](https://github.com/home-operations/flate/issues/346)) ([09d10ed](https://github.com/home-operations/flate/commit/09d10ede7632e87d845032f6613a93aa7958712d))
* **orchestrator,cli:** ctx-aware drain + emit/run error join + flag validation ([#339](https://github.com/home-operations/flate/issues/339)) ([10ef9ae](https://github.com/home-operations/flate/commit/10ef9ae979cb8a7ed5c0ad3183b706fe64f9854a))
* **orchestrator:** widen change.Detect to per-side .git root for sibling-checkout diffs ([#348](https://github.com/home-operations/flate/issues/348)) ([522dfe1](https://github.com/home-operations/flate/commit/522dfe197ec759b6d9d8a11a05c4358e12f65f2f))
* **source/bucket:** reset slot before walk to avoid ghost files ([#324](https://github.com/home-operations/flate/issues/324)) ([24ed0a0](https://github.com/home-operations/flate/commit/24ed0a07684e167e544d593778036c96c2435b5f))
* **source/oci,helm:** offline-safe cosign verify + chart cache invalidation ([#336](https://github.com/home-operations/flate/issues/336)) ([2f91d24](https://github.com/home-operations/flate/commit/2f91d240fba092cc7bfb2e1cd72d9ed361c4a749))
* **source/oci:** atomic .flate-digest write + format validation + staged-layer sentinel ([#326](https://github.com/home-operations/flate/issues/326)) ([768ff9d](https://github.com/home-operations/flate/commit/768ff9d682a5e26fab120ac2cf367a35cc630b2f))
* **source/oci:** reset cache slot when leftover OCI layout artifacts found ([#322](https://github.com/home-operations/flate/issues/322)) ([9737dfc](https://github.com/home-operations/flate/commit/9737dfc58c3cce69d52f9ac8b531dae8d889e2cf))
* **source/oci:** tighten state machine — drop ref.Digest fallback, defer reset, cleanup zero-layer ([#325](https://github.com/home-operations/flate/issues/325)) ([cd3a87a](https://github.com/home-operations/flate/commit/cd3a87adf8ba5579c78dea79ecf7b08fdab4ab12))
* **store:** close listener-race + phantom-failure + index-drift gaps ([#331](https://github.com/home-operations/flate/issues/331)) ([1187bde](https://github.com/home-operations/flate/commit/1187bde8b36e0c04b7e00163cf09eb445bd22482))


### Performance Improvements

* **change,loader:** git-aware Detect + same-size hashing + components fold ([#332](https://github.com/home-operations/flate/issues/332)) ([3ba52d2](https://github.com/home-operations/flate/commit/3ba52d23a55f3a9e2b37b017ddbdd5e61b7d9ec1))
* **change,values:** memoize ownership lookups + in-place values merge ([#338](https://github.com/home-operations/flate/issues/338)) ([0aeee7c](https://github.com/home-operations/flate/commit/0aeee7c072e89d7f9e37e55976984522569204bc))
* **discovery,orchestrator:** skip converged RSes + parallel post-run RS render ([#343](https://github.com/home-operations/flate/issues/343)) ([ff1bee0](https://github.com/home-operations/flate/commit/ff1bee03b1bd27fe7ae077ee0be8a10e1bd53e2d))


### Code Refactoring

* **controllers:** base.Controller.Await + nil-safe filter + status preservation ([#337](https://github.com/home-operations/flate/issues/337)) ([532bce5](https://github.com/home-operations/flate/commit/532bce55e53f0ed9da8909dfc62cbf214cbfc99f))
* **helm:** unify OCI pulls through source/oci.Fetcher (the big lift) ([#345](https://github.com/home-operations/flate/issues/345)) ([f9ab065](https://github.com/home-operations/flate/commit/f9ab065cbbe813ca50df7e855bc7d83eebf01648))
* **manifest:** single-source helpers for RepoName, NamespacedName, secret-ref validation ([#335](https://github.com/home-operations/flate/issues/335)) ([c408973](https://github.com/home-operations/flate/commit/c408973e6c4802c1a37ffe922296db521a52048b))
* **source/cache:** atomic-rename slot staging ([#330](https://github.com/home-operations/flate/issues/330)) ([3d9ed6e](https://github.com/home-operations/flate/commit/3d9ed6e09bbccce11269b4e8b0e7150e0cfd948e))
* **source/cache:** slugify by repo name, not by tag ([#328](https://github.com/home-operations/flate/issues/328)) ([1a460fd](https://github.com/home-operations/flate/commit/1a460fdaf02a2d97dd0cfa4580117c115651f432))
* **source:** generic TypedFetcher[T] + Wrap adapter ([#344](https://github.com/home-operations/flate/issues/344)) ([1e61bc1](https://github.com/home-operations/flate/commit/1e61bc1207c6983141e9d9c4da679d234a5228da))

## [0.1.20](https://github.com/home-operations/flate/compare/0.1.19...0.1.20) (2026-05-25)


### Features

* **test:** hide SKIPPED rows by default; --show-skipped opts in ([#321](https://github.com/home-operations/flate/issues/321)) ([fde11d7](https://github.com/home-operations/flate/commit/fde11d737b5fd274436d326179a5b70b90218c97))


### Bug Fixes

* add auto-completions for brew ([aa52b9a](https://github.com/home-operations/flate/commit/aa52b9a17e3c8b7d314fd54cb4cbbac3e3edb895))

## [0.1.19](https://github.com/home-operations/flate/compare/0.1.18...0.1.19) (2026-05-25)


### Features

* **depwait:** add RenderInflight quiescence signal for step-2 fail-fast ([#319](https://github.com/home-operations/flate/issues/319)) ([10bf86e](https://github.com/home-operations/flate/commit/10bf86efef77d38587c0f6ec934f16b4d2d670ee))


### Bug Fixes

* **change:** close AddEmitted race + symmetric BFS upgrade + cascade depth test ([#312](https://github.com/home-operations/flate/issues/312)) ([41d025e](https://github.com/home-operations/flate/commit/41d025e2c82449b27ce67d1e842932523ebb7b4c))
* **change:** stop keep-set cascade through ancestor-only renders ([#308](https://github.com/home-operations/flate/issues/308)) ([76e065a](https://github.com/home-operations/flate/commit/76e065a14840458b25744863d65e3b84e1286114))
* **depwait:** cap step-2 render-only wait at RenderProducingTimeout ([#313](https://github.com/home-operations/flate/issues/313)) ([955b5bd](https://github.com/home-operations/flate/commit/955b5bde1a6f749596b08f7e7130793e41e5f786))
* **depwait:** route step-2 long-wait error through classify + missing tests ([#311](https://github.com/home-operations/flate/issues/311)) ([ac04f29](https://github.com/home-operations/flate/commit/ac04f2908e9fb05edb9e207778741cc5421143dc))
* **depwait:** wait beyond grace for render-only deps still in flight ([#310](https://github.com/home-operations/flate/issues/310)) ([00636bb](https://github.com/home-operations/flate/commit/00636bb553689d4fa36531b1cfa156aa54be0b19))


### Documentation

* **change:** sync filter doc comments with AddEmitted-based runtime API ([#315](https://github.com/home-operations/flate/issues/315)) ([4332576](https://github.com/home-operations/flate/commit/433257605546146a4db4896d0bf2638941685a0b))


### Code Refactoring

* **change:** unexport Filter.Add → addUngated ([#317](https://github.com/home-operations/flate/issues/317)) ([478307b](https://github.com/home-operations/flate/commit/478307b528c3ca9e9a28eada3f1dd1ab29283aad))
* **depwait:** bundle existence closures into ExistenceLookup interface ([#316](https://github.com/home-operations/flate/issues/316)) ([e0329fe](https://github.com/home-operations/flate/commit/e0329fe9c29d106c42942f5903deef3e09c403a8))
* **depwait:** rename IsKnown → IsFileIndexed for clarity ([#314](https://github.com/home-operations/flate/issues/314)) ([429a526](https://github.com/home-operations/flate/commit/429a526ec4aa5a4b5b88589836abb05d1ad68a15))

## [0.1.18](https://github.com/home-operations/flate/compare/0.1.17...0.1.18) (2026-05-25)


### Features

* **helm:** route OCI chart pulls through source.oci.Fetcher ([#269](https://github.com/home-operations/flate/issues/269)) ([be0feca](https://github.com/home-operations/flate/commit/be0fecaaa56c0ec287695a3158b536da6e4a3090))


### Bug Fixes

* **change:** Filter.Add walks transitiveDeps + refires source listeners ([#260](https://github.com/home-operations/flate/issues/260)) ([#261](https://github.com/home-operations/flate/issues/261)) ([eaca342](https://github.com/home-operations/flate/commit/eaca3422bfc70f1d260c6ed785ecb2eb5d522079))
* **discovery:** alias bootstrap OCIRepository sources too ([#263](https://github.com/home-operations/flate/issues/263)) ([0ac5ffe](https://github.com/home-operations/flate/commit/0ac5ffe07afe5814c3f495d913364a4ef56f93b4))
* **orchestrator:** assert WithFetcher pre-Bootstrap + warn on ignored OCI features ([#276](https://github.com/home-operations/flate/issues/276)) ([139bf0f](https://github.com/home-operations/flate/commit/139bf0ff3946990a18ecf3db1e0f9d67c6b4dd99))
* **source/oci:** write blobs without title annotation to slot ([#265](https://github.com/home-operations/flate/issues/265)) ([c395f08](https://github.com/home-operations/flate/commit/c395f08676afef8987a52b65e09fa3a6de018be5))
* **store:** close AddListener double-fire race + harden Refire/Snapshot ([#274](https://github.com/home-operations/flate/issues/274)) ([c814820](https://github.com/home-operations/flate/commit/c814820d381f7fd0097cf6e4df35bb3719c0742a))


### Miscellaneous Chores

* **diff:** default-strip checksum/secret annotation ([#264](https://github.com/home-operations/flate/issues/264)) ([8549315](https://github.com/home-operations/flate/commit/85493158b15f7fab6b872708398e2612f75ddf54))
* use existing error everywhere ([#307](https://github.com/home-operations/flate/issues/307)) ([6fc0e7b](https://github.com/home-operations/flate/commit/6fc0e7bb100921ff6cdd1aa728eddba3f725f542))


### Code Refactoring

* adopt modern stdlib helpers (maps.Copy, bytes.SplitSeq, errors.AsType) ([#299](https://github.com/home-operations/flate/issues/299)) ([ae68e91](https://github.com/home-operations/flate/commit/ae68e9151a10d2b6db4d0c72f9a8a94c44dd1db1))
* adopt slices.Sorted(maps.Keys()) for collect+sort patterns ([#303](https://github.com/home-operations/flate/issues/303)) ([dec80b4](https://github.com/home-operations/flate/commit/dec80b476d330042e064c9b8179c78605afaa370))
* **cli:** collapse rendersHelm to slices.Contains; inline single-kind sites ([#297](https://github.com/home-operations/flate/issues/297)) ([c1cb490](https://github.com/home-operations/flate/commit/c1cb49015b552a78ba0cfc07a498d3cf633f4d73))
* cmp.Or sweep — two more comparator cascades ([#289](https://github.com/home-operations/flate/issues/289)) ([55b53dd](https://github.com/home-operations/flate/commit/55b53dd97db209dace231ddfe37a399e35a480b6))
* cmp.Or sweep [#4](https://github.com/home-operations/flate/issues/4) across manifest, oci, cli, errors ([#293](https://github.com/home-operations/flate/issues/293)) ([ea13e46](https://github.com/home-operations/flate/commit/ea13e46b8f03bab394ad3ff743899476c45c6bc9))
* cmp.Or sweep [#5](https://github.com/home-operations/flate/issues/5) — chartRef version, dep error, git TLS ([#294](https://github.com/home-operations/flate/issues/294)) ([b0860b1](https://github.com/home-operations/flate/commit/b0860b18ee932123588e88fb4b87188589929b67))
* cmp.Or sweep round 3 — 7 more zero-fallback sites ([#291](https://github.com/home-operations/flate/issues/291)) ([aaf0b79](https://github.com/home-operations/flate/commit/aaf0b7972ea71465a642b2420caed1b9a267303c))
* dedup kustomize-builder filename list to manifest.KustomizeBuilderFilenames ([#283](https://github.com/home-operations/flate/issues/283)) ([5311c03](https://github.com/home-operations/flate/commit/5311c03a3e5ab38055ecb41c0f79f73fb7fc3424))
* dedup RS render-doc identity key (resourceset.DedupKey) ([#285](https://github.com/home-operations/flate/issues/285)) ([fec5616](https://github.com/home-operations/flate/commit/fec5616ebe4d99377f47fdd1b45aeda239e4722b))
* **depwait:** extract tryReadyExpr for the 3-site eval pattern ([#286](https://github.com/home-operations/flate/issues/286)) ([d46f86d](https://github.com/home-operations/flate/commit/d46f86d08e6f3bc18f8eb7ff420efca0cb2ee9d3))
* **diff:** collapse 6-level sort cascade with cmp.Or ([#288](https://github.com/home-operations/flate/issues/288)) ([f209e65](https://github.com/home-operations/flate/commit/f209e6523c50b0a6ea1c3e1d7dcf96a74e6b557f))
* **discovery:** drop dead alreadyAliased skip-set + free knownSourceIDs ([#271](https://github.com/home-operations/flate/issues/271)) ([6ad8555](https://github.com/home-operations/flate/commit/6ad8555adfb9b0f0848b23e8870722c31e746756))
* **discovery:** simplify stripDotSlash + pathUnderRoot with stdlib ([#284](https://github.com/home-operations/flate/issues/284)) ([a180d4a](https://github.com/home-operations/flate/commit/a180d4a41041ca45c61128e900e8ee7215408b25))
* **discovery:** split aliasBootstrapSources into focused passes ([#268](https://github.com/home-operations/flate/issues/268)) ([44f6014](https://github.com/home-operations/flate/commit/44f60148aed4d29e15ad867d136ed70d7dfd290a))
* **helm:** add private secretGetter() snapshot helper ([#302](https://github.com/home-operations/flate/issues/302)) ([b09095c](https://github.com/home-operations/flate/commit/b09095c8fa506660e33c669181f6cbfb2a024caf))
* **helm:** observability + semver guard + distinct ambiguous error ([#273](https://github.com/home-operations/flate/issues/273)) ([e1091d2](https://github.com/home-operations/flate/commit/e1091d2d420d79b67bc3473bdfc8ed7e87a20793))
* **helm:** route resolver snapshots through Client.Resolver() ([#301](https://github.com/home-operations/flate/issues/301)) ([deaf8e0](https://github.com/home-operations/flate/commit/deaf8e05801c40a5f61e3efc323eb50583e914b1))
* **helm:** split repo.go into auth.go + oci_chart.go ([#287](https://github.com/home-operations/flate/issues/287)) ([eb65b65](https://github.com/home-operations/flate/commit/eb65b6572c74ac87e8ca67959aee90536ab3ad88))
* **loader:** collapse BuildParentIndex wrappers + drop one-line helper ([#278](https://github.com/home-operations/flate/issues/278)) ([a9c603c](https://github.com/home-operations/flate/commit/a9c603ca83902b77e976a22004f5639e22933257))
* **loader:** export NormalizePrefix and reuse in orchestrator ([#296](https://github.com/home-operations/flate/issues/296)) ([0941e1b](https://github.com/home-operations/flate/commit/0941e1b26a7089e3b1ebf2f9eff50c7ef5358f3c))
* **manifest/helmrelease:** replace 5 zero-fallback patterns with cmp.Or ([#290](https://github.com/home-operations/flate/issues/290)) ([9679b60](https://github.com/home-operations/flate/commit/9679b606f3411ef3fe8562ea6ab6e64746cb8a3d))
* **manifest:** centralize doc field reads behind DocKind/DocAPIVersion/DocMetadata ([#298](https://github.com/home-operations/flate/issues/298)) ([a8e3a18](https://github.com/home-operations/flate/commit/a8e3a186c6c0799bd21e9eca451aa00f23396b4c))
* **manifest:** factor stripObjectMetadata + stripMetadataInList helpers ([#281](https://github.com/home-operations/flate/issues/281)) ([a85fa5c](https://github.com/home-operations/flate/commit/a85fa5c3b877070ec1a01209bab02b3ddef25695))
* **manifest:** unify EnsureMetadata + MergeStringMap helpers ([#300](https://github.com/home-operations/flate/issues/300)) ([7bc7e2e](https://github.com/home-operations/flate/commit/7bc7e2e3d1b6a3a2a2a091b2dce0813b01ff9ee7))
* **orchestrator:** cacheRoot + RS sourceFile lookup → cmp.Or ([#292](https://github.com/home-operations/flate/issues/292)) ([7f8ed56](https://github.com/home-operations/flate/commit/7f8ed5688422e17b075b0f6fd1b8ae7741d14053))
* **orchestrator:** use slices.SortedFunc + NamedResource.Compare in cycles ([#304](https://github.com/home-operations/flate/issues/304)) ([c90e16e](https://github.com/home-operations/flate/commit/c90e16e95d8a2edc70a1b34f05085336eecc1220))
* **resourceset:** factor ensureMetadata for the two writer paths ([#282](https://github.com/home-operations/flate/issues/282)) ([6a6cb7e](https://github.com/home-operations/flate/commit/6a6cb7e60a3890b7d00587c15b88687868fa4cb5))
* route same-kind comparators through NamedResource.Compare ([#305](https://github.com/home-operations/flate/issues/305)) ([37553a7](https://github.com/home-operations/flate/commit/37553a7c674e711aec3723e812ce76666a01b284))
* simplify OCIRepository.Version() — drop unused error, use cmp.Or ([#295](https://github.com/home-operations/flate/issues/295)) ([5036f5a](https://github.com/home-operations/flate/commit/5036f5a5b0c3a61288ecffaa79fd93794822e5f5))
* **source/bucket:** split bucket.go into endpoint.go + transport.go + traversal.go ([#279](https://github.com/home-operations/flate/issues/279)) ([57a9293](https://github.com/home-operations/flate/commit/57a92930b61ff92d6318691585ab51bb734e6519))
* **source/git:** inline refStr fallback with cmp.Or ([#306](https://github.com/home-operations/flate/issues/306)) ([d6bdaf8](https://github.com/home-operations/flate/commit/d6bdaf8c66da99c6b949ab74edc5d84dbea9a549))
* **source/git:** split git.go into auth.go + tls.go ([#277](https://github.com/home-operations/flate/issues/277)) ([ee47c39](https://github.com/home-operations/flate/commit/ee47c39a12628d767cf15db90160f50947044e3a))
* **source/oci:** harden cache invariants + extractTarGz traversal ([#272](https://github.com/home-operations/flate/issues/272)) ([25607bf](https://github.com/home-operations/flate/commit/25607bfb77ec818051deaba176bfcf79e1658132))
* **source/oci:** split out flatFallbackStorage + tighten test ([#266](https://github.com/home-operations/flate/issues/266)) ([8a40b3e](https://github.com/home-operations/flate/commit/8a40b3e2c6775ea4dc31ebc0390df966fe95bea4))
* **source/oci:** use OCI Image Layout via content/oci.Store ([#267](https://github.com/home-operations/flate/issues/267)) ([ea6fac3](https://github.com/home-operations/flate/commit/ea6fac3a60030c2e5dc32c8cfcf13fb7c12ecbe1))
* **source/oci:** zip-bomb cap, cosign Warn, multi-layer ambiguity guard ([#275](https://github.com/home-operations/flate/issues/275)) ([491a27f](https://github.com/home-operations/flate/commit/491a27f7991d87a3290208af1f14c619618250ae))
* **store:** encapsulate status reset inside Refire ([#270](https://github.com/home-operations/flate/issues/270)) ([88d1ad0](https://github.com/home-operations/flate/commit/88d1ad0dbfc9bee859077bdc018d33335c58cb5e))

## [0.1.17](https://github.com/home-operations/flate/compare/0.1.16...0.1.17) (2026-05-24)


### Bug Fixes

* action script point to wrong arch ([b4ddab2](https://github.com/home-operations/flate/commit/b4ddab21592f35f76372e2f4948b2f5d5c8c8206))


### Code Refactoring

* **controllers:** split KS + HR controllers into per-concern files ([#253](https://github.com/home-operations/flate/issues/253)) ([52203c1](https://github.com/home-operations/flate/commit/52203c1210ee95b39083209c90d31311eca6b03c))
* **discovery:** split discovery.go into per-concern files ([#256](https://github.com/home-operations/flate/issues/256)) ([f28a0b4](https://github.com/home-operations/flate/commit/f28a0b44a60100ebb7bb84e058612c14feed9048))
* **orchestrator:** split orchestrator.go into finalize.go + resourceset.go ([#255](https://github.com/home-operations/flate/issues/255)) ([49d00f9](https://github.com/home-operations/flate/commit/49d00f98310ea8935c3eaf172b6efdb8646dfba0))
* **resourceset:** rename doc.go → output.go to avoid package-doc collision ([#258](https://github.com/home-operations/flate/issues/258)) ([8c3b940](https://github.com/home-operations/flate/commit/8c3b940f9f3c7652753bdb80da169c30e5857cbd))
* **resourceset:** split render.go into per-concern files ([#257](https://github.com/home-operations/flate/issues/257)) ([c7c51fe](https://github.com/home-operations/flate/commit/c7c51fe2fd81a1c974ba4f68fb30fc471d176961))
* **store:** typed Get[T] + ListAs[T] generic helpers ([#259](https://github.com/home-operations/flate/issues/259)) ([d6610f5](https://github.com/home-operations/flate/commit/d6610f56948d879b51700a65ef31915148bd1c3c))

## [0.1.16](https://github.com/home-operations/flate/compare/0.1.15...0.1.16) (2026-05-24)


### Bug Fixes

* **controllers/helmrelease:** refresh HR after dependsOn wait ([#251](https://github.com/home-operations/flate/issues/251)) ([55c46ac](https://github.com/home-operations/flate/commit/55c46ac0111e4fa1ed7d5fd0902528d33395a002))

## [0.1.15](https://github.com/home-operations/flate/compare/0.1.14...0.1.15) (2026-05-24)


### Features

* **cli:** add `flate diff all` for combined KS + HR diff ([#250](https://github.com/home-operations/flate/issues/250)) ([b6e2092](https://github.com/home-operations/flate/commit/b6e2092b4df1a58a4aa87c3f9aac48118e7b2888))


### Bug Fixes

* **discovery:** alias in-tree GitRepository whose URL matches working tree ([#248](https://github.com/home-operations/flate/issues/248)) ([306d7b4](https://github.com/home-operations/flate/commit/306d7b40a5ebb3561887a355b96e9d11666d3a57))


### Miscellaneous Chores

* cleanup pass after render-driven discovery migration ([#243](https://github.com/home-operations/flate/issues/243)) ([0413460](https://github.com/home-operations/flate/commit/041346096b1ea39fe0642faeb1282410639b1bcd))
* round-2 correctness pass (sentinel wrapping + honest failures) ([#245](https://github.com/home-operations/flate/issues/245)) ([af27f3e](https://github.com/home-operations/flate/commit/af27f3ea8cd76f1daf6b657320d4c93b6634359d))
* round-3 polish — complete Clone helpers + coverage gaps ([#246](https://github.com/home-operations/flate/issues/246)) ([979f644](https://github.com/home-operations/flate/commit/979f644c821d9c48a7a5d7d43917d5f5356d7491))
* round-4 hardening — version stamp + HTTP cap + path-traversal guard ([#247](https://github.com/home-operations/flate/issues/247)) ([d35fc0b](https://github.com/home-operations/flate/commit/d35fc0bf2bad96cce86682af2d058dfccb815372))
* round-6 — harden normalizeGitURL + integration coverage ([#249](https://github.com/home-operations/flate/issues/249)) ([08524a8](https://github.com/home-operations/flate/commit/08524a84c872e2438bbbda0ae8ec57adb797ab30))

## [0.1.14](https://github.com/home-operations/flate/compare/0.1.13...0.1.14) (2026-05-24)


### Features

* **controllers/kustomization:** substituteFrom ConfigMap as depwait edge (step 3/4) ([#240](https://github.com/home-operations/flate/issues/240)) ([10c605f](https://github.com/home-operations/flate/commit/10c605f740b1fcfa4683211e939d15697c051184))
* **loader:** DiscoveryOnly with lazy promotion (step 4/4) ([#242](https://github.com/home-operations/flate/issues/242)) ([f66cb59](https://github.com/home-operations/flate/commit/f66cb597cffd6f333fa64033584593b200376a91))


### Bug Fixes

* **manifest:** Flux-native envsubst handling — defaults + template skip ([#236](https://github.com/home-operations/flate/issues/236)) ([d445088](https://github.com/home-operations/flate/commit/d4450880635ffc0dfa9a0a7851b59c360948868c))


### Code Refactoring

* **controllers:** ParentOf is a resolver func, queries both sources ([#239](https://github.com/home-operations/flate/issues/239)) ([572e622](https://github.com/home-operations/flate/commit/572e6227d562805080c9fef3efa7254fed158263))
* **orchestrator:** RenderTracker carries parent KS provenance ([#238](https://github.com/home-operations/flate/issues/238)) ([2803315](https://github.com/home-operations/flate/commit/2803315a41bb4ee6b2ecaf391f2bde09ed4a7325))

## [0.1.13](https://github.com/home-operations/flate/compare/0.1.12...0.1.13) (2026-05-24)


### Bug Fixes

* **helmrelease:** dispatch chart-rendered source CRs through AddObject ([#230](https://github.com/home-operations/flate/issues/230)) ([dcd385a](https://github.com/home-operations/flate/commit/dcd385a13ee538ffd26b9f34de3bd8e8add87b39))
* **kustomize:** pre-fetch URL resources to eliminate git binary dep ([#229](https://github.com/home-operations/flate/issues/229)) ([5aa0845](https://github.com/home-operations/flate/commit/5aa0845a0a77f32d98f428365031fd37e09271bc))
* **kustomize:** propagate URL fetch failures as real reconcile errors ([#235](https://github.com/home-operations/flate/issues/235)) ([c024d5d](https://github.com/home-operations/flate/commit/c024d5d6fd670735052474ddb78c6492d0ba6239))


### Performance Improvements

* **kustomize:** dedup remote-resource fetches across preflight passes ([#232](https://github.com/home-operations/flate/issues/232)) ([a4740bf](https://github.com/home-operations/flate/commit/a4740bf0302403a1e73d9e6702df5e7702533f3b))


### Miscellaneous Chores

* **oci:** demote cosign keyless-skip log from WARN to DEBUG ([#233](https://github.com/home-operations/flate/issues/233)) ([53ad9c1](https://github.com/home-operations/flate/commit/53ad9c1552ef147c515912535d331355fc44a141))

## [0.1.12](https://github.com/home-operations/flate/compare/0.1.11...0.1.12) (2026-05-24)


### Features

* **diff:** always emit per-resource header, even for single diffs ([#215](https://github.com/home-operations/flate/issues/215)) ([6c26379](https://github.com/home-operations/flate/commit/6c263798d9ecb6a42bf8b79405b3dd6b21d0fb13))
* **orchestrator:** surface RS-rendered non-Flux docs in parent KS output ([#218](https://github.com/home-operations/flate/issues/218)) ([e1ccae9](https://github.com/home-operations/flate/commit/e1ccae9a0d77250ab4f5b45a22a5e04530fbaf9f))
* **resourceset:** implement spec.inputStrategy: Permute ([#109](https://github.com/home-operations/flate/issues/109)) ([#217](https://github.com/home-operations/flate/issues/217)) ([a8054cc](https://github.com/home-operations/flate/commit/a8054ccede6b51da9827ad620b5110cec108859c))


### Bug Fixes

* bootstrap-GR test skip in changed-only mode + IsSuspended + log noise ([#214](https://github.com/home-operations/flate/issues/214)) ([3833c6e](https://github.com/home-operations/flate/commit/3833c6ec3dd0cead4e1bdb86c144596ad51c09dc))
* **change:** scope keepByName fallback to empty-namespace entries only ([#207](https://github.com/home-operations/flate/issues/207)) ([909c08c](https://github.com/home-operations/flate/commit/909c08c805b72b28ff69c211bfbf3a7a19d45939))
* **cli,testrunner:** diff-images coercion, bootstrap-GR test filter, get-helm gate ([#212](https://github.com/home-operations/flate/issues/212)) ([6e1d2ab](https://github.com/home-operations/flate/commit/6e1d2abc60b3d3e55e63728f2c0c338991f78f72))
* **cli:** surface Bootstrap errors instead of drowning them in test report ([#216](https://github.com/home-operations/flate/issues/216)) ([a3d6dc5](https://github.com/home-operations/flate/commit/a3d6dc52470276c35ba1c005f5931cd7b0b1caca))
* **cli:** validate -o uniformly across get all / get images / test ([#222](https://github.com/home-operations/flate/issues/222)) ([37ae742](https://github.com/home-operations/flate/commit/37ae7429f6f6d4e496996adcf534bc77b4661241))
* **kustomize:** skip broken symlinks during stage instead of aborting ([#228](https://github.com/home-operations/flate/issues/228)) ([42a6ee8](https://github.com/home-operations/flate/commit/42a6ee855b04db148132be51bf02d38e98fea1c1))
* **manifest:** extend strip walker to CronJob jobTemplate + StatefulSet PVCs ([#210](https://github.com/home-operations/flate/issues/210)) ([96f3ab3](https://github.com/home-operations/flate/commit/96f3ab32d23596d6d2b9f3c28257eb8ac8c8139e))


### Performance Improvements

* **helmrelease:** skip duplicate render via parent gate + fingerprint dedup ([#219](https://github.com/home-operations/flate/issues/219)) ([9daefba](https://github.com/home-operations/flate/commit/9daefba56f5072d0ccc07e5eb4d7735fd6ef8371))
* **kustomization:** skip duplicate render via fingerprint dedup ([#220](https://github.com/home-operations/flate/issues/220)) ([69744f3](https://github.com/home-operations/flate/commit/69744f3a17712155b34e888d404ae30fc4655172))


### Documentation

* **readme:** default output filters, build example, pipeline + lib API refresh ([#211](https://github.com/home-operations/flate/issues/211)) ([bb66014](https://github.com/home-operations/flate/commit/bb6601412763610ded9a7e0d44737d64871027b2))


### Miscellaneous Chores

* post-iter-N cleanup — dead format, dedupe, alias warn, README ([#209](https://github.com/home-operations/flate/issues/209)) ([7ad1aa8](https://github.com/home-operations/flate/commit/7ad1aa80efe377f0d0b8ef72a307f138e7fb0ae4))
* StatusReady message constants + Filter.Add ordering godoc ([#213](https://github.com/home-operations/flate/issues/213)) ([f4724f2](https://github.com/home-operations/flate/commit/f4724f2906009fd1c42debf53d77d8e6402d0d96))


### Code Refactoring

* **discovery:** unify ParentOf + HRParentOf into one index ([#227](https://github.com/home-operations/flate/issues/227)) ([3414530](https://github.com/home-operations/flate/commit/34145300b0f659d83a11a005765fde5eb507a334))
* **manifest:** unexport per-kind Parse helpers (internal-only API) ([#223](https://github.com/home-operations/flate/issues/223)) ([5d740c9](https://github.com/home-operations/flate/commit/5d740c9c7391853f7f7989ffc1512ef9547eb4a6))
* **source:** adopt RunWithStatus + YieldSlot to match KS/HR shape ([#221](https://github.com/home-operations/flate/issues/221)) ([1927fdc](https://github.com/home-operations/flate/commit/1927fdc8a20230914f89d0970c7a6e58cfd880f4))

## [0.1.11](https://github.com/home-operations/flate/compare/0.1.10...0.1.11) (2026-05-24)


### Bug Fixes

* **change:** extend keep set when KS controller emits rendered children ([#205](https://github.com/home-operations/flate/issues/205)) ([33fc7f3](https://github.com/home-operations/flate/commit/33fc7f358260d22a8f541e00b5ef93941d716dc5))

## [0.1.10](https://github.com/home-operations/flate/compare/0.1.9...0.1.10) (2026-05-24)


### Bug Fixes

* **discovery:** alias bootstrap GitRepositories in any namespace ([#202](https://github.com/home-operations/flate/issues/202)) ([de497c4](https://github.com/home-operations/flate/commit/de497c45d520ba43f5d29bb984ee35350f96ba3f))

## [0.1.9](https://github.com/home-operations/flate/compare/0.1.8...0.1.9) (2026-05-24)


### Features

* **diff:** emit per-resource header only when there's more than one ([#200](https://github.com/home-operations/flate/issues/200)) ([9f5e7aa](https://github.com/home-operations/flate/commit/9f5e7aa63c72ae2e6ee7c6d3085f9ae54a8c093e))

## [0.1.8](https://github.com/home-operations/flate/compare/0.1.7...0.1.8) (2026-05-24)


### Features

* **diff:** restore --strip-attr (same defaults) as a pre-dyff filter ([#197](https://github.com/home-operations/flate/issues/197)) ([88b13f6](https://github.com/home-operations/flate/commit/88b13f63e5845506a10d41a672e9b86d43fac01e))

## [0.1.7](https://github.com/home-operations/flate/compare/0.1.6...0.1.7) (2026-05-24)


### Features

* **diff:** render via dyff in github mode; drop --strip-attr / --limit-bytes ([#196](https://github.com/home-operations/flate/issues/196)) ([a3c28ed](https://github.com/home-operations/flate/commit/a3c28ed2584a9f3b45c34c1f9c7654188ce1655a))


### Miscellaneous Chores

* immutable releases are painfoil ([258cc10](https://github.com/home-operations/flate/commit/258cc10b0039fbc328c47a7560253307e05375b0))

## [0.1.6](https://github.com/home-operations/flate/compare/0.1.5...0.1.6) (2026-05-23)


### Features

* **orchestrator:** --allow-missing-secrets for ExternalSecret-shaped repos ([#191](https://github.com/home-operations/flate/issues/191)) ([665870e](https://github.com/home-operations/flate/commit/665870ec980dc6d28e4bee53e5da7dcedbcf21fb))


### Bug Fixes

* **helm:** use @ separator for OCIRepository digest refs ([#189](https://github.com/home-operations/flate/issues/189)) ([14d476b](https://github.com/home-operations/flate/commit/14d476b4956f31108c7e771e88e7a4a7f328b8b3))
* **loader:** skip configMapGenerator/secretGenerator data files ([#194](https://github.com/home-operations/flate/issues/194)) ([3bca98b](https://github.com/home-operations/flate/commit/3bca98b4adfd6119f9f6dfa7372d8c87acdd2ffa))

## [0.1.5](https://github.com/home-operations/flate/compare/0.1.4...0.1.5) (2026-05-23)


### Bug Fixes

* **bucket:** SECURITY — reject path-traversal escapes from S3 keys ([#171](https://github.com/home-operations/flate/issues/171)) ([42e177b](https://github.com/home-operations/flate/commit/42e177b031c6bc85d12c5aebf524ff7407348e8e))
* **bucket:** skip VCS/extension default excludes — match source-controller ([#174](https://github.com/home-operations/flate/issues/174)) ([76f7e67](https://github.com/home-operations/flate/commit/76f7e67a595d58c1587c65cefe45cdb9d71f2adc))
* **cli:** apply --skip-secrets/--skip-crds/--skip-kinds to KS-sourced output ([#182](https://github.com/home-operations/flate/issues/182)) ([dfc4f8f](https://github.com/home-operations/flate/commit/dfc4f8f8e66069a6ff397efe30d381a19bf67f3c))
* **cli:** gate helm-only flags off KS-only subcommands ([#186](https://github.com/home-operations/flate/issues/186)) ([25f92e5](https://github.com/home-operations/flate/commit/25f92e56afcdfef61b1975fe4d7602828fdb6d56))
* **depwait:** surface metadata.labels + metadata.annotations to readyExpr CEL ([#172](https://github.com/home-operations/flate/issues/172)) ([5dc3570](https://github.com/home-operations/flate/commit/5dc357077708b0f19b1a123e1c426492a213353d))
* **helm,values:** honor spec.install.replace; valuesFrom ignores ConfigMap.binaryData ([#180](https://github.com/home-operations/flate/issues/180)) ([bc35489](https://github.com/home-operations/flate/commit/bc35489eacc463d63c2331d33339609d0ef7ffc7))
* **helm:** exclude CRDs from commonMetadata stamping ([#175](https://github.com/home-operations/flate/issues/175)) ([632e917](https://github.com/home-operations/flate/commit/632e9177126882e03f912527175e65c2dc0dee5f))
* **helm:** exclude Helm hooks from commonMetadata + origin-label stamps ([#173](https://github.com/home-operations/flate/issues/173)) ([e08e20e](https://github.com/home-operations/flate/commit/e08e20e60c5a2a3538ce0e8dd7c747ceb1719d1f))
* **manifest:** reject Kustomization with empty sourceRef.name at parse time ([#179](https://github.com/home-operations/flate/issues/179)) ([70648e6](https://github.com/home-operations/flate/commit/70648e6464000755733b3513fdddd410ee57f941))
* **orchestrator:** apply --skip-kinds in Render() so embedders match CLI ([#183](https://github.com/home-operations/flate/issues/183)) ([87926cc](https://github.com/home-operations/flate/commit/87926ccff4b83c5f31aadd7948912262123df4c8))
* **orchestrator:** detect dependsOn cycles up-front, strip cycle edges ([#178](https://github.com/home-operations/flate/issues/178)) ([bd25c6b](https://github.com/home-operations/flate/commit/bd25c6b88c1f998c40b42018b2e1823baaf464f1))
* **resourceset:** reject inputStrategy=Permute instead of silent fall-through ([#176](https://github.com/home-operations/flate/issues/176)) ([ac548dc](https://github.com/home-operations/flate/commit/ac548dc026cc47fb9d39880ebbae687c7cafeeb9))
* **testrunner,orchestrator:** apply TrimSentinelPrefix at user-facing boundaries ([#185](https://github.com/home-operations/flate/issues/185)) ([d63ba9e](https://github.com/home-operations/flate/commit/d63ba9e655e0f8cb5f18c2e22f0269b78e781434))


### Documentation

* strip README to elegant, concise, digestible form ([#163](https://github.com/home-operations/flate/issues/163)) ([a5b3f3e](https://github.com/home-operations/flate/commit/a5b3f3eff679e8afa02ed0a29fa59beada923669))
* surface kustomize.Prepare alongside helm.Prepare ([#168](https://github.com/home-operations/flate/issues/168)) ([97f9116](https://github.com/home-operations/flate/commit/97f91169aa6c046a2a83b49c2b355adf7df943ee))


### Miscellaneous Chores

* add flate install gh action ([39ecdfe](https://github.com/home-operations/flate/commit/39ecdfeb38a313394f46c291475ca9d943dc6e20))


### Code Refactoring

* **cli:** consume Orchestrator.Render() instead of reaching into Store ([#166](https://github.com/home-operations/flate/issues/166)) ([03ee28d](https://github.com/home-operations/flate/commit/03ee28deeacfb87b12c349d0590cb5e77ee1a955))
* **helm:** route HelmChartSource through SourceResolver ([#184](https://github.com/home-operations/flate/issues/184)) ([2e212d9](https://github.com/home-operations/flate/commit/2e212d9daa6127a3e8d4a9fe6205a6ffe9345254))
* **kustomize:** add Prepare helper symmetric to helm.Prepare ([#167](https://github.com/home-operations/flate/issues/167)) ([00a5633](https://github.com/home-operations/flate/commit/00a563345b31ab9bfa96f22ed44154862e50a2be))
* **manifest:** move trimFluxPrefix → manifest.TrimSentinelPrefix ([#181](https://github.com/home-operations/flate/issues/181)) ([6135708](https://github.com/home-operations/flate/commit/61357088a2aeaeef92165ef45adc018cafcfa494))
* **manifest:** split helm.go into per-kind files ([#188](https://github.com/home-operations/flate/issues/188)) ([1126153](https://github.com/home-operations/flate/commit/1126153bcf214179868cdf11102229be76803d28))
* **orchestrator,kustomization:** extract finalize + emitRenderedChildren ([#187](https://github.com/home-operations/flate/issues/187)) ([3389fdf](https://github.com/home-operations/flate/commit/3389fdfb7e2445bc09509862b501565710510d14))
* **store:** move rendered-set bookkeeping out into orchestrator ([#177](https://github.com/home-operations/flate/issues/177)) ([4d92319](https://github.com/home-operations/flate/commit/4d92319ebfdce7c6d5e39dfd31a4e0341524b4ef))
* **store:** split methods across topic files (objects/status/artifacts/events/rendered) ([#170](https://github.com/home-operations/flate/issues/170)) ([372f366](https://github.com/home-operations/flate/commit/372f36613140474b954bd8e7a7451e5ecd074295))

## [0.1.4](https://github.com/home-operations/flate/compare/0.1.3...0.1.4) (2026-05-23)


### Features

* add homebrew support ([d5e88df](https://github.com/home-operations/flate/commit/d5e88df6ba63d4afea598e18c84906dc3f8fe378))
* add homebrew support ([3e1c190](https://github.com/home-operations/flate/commit/3e1c190d330b0aaccd6b76c60e3e9dadd3a0f752))
* **api:** Orchestrator.Render + typed Store.On* listeners ([#156](https://github.com/home-operations/flate/issues/156)) ([845b730](https://github.com/home-operations/flate/commit/845b73095ae32da2192c8bdfbb3417c62a1c54d3))
* **bucket:** support certSecretRef for mTLS endpoints ([#47](https://github.com/home-operations/flate/issues/47)) ([7c04ec2](https://github.com/home-operations/flate/commit/7c04ec25ce86b5cb373ab3ad0aebb56cc105a4a5))
* **cli:** broaden get hr/ks structured projection ([#93](https://github.com/home-operations/flate/issues/93)) ([5034bf8](https://github.com/home-operations/flate/commit/5034bf8a0ac2da4f311ad0fe44083e68e5869791))
* **controllers:** fail-loud on SOPS-encrypted rendered output ([#38](https://github.com/home-operations/flate/issues/38)) ([a25cbb5](https://github.com/home-operations/flate/commit/a25cbb5aaf4ee8e79969c3dc2ef795aa9c0a5be1))
* **depwait:** dependsOn ReadyExpr CEL evaluation ([#39](https://github.com/home-operations/flate/issues/39)) ([51052f7](https://github.com/home-operations/flate/commit/51052f7e7c14bb30eb5b8c35e45f0dcaa74dbba8))
* **depwait:** project observedGeneration/generation for CEL ReadyExpr ([#61](https://github.com/home-operations/flate/issues/61)) ([8970dce](https://github.com/home-operations/flate/commit/8970dceecfaa01018f9f66f5430ee444b62a5b4b))
* **get:** add `flate get images`; drop --enable-images / --only-images on `get all` ([#123](https://github.com/home-operations/flate/issues/123)) ([e3cc8ba](https://github.com/home-operations/flate/commit/e3cc8bae0faad6113164c611b2e3086323f45b9b))
* **git:** support ca.crt / caFile in GitRepository SecretRef ([#48](https://github.com/home-operations/flate/issues/48)) ([2906860](https://github.com/home-operations/flate/commit/290686090b058d0ad7ed524a08e3c31b2ca22fc4))
* **git:** support spec.verify (PGP commit/tag verification) ([#60](https://github.com/home-operations/flate/issues/60)) ([bb06f0b](https://github.com/home-operations/flate/commit/bb06f0bbf72fd0ee5cb28fb00be85b4167c06e30))
* **helm:** HelmRepository certSecretRef (mTLS) ([#45](https://github.com/home-operations/flate/issues/45)) ([fa33e01](https://github.com/home-operations/flate/commit/fa33e0187db8d6ce83dd6def91ab400445281f39))
* **helm:** HelmRepository SecretRef auth (HTTP basic + bearer) ([#37](https://github.com/home-operations/flate/issues/37)) ([924fb71](https://github.com/home-operations/flate/commit/924fb7140970aa10e887f993ab4edcd1a31bed1c))
* **helmrelease:** honor disableHooks, spec.test.enable, spec.commonMetadata ([#88](https://github.com/home-operations/flate/issues/88)) ([c57fa29](https://github.com/home-operations/flate/commit/c57fa293bd21712e19e2d376fde7468e2edd7394))
* **helmrelease:** honor spec.install.crds / spec.upgrade.crds policy ([#59](https://github.com/home-operations/flate/issues/59)) ([462e5df](https://github.com/home-operations/flate/commit/462e5df2b877af1a66d3974e9520fb8a6375b167))
* **helmrelease:** honor spec.postRenderers ([#80](https://github.com/home-operations/flate/issues/80)) ([efebeb8](https://github.com/home-operations/flate/commit/efebeb8618094a495b85ab9e5f08a642d75d26bd))
* **helmrelease:** honor spec.releaseName ([#76](https://github.com/home-operations/flate/issues/76)) ([72a8701](https://github.com/home-operations/flate/commit/72a8701cac8089912086a376672772f7f62f3f22))
* **kustomize:** honor Kustomization spec.commonMetadata ([#71](https://github.com/home-operations/flate/issues/71)) ([d90e5c2](https://github.com/home-operations/flate/commit/d90e5c22609e2da61fbc06dcb11d22000b204c28))
* **kustomize:** inject Flux owner labels on rendered children ([#81](https://github.com/home-operations/flate/issues/81)) ([a168f5c](https://github.com/home-operations/flate/commit/a168f5c333b35b7040e59340c79988929574ddd7))
* **manifest:** parse HelmRelease.serviceAccountName + HelmChart.reconcileStrategy ([#49](https://github.com/home-operations/flate/issues/49)) ([5c6e28a](https://github.com/home-operations/flate/commit/5c6e28a16d3722821555cd9cda71cb7c4c8398d1))
* **oci:** cosign keyed verification for OCIRepository ([#50](https://github.com/home-operations/flate/issues/50)) ([418caad](https://github.com/home-operations/flate/commit/418caad116d0d27457957a5f6e86c0b24cdce42c))
* **oci:** honor OCIRepository.spec.layerSelector ([#54](https://github.com/home-operations/flate/issues/54)) ([2e9cc8e](https://github.com/home-operations/flate/commit/2e9cc8eabcce32b63e8fce53ac3e610cb850d276))
* **resourceset:** expand flux-operator ResourceSet CRs at load time ([#106](https://github.com/home-operations/flate/issues/106)) ([5c41b12](https://github.com/home-operations/flate/commit/5c41b124a261e3e8f9d343f3bbbddcb71a70f024))
* **resourceset:** resolve spec.inputsFrom via ResourceSetInputProvider ([#122](https://github.com/home-operations/flate/issues/122)) ([3acbd34](https://github.com/home-operations/flate/commit/3acbd34102c4667d5037906c47afe47c2a4ecc85))
* **source/git:** RecurseSubmodules + spec.ref.name ([#41](https://github.com/home-operations/flate/issues/41)) ([0b664dd](https://github.com/home-operations/flate/commit/0b664dde73e13742910c15433175a1285383beef))
* **source/git:** spec.sparseCheckout ([#44](https://github.com/home-operations/flate/issues/44)) ([45f14bf](https://github.com/home-operations/flate/commit/45f14bf102ec3fd223135fad6478182b99248aa1))
* **source/oci:** certSecretRef (mTLS) + spec.insecure ([#46](https://github.com/home-operations/flate/issues/46)) ([2dbcee1](https://github.com/home-operations/flate/commit/2dbcee1c4ba7d7bd262d5797a26d240100ee173e))
* **source:** add Bucket support (generic / S3-compatible provider) ([#34](https://github.com/home-operations/flate/issues/34)) ([0840d4f](https://github.com/home-operations/flate/commit/0840d4ffb036ec472739f43039bed7faf5f301bd))
* **source:** add ExternalArtifact support ([#33](https://github.com/home-operations/flate/issues/33)) ([994d423](https://github.com/home-operations/flate/commit/994d4237a2cfe483be51f88c33406eabd77857ea))
* **source:** GitRepository SecretRef auth (HTTPS basic / bearer / SSH) ([#35](https://github.com/home-operations/flate/issues/35)) ([4089381](https://github.com/home-operations/flate/commit/4089381d5c5f75c0c6ef44b4b5a4be5fe0ca4205))
* **source:** honor spec.ignore on Git/OCI/Bucket sources ([#89](https://github.com/home-operations/flate/issues/89)) ([35dd6a8](https://github.com/home-operations/flate/commit/35dd6a8950a8e845d4f59468d848690da2d6fd3a))
* **source:** OCIRepository SecretRef auth (dockerconfigjson) ([#36](https://github.com/home-operations/flate/issues/36)) ([39d8094](https://github.com/home-operations/flate/commit/39d809409ac3d75f43d7c5b91b8555b425311738))
* **source:** support spec.proxySecretRef on Git/OCI/Bucket ([#55](https://github.com/home-operations/flate/issues/55)) ([ae28030](https://github.com/home-operations/flate/commit/ae28030ba7d420f99f7b9527858509e13698f2e2))
* **task:** bounded-concurrency Service via worker-pool semaphore ([#63](https://github.com/home-operations/flate/issues/63)) ([bbfbcad](https://github.com/home-operations/flate/commit/bbfbcadd6903c0fa88616820511b1272ee5ea820))
* thread ctx through RenderFlux and Loader.Load ([#62](https://github.com/home-operations/flate/issues/62)) ([f6f2c9c](https://github.com/home-operations/flate/commit/f6f2c9c1e1c4f141aaab756fa1900f2185d370b6))
* wipe SOPS-encrypted secrets to PLACEHOLDER instead of failing ([#53](https://github.com/home-operations/flate/issues/53)) ([2b085f2](https://github.com/home-operations/flate/commit/2b085f2f56236d862a356c94282e787ac3cdced2))


### Bug Fixes

* align with upstream Flux on CEL, helm strvals, slug, bucket, release-name ([#154](https://github.com/home-operations/flate/issues/154)) ([dfbcf15](https://github.com/home-operations/flate/commit/dfbcf15a37ae291cbdf8ae63b0868e5261c94947))
* **change:** keep ancestor Kustomizations in changed-only keep set ([#104](https://github.com/home-operations/flate/issues/104)) ([96752cd](https://github.com/home-operations/flate/commit/96752cd99b5d214409407a54f47bd7401063939f))
* **cli:** exit non-zero on per-resource reconcile failures ([#158](https://github.com/home-operations/flate/issues/158)) ([3aa8fa6](https://github.com/home-operations/flate/commit/3aa8fa68153857fbf9f3efc94c2e8a5781c44358))
* **cli:** gate --output per subcommand; honor -o json for build ([#159](https://github.com/home-operations/flate/issues/159)) ([d36f4c4](https://github.com/home-operations/flate/commit/d36f4c4460a2a7159cbea92d1a0dbb99a7ff0df1))
* **controllers:** typed errors with %w wrapping for dependency / source failures ([#57](https://github.com/home-operations/flate/issues/57)) ([4412cdc](https://github.com/home-operations/flate/commit/4412cdc292ff52ce540830d7d2779155f3e62ddc))
* correctness pass — git race, depwait drop, HR dependsOn pruning ([#124](https://github.com/home-operations/flate/issues/124)) ([cc71799](https://github.com/home-operations/flate/commit/cc7179986606a48ac021ca54b72d5fb48c5b6688))
* **cosign:** ed25519 verify on raw payload; -l scoped to get; deterministic test ([#144](https://github.com/home-operations/flate/issues/144)) ([4b22a75](https://github.com/home-operations/flate/commit/4b22a757d768782a2833aafc165793f4e7607ee0))
* **cosign:** skip keyless verification with warn instead of failing fetch ([#121](https://github.com/home-operations/flate/issues/121)) ([1bd3860](https://github.com/home-operations/flate/commit/1bd3860c41758ff5be9fa1a7adce0740af702f10))
* ctx-aware per-key locks for kustomize render + helm chart cache ([#74](https://github.com/home-operations/flate/issues/74)) ([035b263](https://github.com/home-operations/flate/commit/035b263f97d3aff8e224960d8c65dfe413ae268a))
* decode YAML the way Flux does; widen change-filter to structural parent ([#120](https://github.com/home-operations/flate/issues/120)) ([4d9ec67](https://github.com/home-operations/flate/commit/4d9ec67ef7645dff63cfb01351607c9a78a3bfe9))
* **depwait:** honor spec.timeout on Kustomization + HelmRelease ([#142](https://github.com/home-operations/flate/issues/142)) ([869bba1](https://github.com/home-operations/flate/commit/869bba150a18d550d9bc373fc252ccc09724f486))
* **depwait:** ReadyExpr non-additive default + status event for any condition ([#43](https://github.com/home-operations/flate/issues/43)) ([75f47c6](https://github.com/home-operations/flate/commit/75f47c6f8ebe768b22ff8c3b230f0c329d381de7))
* **depwait:** recover panics in per-dep watch goroutine ([#73](https://github.com/home-operations/flate/issues/73)) ([6c3bc6d](https://github.com/home-operations/flate/commit/6c3bc6d7a4337be73bc6dce6ab3373cb7e9ff51d))
* **flux-fidelity:** correctness bugs C1-C5 ([#26](https://github.com/home-operations/flate/issues/26)) ([41a90ac](https://github.com/home-operations/flate/commit/41a90ac9586196f901dd1d8a36a14bdde1429af7))
* **helm:** skip schema validation when values contain wipe placeholders ([#138](https://github.com/home-operations/flate/issues/138)) ([0395e1d](https://github.com/home-operations/flate/commit/0395e1dea3c93746cd07b17f35c5351c7464d2eb))
* **helm:** stamp helm.toolkit.fluxcd.io origin labels on rendered docs ([#113](https://github.com/home-operations/flate/issues/113)) ([3b71293](https://github.com/home-operations/flate/commit/3b712937f8cae4b63baad8df08ef282a07879eed))
* iter-7 — CLI UX, store replay panic, helm listener payload, perf ([#145](https://github.com/home-operations/flate/issues/145)) ([6bf9fbf](https://github.com/home-operations/flate/commit/6bf9fbfcf55f34f3293024838bf96883ac87b913))
* **kustomization:** emit render output before SOPS abort + dispatch child events ([#52](https://github.com/home-operations/flate/issues/52)) ([264b9d6](https://github.com/home-operations/flate/commit/264b9d6115ac160aa3ff91b60e4f786654cd6fa6))
* **kustomization:** fall back to bootstrap source when sourceRef is empty ([#107](https://github.com/home-operations/flate/issues/107)) ([093d222](https://github.com/home-operations/flate/commit/093d222862e8115c0c2b9ca56fb5965cf9f48408))
* **kustomization:** wait for structural parent before child reconcile ([#103](https://github.com/home-operations/flate/issues/103)) ([47f319c](https://github.com/home-operations/flate/commit/47f319c2bcb627bb8dd296a2cd95416d2a9cc329))
* **kustomize:** match upstream non-strict postBuild substitution default ([#119](https://github.com/home-operations/flate/issues/119)) ([e016444](https://github.com/home-operations/flate/commit/e016444a2e562a05a20ab34c92b870411a9e96ea))
* **loader:** inherit Flux KS effective namespace onto cross-tree spec.path resources ([#101](https://github.com/home-operations/flate/issues/101)) ([898c385](https://github.com/home-operations/flate/commit/898c38542d69c2e8211238bf86e7ce61e9674f5a))
* **loader:** skip kustomize Component subtrees during manifest discovery ([#136](https://github.com/home-operations/flate/issues/136)) ([00f923c](https://github.com/home-operations/flate/commit/00f923cc556d716fd662dae1ea432a2805f20a6f))
* **manifest:** wrap underlying err with %w in inputf call sites ([#127](https://github.com/home-operations/flate/issues/127)) ([8512c62](https://github.com/home-operations/flate/commit/8512c62d59e3a3ec02a64c0f3af52a0c131ed5f1))
* **orchestrator:** strip flux:input: prefix; include source file in failure messages ([#160](https://github.com/home-operations/flate/issues/160)) ([5b85975](https://github.com/home-operations/flate/commit/5b85975f6ac964a3315da4bc308f4df1a68a509b))
* pass [#5](https://github.com/home-operations/flate/issues/5) follow-ups — kustomize label precedence, cache.Reset lock, filterShowOnly tests ([#116](https://github.com/home-operations/flate/issues/116)) ([d7840e3](https://github.com/home-operations/flate/commit/d7840e3ee4f9bbe2d19107b7a82b61ecc7a871cf))
* postBuild precedence + bootstrap aliases + git slot lock + dead code ([#143](https://github.com/home-operations/flate/issues/143)) ([9498cb4](https://github.com/home-operations/flate/commit/9498cb4d14ac0c269dfbc6bc8a63291a63beb0b1))
* **resourceset:** align renderer with upstream flux-operator semantics ([#111](https://github.com/home-operations/flate/issues/111)) ([2519509](https://github.com/home-operations/flate/commit/25195097a742c159e2d518cb2828d62c616bad77))
* **source,manifest:** sourceignore defaults + ResolveChartRef HelmChart hole ([#90](https://github.com/home-operations/flate/issues/90)) ([0b5b6ea](https://github.com/home-operations/flate/commit/0b5b6eafb075a128b403b0a3c412192ba2feffbb))
* **source/cache:** per-slot locking; OCI defer-order on error paths ([#140](https://github.com/home-operations/flate/issues/140)) ([c63e389](https://github.com/home-operations/flate/commit/c63e3899784faf75142cc79284943abc3441ce73))
* **source/git:** preserve cache-hit signal across ApplyIgnore ([#137](https://github.com/home-operations/flate/issues/137)) ([a71830c](https://github.com/home-operations/flate/commit/a71830ceb684670e1ce3d1360b0ffd8cca80f0b7))
* **source:** base64-decode Secret.Data values in StringFromSecret ([#133](https://github.com/home-operations/flate/issues/133)) ([c5eab31](https://github.com/home-operations/flate/commit/c5eab310706623e62e6649a87c61f4fc8aea11ff))
* **source:** coalesce per-id fetches ([#75](https://github.com/home-operations/flate/issues/75)) ([4970e9a](https://github.com/home-operations/flate/commit/4970e9a32724721d6f8e1c8fed90d69c61d0a0bd))
* **source:** walk all files instead of SkipDir on excluded parents ([#135](https://github.com/home-operations/flate/issues/135)) ([b5c4840](https://github.com/home-operations/flate/commit/b5c48403a7208bbfe1a88d7e70dbe4cb55eb05ed))
* store-immutability violations + 4 resource-leak / cleanup bugs ([#129](https://github.com/home-operations/flate/issues/129)) ([1e6da3e](https://github.com/home-operations/flate/commit/1e6da3ee88f3df370b44fb89e2f3f49d1932214a))
* **store:** document immutability contract; clone-then-add at mutation sites ([#56](https://github.com/home-operations/flate/issues/56)) ([011bfa0](https://github.com/home-operations/flate/commit/011bfa0b0b6bd2dc242b3c15c2cf62f0580ed70a))
* **task:** reset Coalescer slot when fn panics ([#67](https://github.com/home-operations/flate/issues/67)) ([95e71d6](https://github.com/home-operations/flate/commit/95e71d65d7c6554dc0a053e1d8a283d625084dbd))
* **task:** YieldSlot to break parent/child KS pool deadlock ([#72](https://github.com/home-operations/flate/issues/72)) ([2d5d09a](https://github.com/home-operations/flate/commit/2d5d09afea6ac43c86e455bdc60353c6df769c4e))
* three correctness bugs from post-merge architecture review ([#86](https://github.com/home-operations/flate/issues/86)) ([8716471](https://github.com/home-operations/flate/commit/8716471ea25f20381e916208e11248e53899c9e5))


### Performance Improvements

* **build:** stream per-artifact, sort for deterministic output ([#146](https://github.com/home-operations/flate/issues/146)) ([0ec623b](https://github.com/home-operations/flate/commit/0ec623b10f669fba3ca9306bcf5a344c677201f2))
* **helm:** coalesce concurrent first-load of the same chart ([#153](https://github.com/home-operations/flate/issues/153)) ([b016d56](https://github.com/home-operations/flate/commit/b016d561ba9e2bec3b669f0b94b271e98d360fc8))
* list-objects by-kind index + skip envsubst round-trip on docs with no `${` ([#125](https://github.com/home-operations/flate/issues/125)) ([e35262c](https://github.com/home-operations/flate/commit/e35262c82cecd874bb6d54dabc7bf9d82642cf92))


### Documentation

* document Phase 3 capabilities ([#42](https://github.com/home-operations/flate/issues/42)) ([46adbee](https://github.com/home-operations/flate/commit/46adbee904fd8b399e8c471b3b6f4b6c547b3c69))
* document the library/embed API; refresh post-iter-12 surface ([#161](https://github.com/home-operations/flate/issues/161)) ([bad11d6](https://github.com/home-operations/flate/commit/bad11d6cd40c92a3a536612d0b21ccec221a08ef))
* prune stale comments and outdated references ([#118](https://github.com/home-operations/flate/issues/118)) ([682dd18](https://github.com/home-operations/flate/commit/682dd1860c52c3f56d09f78de34b8546bc030cba))
* refresh README + TESTDATA for SOPS placeholder, cosign, fixtures ([#66](https://github.com/home-operations/flate/issues/66)) ([0daacd6](https://github.com/home-operations/flate/commit/0daacd6831b6513be1bf871e5baeb02e7fcf6773))


### Miscellaneous Chores

* camelCase log keys + README drift fixes ([#128](https://github.com/home-operations/flate/issues/128)) ([19d42d4](https://github.com/home-operations/flate/commit/19d42d47adf7aeb98991f52f622f733f45706366))
* clean up 5 revive lint warnings ([#134](https://github.com/home-operations/flate/issues/134)) ([1f362d8](https://github.com/home-operations/flate/commit/1f362d8c672c502c3e12cf3286d81f413d7b23a5))
* consolidate test cert helpers + drop unused IDName ([#70](https://github.com/home-operations/flate/issues/70)) ([5b0f81e](https://github.com/home-operations/flate/commit/5b0f81ec3c4969dea32fb6f2c2df10333a6132ce))
* **format:** drop unused OutputWide constant ([#115](https://github.com/home-operations/flate/issues/115)) ([de4a3fe](https://github.com/home-operations/flate/commit/de4a3fe359a86bdc11c59295fc14a14da8ce8e2a))
* **lint:** drop unused ctx/obj parameters in base_test ([#32](https://github.com/home-operations/flate/issues/32)) ([2e76027](https://github.com/home-operations/flate/commit/2e76027ac102d3a816b5384712dbb45822a1a840))
* **manifest,store,helm:** prune dead code and redundant defensive clones ([#91](https://github.com/home-operations/flate/issues/91)) ([4add754](https://github.com/home-operations/flate/commit/4add754ce15a7a5025add65a459e3588df89d9e7))
* post-embed cleanup + controller unit tests ([#87](https://github.com/home-operations/flate/issues/87)) ([542c421](https://github.com/home-operations/flate/commit/542c4213c56f4ab076838bfeaba3aba3bf61b75a))
* purge dead code from review pass ([#126](https://github.com/home-operations/flate/issues/126)) ([671d5f6](https://github.com/home-operations/flate/commit/671d5f6d56c6b3baca25145a46cdbe686e374078))
* remove dead code ([#77](https://github.com/home-operations/flate/issues/77)) ([0c40ffa](https://github.com/home-operations/flate/commit/0c40ffaae04cdbd0ea1ab424caa9d1a7cf8f3650))
* review follow-ups — mergeChartValuesFiles coverage, dedupe proxy tests, name slug cap ([#94](https://github.com/home-operations/flate/issues/94)) ([bda7a07](https://github.com/home-operations/flate/commit/bda7a077bfa10648d1c6e01718da97ea83270001))


### Code Refactoring

* **api:** dedupe SecretGetter, simplify discovery.Config, document shadow-fields rule ([#152](https://github.com/home-operations/flate/issues/152)) ([ccfac8e](https://github.com/home-operations/flate/commit/ccfac8ed64bff800175e0574456974c0ddfe1ebc))
* code-quality pass — bugs, perf, DRY, structural cleanup ([#23](https://github.com/home-operations/flate/issues/23)) ([d868aec](https://github.com/home-operations/flate/commit/d868aec683be0e42507dfffadb27c247000a313a))
* **controllers:** consolidate lifecycle into base.Controller; fix(oci): cache-poison ([#150](https://github.com/home-operations/flate/issues/150)) ([8ba6a5c](https://github.com/home-operations/flate/commit/8ba6a5c7774d87b24e8a23be194a155e7b4886fe))
* **controllers:** extract base.Recover + base.RunWithStatus ([#30](https://github.com/home-operations/flate/issues/30)) ([3b17ae7](https://github.com/home-operations/flate/commit/3b17ae7cb06efa481af524f78d7b4397a85d9b24))
* ExistenceFetcher in source controller; canonical BootstrapSourceID ([#132](https://github.com/home-operations/flate/issues/132)) ([bbc95f1](https://github.com/home-operations/flate/commit/bbc95f167e544849d20af02a2ea8ca965e339361))
* extract pkg/discovery; slices.SortFunc + store.Mutate[T] ([#149](https://github.com/home-operations/flate/issues/149)) ([32cc081](https://github.com/home-operations/flate/commit/32cc0819886fb70322315daed74999da07792930))
* **helm:** delete legacy Add* push API; SourceResolver is the only path ([#162](https://github.com/home-operations/flate/issues/162)) ([d7c2baf](https://github.com/home-operations/flate/commit/d7c2baf35a4fbd47efeb365d78bdfe0ac253289a))
* **helm:** route source-CR lookups through SourceResolver ([#155](https://github.com/home-operations/flate/issues/155)) ([742b75e](https://github.com/home-operations/flate/commit/742b75e71dd02ccb944ab904b2f8dd41b229b2f9))
* **manifest:** alias local ref types to fluxcd upstream ([#68](https://github.com/home-operations/flate/issues/68)) ([03e114e](https://github.com/home-operations/flate/commit/03e114e7d83b0c895b127c5499624333f3d199c3))
* **manifest:** alias value-twin source types to upstream ([#83](https://github.com/home-operations/flate/issues/83)) ([7eac7c1](https://github.com/home-operations/flate/commit/7eac7c17dad0f1e5dbed43956a2db38fe0368cc8))
* **manifest:** embed upstream Specs into all 8 top-level CRs ([#85](https://github.com/home-operations/flate/issues/85)) ([061c2f2](https://github.com/home-operations/flate/commit/061c2f2dc8f55a73f2e56d2c7f2a4f2ed78bcceb))
* **manifest:** replace provider/verify-mode shadows with upstream ([#79](https://github.com/home-operations/flate/issues/79)) ([9ab91da](https://github.com/home-operations/flate/commit/9ab91da4816eaca2c5af6b73f316990663cf79b1))
* **manifest:** use typed Flux APIs for Parse* functions ([#24](https://github.com/home-operations/flate/issues/24)) ([c79f6d2](https://github.com/home-operations/flate/commit/c79f6d251a9246e6d64dc89cc23209ce6336f7ae))
* **source:** extract BuildTLSConfig helper; delete duplicate secret reader ([#64](https://github.com/home-operations/flate/issues/64)) ([1ce7e82](https://github.com/home-operations/flate/commit/1ce7e827fbd2951f4c0a84d706cbc06c7edb48c6))
* **source:** hoist cert-secret resolution into ResolveCertSecret ([#78](https://github.com/home-operations/flate/issues/78)) ([cab3318](https://github.com/home-operations/flate/commit/cab3318ae6ec8ae081da1c95992acadfc0873a15))
* **source:** per-kind subdirectories ([#40](https://github.com/home-operations/flate/issues/40)) ([3cddc9d](https://github.com/home-operations/flate/commit/3cddc9dc1496bb0edfdbeca85a9fc0a8838bfdb9))
* **source:** unified SourceArtifact + Fetcher interface ([#31](https://github.com/home-operations/flate/issues/31)) ([3dc0375](https://github.com/home-operations/flate/commit/3dc03750def17444c3bf11349cd7a5481aa7278b))
* **store:** switch internal status to []metav1.Condition ([#28](https://github.com/home-operations/flate/issues/28)) ([d3318a5](https://github.com/home-operations/flate/commit/d3318a585dfab8af7b3b7291611bcc911eebbd38))

## [0.1.3](https://github.com/home-operations/flate/compare/0.1.2...0.1.3) (2026-05-22)


### Bug Fixes

* create tag in release please workflow ([5726b91](https://github.com/home-operations/flate/commit/5726b9179f10a2a007a9cafa7215e614b031cf7c))

## [0.4.0](https://github.com/home-operations/flate/compare/0.3.0...0.4.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* have goreleaser manage releases ([c9b7fe4](https://github.com/home-operations/flate/commit/c9b7fe40799c5d05b87475f7c8333c6453a54caf))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* **main:** release 0.2.0 ([#16](https://github.com/home-operations/flate/issues/16)) ([3ed0289](https://github.com/home-operations/flate/commit/3ed0289ef82e411609d0766f698b2d6458bd62f9))
* **main:** release 0.3.0 ([#17](https://github.com/home-operations/flate/issues/17)) ([1064a80](https://github.com/home-operations/flate/commit/1064a80257b53e5c4ed3a5a6f25f75396f0ecd95))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.3.0](https://github.com/home-operations/flate/compare/0.2.0...0.3.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* **main:** release 0.2.0 ([#16](https://github.com/home-operations/flate/issues/16)) ([3ed0289](https://github.com/home-operations/flate/commit/3ed0289ef82e411609d0766f698b2d6458bd62f9))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.2.0](https://github.com/home-operations/flate/compare/0.1.3...0.2.0) (2026-05-22)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))
* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))
* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))
* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))
* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))
* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))


### Miscellaneous Chores

* **main:** release 0.0.2 ([c002790](https://github.com/home-operations/flate/commit/c002790de32c297cc3e054b89df5d9cc76d2d21e))
* **main:** release 0.0.2 ([d8e85c2](https://github.com/home-operations/flate/commit/d8e85c2414a21e6b26bd081f72782f3771ddadfe))
* **main:** release 0.0.3 ([70a603a](https://github.com/home-operations/flate/commit/70a603af1a787f5b7505f11bf358d921774fd55d))
* **main:** release 0.0.3 ([3c5ed91](https://github.com/home-operations/flate/commit/3c5ed91699fc229688cee6d05c9bde36531ef687))
* **main:** release 0.0.4 ([68fd2ad](https://github.com/home-operations/flate/commit/68fd2adb5765a9bb0273684b5ff76a726cdb308d))
* **main:** release 0.0.4 ([6ebe77b](https://github.com/home-operations/flate/commit/6ebe77b4c94666442f7361daffb4989060ee4233))
* **main:** release 0.1.0 ([#9](https://github.com/home-operations/flate/issues/9)) ([e21888f](https://github.com/home-operations/flate/commit/e21888fb9f6aaa90f054a1f6b09f9015807b7214))
* **main:** release 0.1.1 ([#13](https://github.com/home-operations/flate/issues/13)) ([c028e1d](https://github.com/home-operations/flate/commit/c028e1d1fe6f0e070854dc61a68c7bfb1f0d1fd1))
* **main:** release 0.1.2 ([#14](https://github.com/home-operations/flate/issues/14)) ([e0f7685](https://github.com/home-operations/flate/commit/e0f7685f082b49d56cdfc4146c2b4595b068be03))
* **main:** release 0.1.3 ([#15](https://github.com/home-operations/flate/issues/15)) ([af0fe5f](https://github.com/home-operations/flate/commit/af0fe5f33a5d8c92ff5505bf1e677cd0ce0bf217))
* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

## [0.1.3](https://github.com/home-operations/flate/compare/0.1.2...0.1.3) (2026-05-22)


### Bug Fixes

* **ci:** create releases as drafts so goreleaser can upload before immutability ([f28c03b](https://github.com/home-operations/flate/commit/f28c03be95f4b60d941d3fbdd1631c85717fb717))

## [0.1.2](https://github.com/home-operations/flate/compare/0.1.1...0.1.2) (2026-05-22)


### Bug Fixes

* use cosign bundle format and drop redundant checksum name_template ([bb7e39d](https://github.com/home-operations/flate/commit/bb7e39ded817d999fd594f3eea9684cb5b9ee213))

## [0.1.1](https://github.com/home-operations/flate/compare/0.1.0...0.1.1) (2026-05-22)


### Bug Fixes

* add gorelease and jacked security ([cd7cff3](https://github.com/home-operations/flate/commit/cd7cff3379512922a9c51430ed7db4e4d25bfa82))

## [0.1.0](https://github.com/home-operations/flate/compare/0.0.4...0.1.0) (2026-05-21)


### ⚠ BREAKING CHANGES

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11))

### Features

* **cli:** unify ks/hr/all layout across get and test ([#11](https://github.com/home-operations/flate/issues/11)) ([e490dc8](https://github.com/home-operations/flate/commit/e490dc87df12fd53bd66426260f12e38fe040ed4))


### Documentation

* correct helm version reference from v3 to v4 ([#10](https://github.com/home-operations/flate/issues/10)) ([eb174dd](https://github.com/home-operations/flate/commit/eb174dd230ec06cf68bd6ff10440e42b46170687))
* document --only-crds and get-all image flags ([#12](https://github.com/home-operations/flate/issues/12)) ([c9e5cc9](https://github.com/home-operations/flate/commit/c9e5cc95e7d619e173c8e43a9be703602e8b0471))
* polish README and trim helper comments ([#8](https://github.com/home-operations/flate/issues/8)) ([3956827](https://github.com/home-operations/flate/commit/39568273c88ea1b06ee6e746a36fc3db5cae64f4))

## [0.0.4](https://github.com/home-operations/flate/compare/0.0.3...0.0.4) (2026-05-21)


### Features

* **diff:** add `diff images` for set-diffing changed container images ([adf94e0](https://github.com/home-operations/flate/commit/adf94e036c65b6b62e771d569b090011aa334b21))
* **diff:** add `diff images` for set-diffing changed container images ([1854bf2](https://github.com/home-operations/flate/commit/1854bf2f3072ceb4874bf207abac50fcf4141988))

## [0.0.3](https://github.com/home-operations/flate/compare/0.0.2...0.0.3) (2026-05-21)


### Bug Fixes

* **diff:** correct deletion output and stabilize reconcile race ([f06751f](https://github.com/home-operations/flate/commit/f06751faf4d20ceda04f3eae9dbe1c22b36e3421))
* **diff:** correct deletion output and stabilize reconcile race ([7b0aa9d](https://github.com/home-operations/flate/commit/7b0aa9d2b728546efe1cbdf81c0d053ccc7f0ac8))

## [0.0.2](https://github.com/home-operations/flate/compare/0.0.1...0.0.2) (2026-05-21)


### Features

* add fluxrr ([6ee19d7](https://github.com/home-operations/flate/commit/6ee19d76a213f831f02c67da2f5b746626f8efda))


### Bug Fixes

* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([2d75b26](https://github.com/home-operations/flate/commit/2d75b260e2476cbeb9071c0f19da196c61e26dc6))
* **deps:** update module k8s.io/apimachinery (v0.36.0 → v0.36.1) ([b14bcb4](https://github.com/home-operations/flate/commit/b14bcb4bdb587c3ea7d8ffb3a4f49a9fb7f56430))


### Miscellaneous Chores

* update a lot... ([f84f766](https://github.com/home-operations/flate/commit/f84f766035052d71513cf993a0c1f9d18d18bb42))

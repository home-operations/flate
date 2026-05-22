# Changelog

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

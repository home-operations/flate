package sourceref_test

import (
	stdcmp "cmp"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	gogit "github.com/go-git/go-git/v5"
	"github.com/go-git/go-git/v5/config"
	"github.com/go-git/go-git/v5/plumbing"
	"github.com/go-git/go-git/v5/plumbing/object"
	"github.com/google/go-cmp/cmp"

	"github.com/home-operations/flate/internal/assert"
	"github.com/home-operations/flate/internal/testutil"
	"github.com/home-operations/flate/pkg/discovery"
	"github.com/home-operations/flate/pkg/loader"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/store"
	"github.com/home-operations/flate/test/e2e/internal/harness"
)

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerName(t *testing.T, tt *struct {
	name          string
	appsFile      string
	sourcesFile   string
	sourcesName   string
	sourceRefName string
	sourceURL     string
	noRef         bool
},
) {
	t.Helper()

	root := t.TempDir()
	repo := testutil.GitInit(t, root)
	{
		_, err := repo.CreateRemote(&config.RemoteConfig{
			Name: "origin", URLs: []string{"git://fixture.invalid/cluster"},
		})
		assert.FatalIf(t, err != nil, "%v", err)
	}
	testutil.WriteFile(
		t,
		root,
		"flux/"+tt.appsFile,
		testutil.KustomizationYAML(
			"apps",
			"flux-system",
			"  interval: 10m\n  path: ./apps\n  sourceRef: {kind: GitRepository, name: pinned, namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(
		t,
		root,
		"flux/"+tt.sourcesFile,
		testutil.KustomizationYAML(
			tt.sourcesName,
			"flux-system",
			"  interval: 10m\n  path: ./sources\n  sourceRef: {kind: GitRepository, name: "+tt.sourceRefName+", namespace: flux-system}\n",
		),
	)
	testutil.WriteFile(t, root, "sources/kustomization.yaml", "resources: [repo.yaml]\n")
	url, ref := stdcmp.Or(tt.sourceURL, "git://fixture.invalid/cluster"), "  ref: {tag: v1.0.0}\n"
	if tt.noRef {
		ref = ""
	}
	testutil.WriteFile(t, root, "sources/repo.yaml", `apiVersion: source.toolkit.fluxcd.io/v1
kind: GitRepository
metadata: {name: pinned, namespace: flux-system}
spec:
  interval: 10m
  url: `+url+"\n"+ref)
	testutil.WriteFile(t, root, "apps/kustomization.yaml", "resources: [cm.yaml, deleted.yaml, extra.yaml]\n")
	testutil.WriteFile(
		t,
		root,
		"apps/cm.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: hello, namespace: apps}\ndata: {value: pinned}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/deleted.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: deleted, namespace: apps}\ndata: {value: pinned-deleted}\n",
	)
	testutil.WriteFile(
		t,
		root,
		"apps/extra.yaml",
		"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra, namespace: apps}\ndata: {value: pinned-extra}\n",
	)
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	head, err := repo.Head()
	assert.NoError(t, err)
	assert.NoError(t, repo.Storer.SetReference(
		plumbing.NewHashReference(plumbing.NewTagReferenceName("v1.0.0"), head.Hash()),
	))
	assert.NoError(t, os.Remove(filepath.Join(root, "apps", "deleted.yaml")))
	harness.MutateFile(t, filepath.Join(root, "apps", "cm.yaml"), "value: pinned", "value: newer")
	testutil.GitCommitAll(
		t,
		repo,
		"init",
		&gogit.CommitOptions{Author: &object.Signature{Name: "t", Email: "t@example", When: time.Unix(0, 0)}},
	)
	harness.MutateFile(t, filepath.Join(root, "apps", "cm.yaml"), "value: newer", "value: dirty")
	testutil.WriteFile(t, root, "apps/extra.yaml", "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: extra\n")
	transport := installSourceRefTransport(t, repo.Storer)
	var wantError string
	if tt.noRef || tt.sourceURL != "" {
		l := loader.New(store.New())
		l.SourceRoot = root
		{
			_, loadErr := l.Load(t.Context(), filepath.Join(root, "apps"))
			assert.FatalIf(t, loadErr != nil, "%v", loadErr)
		}
		l.Admit(root)
		decodeError := l.EntryFailure(t.Context(), root)
		assert.FatalIf(
			t,
			!errors.Is(decodeError, manifest.ErrInput),
			"expected working-tree decode error, got %v",
			decodeError,
		)
		wantError = "flate error: " + decodeError.Error() + "\n"
	}
	for _, command := range []string{"build", "diff"} {
		t.Run(command, func(t *testing.T) {
			e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommand(t, &command, &root, struct {
				tt **struct {
					name          string
					appsFile      string
					sourcesFile   string
					sourcesName   string
					sourceRefName string
					sourceURL     string
					noRef         bool
				}
				transport **sourceRefLoader
				wantError *string
			}{tt: &tt, transport: &transport, wantError: &wantError})
		})
	}
	if wantError != "" {
		return
	}
	st := store.New()
	res, err := discovery.Run(t.Context(), discovery.Config{
		Path: filepath.Join(root, "flux"), Store: st,
	})
	assert.NoError(t, err)
	artifact, ok := st.GetArtifact(manifest.NamedResource{
		Group: "source.toolkit.fluxcd.io",
		Kind:  manifest.KindGitRepository, Namespace: "flux-system", Name: "pinned",
	}, store.ArtifactContent).(*store.SourceArtifact)
	assert.FatalIf(
		t,
		!ok || artifact.LocalPath == root,
		"expected committed source artifact, got %+v",
		artifact,
	)
	for _, file := range []struct{ name, path string }{{"hello", "cm.yaml"}, {"deleted", "deleted.yaml"}, {"extra", "extra.yaml"}} {
		id := manifest.NamedResource{Kind: manifest.KindConfigMap, Namespace: "apps", Name: file.name}
		record, indexed := res.Index.Lookup(id)
		assert.FatalIf(t, !indexed || record.Path != filepath.Join(artifact.LocalPath, "apps", file.path) ||
			filepath.Join(
				root,
				filepath.FromSlash(record.File),
			) != record.Path, "pinned discovery metadata for %s: indexed=%t path=%q source=%q", id, indexed, record.Path, record.File)
	}
}

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommand(
	t *testing.T,
	command *string,
	root *string,
	capturedInputs struct {
		tt **struct {
			name          string
			appsFile      string
			sourcesFile   string
			sourcesName   string
			sourceRefName string
			sourceURL     string
			noRef         bool
		}
		transport **sourceRefLoader
		wantError *string
	},
) {
	t.Helper()

	var previous string
	for _, concurrency := range []string{"2", "8"} {
		t.Run("concurrency_"+concurrency, func(t *testing.T) {
			e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommandCase(t, &command, &root, struct {
				concurrency    *string
				capturedInputs *struct {
					tt **struct {
						name          string
						appsFile      string
						sourcesFile   string
						sourcesName   string
						sourceRefName string
						sourceURL     string
						noRef         bool
					}
					transport **sourceRefLoader
					wantError *string
				}
				previous *string
			}{concurrency: &concurrency, capturedInputs: &capturedInputs, previous: &previous})
		})
	}
}

func e2ESourceRefPinnedSourceDiscoveredAfterConsumerNameCommandCase(
	t *testing.T,
	command **string,
	root **string,
	capturedInputs struct {
		concurrency    *string
		capturedInputs *struct {
			tt **struct {
				name          string
				appsFile      string
				sourcesFile   string
				sourcesName   string
				sourceRefName string
				sourceURL     string
				noRef         bool
			}
			transport **sourceRefLoader
			wantError *string
		}
		previous *string
	},
) {
	t.Helper()

	args := []string{
		(*(*command)), "all", "--path", filepath.Join((*(*root)), "flux"),
		"--concurrency", (*capturedInputs.concurrency), "--cache-dir", t.TempDir(),
	}
	if (*(*command)) == "diff" {
		args = append(args, "--base", "HEAD", "-o", "diff")
	}
	runCLIBuffersResult1 := harness.RunCLIBuffers(args...)
	out, stderr, code := runCLIBuffersResult1.Stdout, runCLIBuffersResult1.Stderr, runCLIBuffersResult1.Code
	if (*capturedInputs.capturedInputs.tt).sourceURL != "" {
		wantCode, wantReason := 0, "external source GitRepository.source.toolkit.fluxcd.io/flux-system/pinned is outside the local tree; not rendered"
		if (*(*command)) == "build" {
			wantCode, wantReason = 1, "repository not found"
		}
		if code != wantCode || !strings.Contains(stderr, wantReason) ||
			strings.Contains(stderr, "loader: decode") ||
			(*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
			((*(*command)) == "diff" && out != "") {
			t.Fatalf(
				"external source must release the working-tree error: exit=%d transport=%d\n%s\nstderr:\n%s",
				code,
				(*capturedInputs.capturedInputs.transport).calls.Load(),
				out,
				stderr,
			)
		}
		return
	}
	if (*capturedInputs.capturedInputs.wantError) != "" {
		wantReason := "kustomize build ./apps"
		if code != 1 || !strings.Contains(stderr, wantReason) ||
			(*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
			((*(*command)) == "diff" && out != "") {
			t.Fatalf(
				"unpinned Kustomization failure: exit=%d transport=%d\n%s\nstderr:\n%s",
				code,
				(*capturedInputs.capturedInputs.transport).calls.Load(),
				out,
				stderr,
			)
		}
		return
	}
	if code != 0 || (*capturedInputs.capturedInputs.transport).calls.Load() != 0 ||
		((*(*command)) == "build" && (!strings.Contains(out, "value: pinned") ||
			!strings.Contains(out, "value: pinned-deleted") || !strings.Contains(out, "value: pinned-extra") ||
			strings.Contains(out, "value: dirty") || strings.Contains(out, "value: newer"))) ||
		((*(*command)) == "diff" && out != "") {
		t.Fatalf(
			"late source pin: exit=%d transport=%d\n%s\nstderr:\n%s",
			code,
			(*capturedInputs.capturedInputs.transport).calls.Load(),
			out,
			stderr,
		)
	}
	assert.FatalIf(
		t,
		(*capturedInputs.concurrency) == "8" && out != (*capturedInputs.previous),
		"output differs across concurrency levels (-want +got):\n%s",
		cmp.Diff((*capturedInputs.previous), out),
	)
	(*capturedInputs.previous) = out
}

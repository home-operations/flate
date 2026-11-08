package cli

import (
	"bytes"
	"cmp"
	"compress/gzip"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"maps"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"runtime/metrics"
	"strconv"
	"strings"
	"testing"
	"time"

	sourcecache "github.com/home-operations/flate/internal/source/cache"
	"github.com/home-operations/flate/pkg/manifest"

	"github.com/opencontainers/go-digest"
	ocispec "github.com/opencontainers/image-spec/specs-go/v1"

	"github.com/home-operations/flate/internal/testutil"
)

type workBenchValidation struct {
	output [sha256.Size]byte
	work   manifest.WorkCounts
}

type workBenchPreparation struct {
	args  []string
	reset func()
}

func workBenchRemoteCases(b *testing.B) []workBenchCase {
	b.Helper()
	serverURL := workBenchRemoteServer(b)
	ociFiles := map[string]string{}
	for i := range 16 {
		ociFiles[fmt.Sprintf(
			"flux/release-%02d.yaml",
			i,
		)] = fmt.Sprintf(
			"apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name"+
				": release-%02d, namespace: apps}\nspec:\n  interval: 1h\n  chart:\n    ki"+
				"nd: OCIRepository\n    spec: {url: 'oci://%s/demo-%02d', insecure: true, "+
				"ref: {tag: '1.0.0'}}\n",
			i,
			strings.TrimPrefix(
				serverURL,
				"http://",
			),
			i,
		)
	}
	httpFiles := map[string]string{
		"flux/repo.yaml": "apiVersion: source.toolkit.fluxcd.io/v1\nkind: HelmRepository\nmetadata: " +
			"{name: repo, namespace: apps}\nspec: {url: '" + serverURL + "', interval: 1h}\n",

		"flux/release.yaml": "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name" +
			": release, namespace: apps}\nspec:\n  chart:\n    spec: {chart: demo, ver" +
			"sion: '1.0.0', sourceRef: {kind: HelmRepository, name: repo}}\n",
	}
	shared := maps.Clone(httpFiles)
	for i := range 8 {
		shared[fmt.Sprintf(
			"flux/release-%02d.yaml",
			i,
		)] = strings.ReplaceAll(
			httpFiles["flux/release.yaml"],
			"name: release,",
			"name: release-"+strconv.Itoa(
				i,
			)+",",
		)
	}
	return []workBenchCase{
		{name: "acquisition/oci-simultaneous", files: ociFiles, argv: []string{"build", "all"}},
		{name: "acquisition/large-index", files: httpFiles, argv: []string{"build", "all"}},
		{name: "acquisition/cold-shared-source", files: shared, argv: []string{"build", "all"}},
	}
}

func workBenchDiffCases(base map[string]string) []workBenchCase {
	edits := workBenchEdits()
	cases := make([]workBenchCase, 0, 2*len(edits))
	for _, edit := range edits {
		before := maps.Clone(base)
		maps.Copy(before, edit.prepare)
		after := maps.Clone(before)
		after[edit.file] = strings.ReplaceAll(after[edit.file], edit.old, edit.next)
		edited := 1
		if edit.name == "multi-file" {
			after["flux/release.yaml"] = strings.ReplaceAll(after["flux/release.yaml"], "value: old", "value: new")
			edited++
		}
		for _, warm := range []bool{false, true} {
			cases = append(
				cases,
				workBenchCase{
					name:   "diff/" + edit.name,
					before: before,
					files:  after,
					argv: []string{
						"diff",
						"all",
						"-o",
						"diff",
					},
					warm:   warm,
					edited: edited,
					owners: edit.owners,
				},
			)
		}
	}
	return cases
}

func workBenchProof(b *testing.B, diagnostic string) manifest.WorkCounts {
	b.Helper()
	var fileVisits, readerVisits, rawKeys uint64
	fields := regexp.MustCompile(`(FileEntries|ReaderEdges|RawKeyPrepares):(\d+)`)
	var summaries int
	for line := range strings.Lines(diagnostic) {
		if !strings.Contains(line, `msg="orchestrator: reconcile complete"`) {
			continue
		}
		summaries++
		for _, match := range fields.FindAllStringSubmatch(line, -1) {
			count, err := strconv.ParseUint(match[2], 10, 64)
			if err != nil {
				b.Fatal(err)
			}
			switch match[1] {
			case "FileEntries":
				fileVisits += count
			case "ReaderEdges":
				readerVisits += count
			case "RawKeyPrepares":
				rawKeys += count
			}
		}
	}
	if summaries != 2 || fileVisits == 0 || rawKeys == 0 {
		b.Fatalf("missing paired work proof: summaries=%d files=%d raw keys=%d", summaries, fileVisits, rawKeys)
	}
	return manifest.WorkCounts{FileEntries: fileVisits, ReaderEdges: readerVisits, RawKeyPrepares: rawKeys}
}

func newHeapSampler() heapSampler {
	s := heapSampler{command: make(chan bool), result: make(chan uint64), joined: make(chan struct{})}
	go func() {
		defer close(s.joined)
		ticker := time.NewTicker(time.Millisecond)
		ticker.Stop()
		defer ticker.Stop()
		var baseline, peak uint64
		samples := []metrics.Sample{{Name: "/memory/classes/heap/objects:bytes"}}
		sample := func() uint64 {
			metrics.Read(samples)
			heap := samples[0].Value.Uint64()
			peak = max(peak, heap)
			return heap
		}
		for {
			select {
			case start, ok := <-s.command:
				if !ok {
					return
				}
				if start {
					peak = 0
					baseline = sample()
					ticker.Reset(time.Millisecond)
					s.result <- 0
				} else {
					ticker.Stop()
					sample()
					s.result <- peak - baseline
				}
			case <-ticker.C:
				sample()
			}
		}
	}()
	return s
}

type heapSampler struct {
	command chan bool
	result  chan uint64
	joined  chan struct{}
}

func workBenchEdits() []struct {
	name, file, old, next string
	prepare               map[string]string
	owners                int
} {
	return []struct {
		name, file, old, next string
		prepare               map[string]string
		owners                int
	}{
		{"release-values", "flux/release.yaml", "value: old", "value: new", nil, 1},

		{"image", "app/cm-0000.yaml", "busybox:1", "busybox:2", nil, 1},

		{
			"kustomization-patch",
			"flux/owner.yaml",
			"  interval: 1h",
			"  interval: 1h\n  patches:\n    - target: {kind: ConfigMap, name: output-" +
				"0000}\n      patch: '[{\"op\":\"add\",\"path\":\"/data/patched\",\"value\"" +
				":\"new\"}]'",
			nil,
			1,
		},

		{
			"annotation",
			"app/kustomization.yaml",
			"components:",
			"commonAnnotations: {edited: yes}\ncomponents:",
			nil,
			1,
		},

		{"namespace", "app/kustomization.yaml", "components:", "namespace: moved\ncomponents:", nil, 1},

		{"component", "shared/kustomization.yaml", "shared: old", "shared: new", nil, 1},

		{"substitution", "flux/vars.yaml", "VALUE: old", "VALUE: new", nil, 1},

		{"interval", "flux/owner.yaml", "interval: 1h", "interval: 2h", nil, 0},

		{
			"nested-patch",
			"patches/nested/patch.yaml",
			"patched: old",
			"patched: new",
			map[string]string{
				"app/kustomization.yaml": "resources: [../base]\ncomponents: [../shared]\n",

				"base/kustomization.yaml": "resources: [../app/cm-0000.yaml]\npatches: [{path: ../patches/nested/patch.yaml}]\n",

				"patches/nested/patch.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output-0000, namespace:" +
					" apps}\ndata: {patched: old}\n",
			},
			1,
		},

		{"resourceset-input", "flux/set.yaml", "value: old", "value: new", nil, 1},

		{"multi-file", "app/cm-0000.yaml", "busybox:1", "busybox:2", nil, 2},
	}
}

func workBenchCases() []workBenchCase {
	base := workBenchFiles(16)
	cases := workBenchScopeCases(base)
	cases = append(cases, workBenchDiffCases(base)...)
	historyBefore := maps.Clone(base)
	historyBefore["charts/demo/requirements.yaml"] = "dependencies: []\n"
	historyAfter := maps.Clone(historyBefore)
	historyAfter["flux/release.yaml"] = strings.ReplaceAll(
		historyAfter["flux/release.yaml"],
		"value: old",
		"value: new",
	)
	cases = append(
		cases,
		workBenchCase{
			name:   "diff/decoder-history",
			before: historyBefore,
			files:  historyAfter,
			argv: []string{
				"diff",
				"all",
				"-o",
				"diff",
			},
			warm:   true,
			edited: 1,
			owners: 1,
		},
	)
	cases = append(cases, workBenchComplexityCases()...)
	cases = append(
		cases,

		workBenchCase{
			name: "full/large",
			files: workBenchFiles(
				1000,
			),
			argv: []string{
				"build",
				"all",
			},
			operationLimit: 2500 * time.Millisecond,
			setupLimit:     2 * time.Second,
		},

		workBenchCase{name: "full/warm", files: base, argv: []string{"build", "all"}, warm: true},
	)
	for _, workers := range []int{1, 4, 16} {
		cases = append(
			cases,
			workBenchCase{
				name: fmt.Sprintf(
					"full/concurrency_%d",
					workers,
				),
				files: base,
				argv: []string{
					"build",
					"all",
					"--concurrency",
					strconv.Itoa(
						workers,
					),
				},
			},
		)
	}
	large := maps.Clone(base)
	large["charts/demo/templates/cm.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: release-output, namespa" +
		"ce: apps}\ndata: {payload: '{{ repeat 262144 \"x\" }}'}\n"
	cases = append(
		cases,
		workBenchCase{
			name:  "output/large-json",
			files: large,
			argv:  []string{"build", "all", "-o", "json"},
			warm:  true,
		},
	)
	multi := map[string]string{}
	for _, cluster := range []string{"alpha", "omega"} {
		multi[cluster+"/flux.yaml"] = testutil.KustomizationYAML(cluster, "apps", "  path: ./"+cluster+"/app\n")
		multi[cluster+"/app/kustomization.yaml"] = "resources: [cm.yaml]\n"
		multi[cluster+"/app/cm.yaml"] = "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: " + cluster + ", namespace: apps}\n"
	}
	multi["flux/kustomization.yaml"] = "resources: [../alpha/flux.yaml, ../omega/flux.yaml]\n"
	cases = append(cases, workBenchCase{name: "clusters/two", files: multi, argv: []string{"build", "all"}})
	return cases
}

func benchmarkWorkRun(b *testing.B, tc *workBenchCase, peakArm bool) {
	b.Helper()
	setup := time.Now()
	preparation := prepareWorkBench(b, tc)
	args, reset := preparation.args, preparation.reset
	var stdout, stderr bytes.Buffer
	reset()
	proof := tc.validateRun(b, args, &stdout, &stderr)
	want, fileVisits, readerVisits, rawKeys := proof.output, proof.work.FileEntries, proof.work.ReaderEdges, proof.work.RawKeyPrepares

	var sampler heapSampler
	if peakArm {
		sampler = newHeapSampler()
		defer func() { close(sampler.command); <-sampler.joined }()
	}
	setupElapsed := time.Since(setup)
	operationLimit := cmp.Or(tc.operationLimit, 250*time.Millisecond)
	setupLimit := cmp.Or(tc.setupLimit, time.Second)
	b.ReportAllocs()
	var resetTime time.Duration
	var peak uint64
	var overruns int
	for b.Loop() {
		b.StopTimer()
		started := time.Now()
		reset()
		resetTime += time.Since(started)
		stdout.Reset()
		stderr.Reset()
		if peakArm {
			sampler.command <- true
			<-sampler.result
		}
		b.StartTimer()
		started = time.Now()
		code := Run(args, &stdout, &stderr)
		elapsed := time.Since(started)
		b.StopTimer()
		if peakArm {
			sampler.command <- false
			peak = max(peak, <-sampler.result)
		}
		if elapsed > operationLimit {
			overruns++
		}
		if code != 0 || sha256.Sum256(stdout.Bytes()) != want {
			b.Fatalf("operation exit=%d output digest differs: %s", code, &stderr)
		}
		b.StartTimer()
	}
	b.ReportMetric(float64(setupElapsed.Nanoseconds()), "setup-ns")
	var setupOverruns float64
	if setupElapsed > setupLimit {
		setupOverruns = 1
	}
	b.ReportMetric(setupOverruns, "setup-overruns")
	b.ReportMetric(float64(setupLimit.Nanoseconds()), "setup-limit-ns")
	b.ReportMetric(float64(operationLimit.Nanoseconds()), "operation-limit-ns")
	b.ReportMetric(float64(resetTime.Nanoseconds())/float64(b.N), "reset-ns/op")
	b.ReportMetric(float64(overruns)/float64(b.N), "budget-overruns/op")
	b.ReportMetric(float64(len(tc.files)), "total-files")
	b.ReportMetric(float64(tc.edited), "edited-files")
	b.ReportMetric(float64(tc.owners), "changed-owners")
	if tc.edited != 0 {
		b.ReportMetric(float64(fileVisits), "file-visits")
		b.ReportMetric(float64(readerVisits), "reader-visits")
		b.ReportMetric(float64(rawKeys), "raw-key-prepares")
	}
	if peakArm {
		b.ReportMetric(float64(peak), "peak-heap-B")
	}
}

type workBenchCase struct {
	name           string
	files          map[string]string
	before         map[string]string
	argv           []string
	warm           bool
	edited, owners int
	operationLimit time.Duration
	setupLimit     time.Duration
}

func (tc *workBenchCase) validateRun(
	b *testing.B,
	args []string,
	stdout, stderr *bytes.Buffer,
) workBenchValidation {
	b.Helper()
	proofArgs := args
	if tc.edited != 0 {
		proofArgs = append(append([]string{}, args...), "--log-level", "debug")
	}
	if code := Run(proofArgs, stdout, stderr); code != 0 {
		b.Fatalf("prevalidation exit=%d: %s", code, stderr)
	}
	want := sha256.Sum256(stdout.Bytes())
	if (tc.before == nil || !strings.HasSuffix(tc.name, "/interval")) && stdout.Len() == 0 {
		b.Fatal("prevalidation produced no output")
	}
	var work manifest.WorkCounts
	if tc.edited != 0 {
		work = workBenchProof(b, stderr.String())
	}

	return workBenchValidation{output: want, work: work}
}

func workBenchComplexityCases() []workBenchCase {
	cases := make([]workBenchCase, 0, 4)
	for _, count := range []int{100, 200} {
		before := workBenchFiles(1)
		var inputs strings.Builder
		inputs.WriteString("components: [../shared]\nconfigMapGenerator:\n  - name: file-inputs\n    files:\n")
		for i := range count {
			name := fmt.Sprintf("input-%04d.txt", i)
			fmt.Fprintf(&inputs, "      - files/%s\n", name)
			before["app/files/"+name] = "before\n"
		}
		before["app/kustomization.yaml"] = inputs.String()
		for _, mode := range []string{"logical-edit-many-files", "one-edited-file"} {
			after := maps.Clone(before)
			edited := count
			if mode == "one-edited-file" {
				edited = 1
			}
			for i := range edited {
				file := fmt.Sprintf("app/files/input-%04d.txt", i)
				after[file] = "after!\n"
			}
			cases = append(
				cases,
				workBenchCase{
					name: fmt.Sprintf(
						"complexity/%s/candidates_%d/edited_%d",
						mode,
						count,
						edited,
					),
					before: before,
					files:  after,
					argv: []string{
						"diff",
						"all",
						"-o",
						"diff",
					},
					warm:   true,
					edited: edited,
					owners: 1,
				},
			)
		}
	}
	return cases
}

func requireWorkBenchPersistence(b *testing.B, seed string) {
	b.Helper()
	owner, err := sourcecache.New(b.Context(), seed)
	if err != nil {
		b.Fatal(err)
	}
	persistent := owner.CanPersistRenders()
	if err := owner.Close(); err != nil {
		b.Fatal(err)
	}
	if !persistent {
		b.Fatal("warm rows require a clean VCS-stamped build with render persistence")
	}
}

func prepareWorkBench(b *testing.B, tc *workBenchCase) workBenchPreparation {
	b.Helper()
	root := testutil.WriteTree(b, tc.files)
	testutil.GitInit(b, root)
	cache := b.TempDir()
	seed := b.TempDir()
	args := append([]string{}, tc.argv...)
	args = append(args, "--path", filepath.Join(root, "flux"), "--cache-dir", cache)
	hasConcurrency := false
	for _, arg := range args {
		hasConcurrency = hasConcurrency || arg == "--concurrency"
	}
	if !hasConcurrency {
		args = append(args, "--concurrency", "4")
	}
	seedRoot := root
	if tc.before != nil {
		seedRoot = testutil.WriteTree(b, tc.before)
		testutil.GitInit(b, seedRoot)
		args = append(args, "--path-orig", filepath.Join(seedRoot, "flux"))
	}
	if tc.warm {
		requireWorkBenchPersistence(b, seed)
		var stderr bytes.Buffer
		if code := Run(
			[]string{
				"build",
				"all",
				"--path",
				filepath.Join(seedRoot, "flux"),
				"--cache-dir",
				seed,
				"--concurrency",
				"4",
			},
			io.Discard,
			&stderr,
		); code != 0 {
			if tc.before != nil {
				b.Fatalf("accepted BASE seed: %s", &stderr)
			}
			b.Fatalf("accepted seed: %s", &stderr)
		}
	}
	reset := func() {
		if err := os.RemoveAll(cache); err != nil {
			b.Fatal(err)
		}
		if err := os.MkdirAll(cache, 0o750); err != nil {
			b.Fatal(err)
		}
		if tc.warm {
			testutil.CopyTree(b, seed, cache)
		}
	}
	return workBenchPreparation{args: args, reset: reset}
}

func workBenchFiles(count int) map[string]string {
	files := map[string]string{
		"flux/kustomization.yaml": "resources: [owner.yaml, release.yaml, set.yaml, vars.yaml]\n",

		"flux/owner.yaml": testutil.KustomizationYAML(
			"app",
			"apps",
			"  interval: 1h\n  path: ./app\n  sourceRef: {kind: GitRepository, name: c"+
				"luster}\n  postBuild:\n    substituteFrom: [{kind: ConfigMap, name: vars}"+
				"]\n",
		),

		"flux/release.yaml": "apiVersion: helm.toolkit.fluxcd.io/v2\nkind: HelmRelease\nmetadata: {name" +
			": release, namespace: apps}\nspec:\n  interval: 1h\n  chart:\n    spec:\n" +
			"      chart: charts/demo\n      sourceRef: {kind: GitRepository, name: cl" +
			"uster}\n  values: {value: old}\n",

		"flux/set.yaml": "apiVersion: fluxcd.controlplane.io/v1\nkind: ResourceSet\nmetadata: {name" +
			": set, namespace: apps}\nspec:\n  inputs: [{name: one, value: old}]\n  re" +
			"sources:\n    - apiVersion: v1\n      kind: ConfigMap\n      metadata: {n" +
			"ame: 'set-<< inputs.name >>', namespace: apps}\n      data: {value: '<< i" +
			"nputs.value >>'}\n",

		"flux/vars.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: vars, namespace: apps}\ndata: {VALUE: old}\n",

		"charts/demo/Chart.yaml": "apiVersion: v2\nname: demo\nversion: 1.0.0\n",

		"charts/demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: release-output, namespa" +
			"ce: apps}\ndata: {value: {{ .Values.value | quote }}}\n",

		"shared/kustomization.yaml": "apiVersion: kustomize.config.k8s.io/v1alpha1\nkind: Component\ncommonAnnotations: {shared: old}\n",
	}
	var resources strings.Builder
	for i := range count {
		name := fmt.Sprintf("cm-%04d.yaml", i)
		fmt.Fprintf(&resources, "- %s\n", name)
		files["app/"+name] = fmt.Sprintf(
			"apiVersion: v1\nkind: ConfigMap\nmetadata: {name: output-%04d, namespace:"+
				" apps}\ndata: {value: '${VALUE}', image: 'busybox:1'}\n",
			i,
		)
	}
	files["app/kustomization.yaml"] = "components: [../shared]\nresources:\n" + resources.String()
	return files
}

func workBenchRemoteServer(b *testing.B) string {
	b.Helper()
	chart := testutil.TarGz(b, map[string]string{
		"demo/Chart.yaml":        "apiVersion: v2\nname: demo\nversion: 1.0.0\n",
		"demo/templates/cm.yaml": "apiVersion: v1\nkind: ConfigMap\nmetadata: {name: '{{ .Release.Name }}', namespace: apps}\n",
	}, 0o644, gzip.DefaultCompression)
	blobs := map[string][]byte{}
	descriptor := func(body []byte, mediaType string) ocispec.Descriptor {
		d := digest.FromBytes(body)
		blobs[d.String()] = body
		return ocispec.Descriptor{MediaType: mediaType, Digest: d, Size: int64(len(body))}
	}
	image := ocispec.Manifest{
		SchemaVersion: 2,
		MediaType:     ocispec.MediaTypeImageManifest,
		Config: descriptor(
			[]byte(
				`{}`,
			),
			"application/vnd.cncf.helm.config.v1+json",
		),
		Layers: []ocispec.Descriptor{
			descriptor(
				chart,
				"application/vnd.cncf.helm.chart.content.v1.tar+gzip",
			),
		},
	}
	imageBytes, err := json.Marshal(image)
	if err != nil {
		b.Fatal(err)
	}
	var index strings.Builder
	index.WriteString("apiVersion: v1\nentries:\n  demo:\n")
	for i := range 1000 {
		fmt.Fprintf(
			&index,
			"    - apiVersion: v2\n      name: demo\n      version: 1.0.%d\n      urls: [chart.tgz]\n",
			i,
		)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.URL.Path == "/index.yaml":
			_, _ = io.WriteString(w, index.String())
		case r.URL.Path == "/chart.tgz":
			_, _ = w.Write(chart)
		case r.URL.Path == "/v2/":
		case strings.Contains(r.URL.Path, "/manifests/"):
			w.Header().Set("Content-Type", ocispec.MediaTypeImageManifest)
			w.Header().Set("Docker-Content-Digest", digest.FromBytes(imageBytes).String())
			w.Header().Set("Content-Length", strconv.Itoa(len(imageBytes)))
			if r.Method != http.MethodHead {
				_, _ = w.Write(imageBytes)
			}
		case strings.Contains(r.URL.Path, "/blobs/"):
			_, key, _ := strings.CutLast(r.URL.Path, "/")
			if body, ok := blobs[key]; ok {
				_, _ = w.Write(body)
			} else {
				http.NotFound(w, r)
			}
		default:
			http.NotFound(w, r)
		}
	}))
	b.Cleanup(server.Close)
	return server.URL
}

func workBenchScopeCases(base map[string]string) []workBenchCase {
	cases := make([]workBenchCase, 0, 6)
	for _, scope := range []struct {
		name string
		argv []string
	}{
		{"hr", []string{"build", "hr", "release"}},
		{"ks", []string{"build", "ks", "app"}},
		{"namespace", []string{"build", "all", "-n", "apps"}},
	} {
		for _, warm := range []bool{false, true} {
			cases = append(cases, workBenchCase{name: "scope/" + scope.name, files: base, argv: scope.argv, warm: warm})
		}
	}
	return cases
}

func BenchmarkWorkCLI(b *testing.B) {
	cases := workBenchCases()
	cases = append(cases, workBenchRemoteCases(b)...)
	for i := range cases {
		tc := &cases[i]
		regime := "cold"
		if tc.warm {
			regime = "warm"
		}
		for _, arm := range []string{"allocations", "peak"} {
			b.Run(tc.name+"/"+regime+"/"+arm, func(b *testing.B) { benchmarkWorkRun(b, tc, arm == "peak") })
		}
	}
}

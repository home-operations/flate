package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"time"

	ocispec "github.com/opencontainers/image-spec/specs-go/v1"
	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resource"
	"sigs.k8s.io/kustomize/api/types"
	"sigs.k8s.io/kustomize/kyaml/filesys"

	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/tree"
)

// remoteFetchTimeout caps each pre-flight HTTP GET. Kustomize's
// built-in loader has no timeout knob; we want broken URLs to fail
// in seconds, not minutes.
const remoteFetchTimeout = 5 * time.Second

// remoteFetchMaxBytes caps each pre-flight response body. A
// kustomization resource is almost always under a megabyte of YAML;
// 64 MiB is several orders of magnitude of headroom that still
// bounds the OOM blast radius from a malicious or accidentally-huge
// URL endpoint.
const remoteFetchMaxBytes = 64 << 20 // 64 MiB

// Remote inputs live only in the private render overlay.
const remoteResourcePrefix = ".flate-remote-"

type inputKind uint8

const (
	fileInput inputKind = iota
	resourceInput
	pluginInput
	generatorInput
	strategicInput
)

type inputField struct {
	path string
	kind inputKind
}

var generatorFields = []inputField{{"files/*", generatorInput}, {"envs/*", fileInput}}

var controlFields = []inputField{
	{"resources/*", resourceInput}, {"bases/*", resourceInput}, {"components/*", resourceInput},
	{"patches/*/path", fileInput}, {"patchesJson6902/*/path", fileInput}, {"patchesStrategicMerge/*", strategicInput},
	{"replacements/*/path", fileInput}, {"configurations/*", fileInput}, {"crds/*", fileInput}, {"openapi/path", fileInput},
	{"configMapGenerator/*/files/*", generatorInput}, {"configMapGenerator/*/envs/*", fileInput}, {"configMapGenerator/*/env", fileInput},
	{"secretGenerator/*/files/*", generatorInput}, {"secretGenerator/*/envs/*", fileInput}, {"secretGenerator/*/env", fileInput},
	{"generators/*", pluginInput}, {"transformers/*", pluginInput}, {"validators/*", pluginInput},
}

// Plugin inputs resolve against the owning loader, even when the config is a
// resource of a separately accumulated directory. Resource leaves stay data
// unless they are reached through a declared plugin edge.
type controlLocation struct {
	file, root, pluginRoot string
	field                  string
	remote                 bool
}

type inputWalk struct {
	cache    *TreeCache
	fs       *treeFS
	visited  map[controlLocation][]byte
	mounts   []string
	injected bool
}

func preflightRemoteResources(ctx context.Context, cache *TreeCache, memFS filesys.FileSystem, subPath string) (bool, error) {
	w := inputWalk{cache: cache, fs: memFS.(*treeFS), visited: make(map[controlLocation][]byte)}
	err := w.directory(ctx, subPath, "")
	if err != nil {
		err = fmt.Errorf("%w: preflight: %w", manifest.ErrFlux, err)
	}
	return w.injected, err
}

func (w *inputWalk) directory(ctx context.Context, dir, pluginRoot string) error {
	canonical, _, err := w.fs.CleanedAbs(dir)
	if err != nil {
		return err
	}
	file, ok := kustomizationFileIn(w.fs, canonical.String())
	if !ok {
		return nil // Build owns missing and conflicting control-file diagnostics.
	}
	loc := controlLocation{file: file, root: canonical.String(), pluginRoot: pluginRoot}
	key := w.location(loc)
	if _, ok := w.visited[key]; ok {
		return nil // Preserve the graph so Build still rejects cycles.
	}
	body, err := w.fs.ReadFile(file)
	if err != nil {
		return err
	}
	// Build owns local parse diagnostics for byte-for-byte parity.
	var k types.Kustomization
	if err := k.Unmarshal(body); err != nil {
		return nil
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return nil
	}
	if err := yaml.NewRNode(&doc).DeAnchor(); err != nil {
		return nil
	}
	w.visited[key] = []byte(canonical.String())
	changed, err := w.fields(ctx, loc, &doc, controlFields)
	if loc.pluginRoot != "" && !w.injected {
		w.visited[key] = []byte(key.root)
	}
	if err != nil || !changed {
		return err
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return err
	}
	return w.fs.WriteFile(file, out)
}

// A plugin directory needs its own control view: the same directory may also
// emit ordinary resource data. Snapshot the whole tree to preserve parent paths;
// only a view containing rewritten remote inputs replaces the declared edge.
func (w *inputWalk) pluginDirectory(ctx context.Context, dir, owner string) (string, bool, error) {
	canonical, _, err := w.fs.CleanedAbs(dir)
	if err != nil {
		return "", false, err
	}
	selected := canonical.String()
	if file, ok := kustomizationFileIn(w.fs, selected); ok {
		key := w.location(controlLocation{file: file, root: selected, pluginRoot: owner})
		if prior, ok := w.visited[key]; ok {
			if string(prior) != key.root {
				selected = string(prior)
				w.injected = true
			}
		} else {
			snapshot, err := w.fs.upper.Build()
			if err != nil {
				return "", false, err
			}
			w.fs.upper = tree.NewBuilder(snapshot)
			name := remoteResourcePrefix + manifest.Fingerprint([]string{key.file, key.root, key.pluginRoot})
			if err := w.fs.upper.Mount(name, snapshot); err != nil {
				return "", false, err
			}
			w.fs.mark(name)
			mount := filepath.Join(w.fs.root, name)
			w.mounts = append(w.mounts, mount)
			rel, err := filepath.Rel(w.fs.root, key.root)
			if err != nil {
				return "", false, err
			}
			selected = filepath.Join(mount, rel)
			previous := w.injected
			w.injected = false
			err = w.directory(ctx, selected, owner)
			changed := w.injected
			w.injected = previous || changed
			if err != nil {
				return "", false, err
			}
			if !changed {
				selected = canonical.String()
			}
			w.visited[key] = []byte(selected)
		}
	}
	rel, err := filepath.Rel(owner, selected)
	return "./" + filepath.ToSlash(rel), selected != canonical.String(), err
}

func (w *inputWalk) location(loc controlLocation) controlLocation {
	for _, mount := range slices.Backward(w.mounts) {
		for _, coordinate := range []*string{&loc.file, &loc.root, &loc.pluginRoot} {
			if *coordinate == mount || strings.HasPrefix(*coordinate, mount+string(filepath.Separator)) {
				*coordinate = w.fs.root + strings.TrimPrefix(*coordinate, mount)
			}
		}
	}
	return loc
}

func (w *inputWalk) fields(ctx context.Context, loc controlLocation, doc *yaml.Node, fields []inputField) (bool, error) {
	changed := false
	for _, field := range fields {
		c, err := w.selectField(ctx, loc, doc, field)
		if err != nil {
			return false, err
		}
		changed = changed || c
	}
	return changed, nil
}

func (w *inputWalk) selectField(ctx context.Context, loc controlLocation, node *yaml.Node, field inputField) (bool, error) {
	if node == nil {
		return false, nil
	}
	if field.path == "" {
		if node.Kind != yaml.ScalarNode || node.Value == "" {
			return false, nil
		}
		changed, err := w.operand(ctx, loc, node, field.kind)
		if err != nil {
			return false, fmt.Errorf("%s %s %q (line %d): %w", loc.file, loc.field, node.Value, node.Line, err)
		}
		return changed, nil
	}
	part, rest, _ := strings.Cut(field.path, "/")
	field.path = rest
	if part != "*" {
		loc.field += "." + part
		return w.selectField(ctx, loc, findMappingValue(node, part), field)
	}
	changed := false
	if node.Kind == yaml.SequenceNode {
		for i, child := range node.Content {
			next := loc
			next.field += fmt.Sprintf("[%d]", i)
			c, err := w.selectField(ctx, next, child, field)
			if err != nil {
				return false, err
			}
			changed = changed || c
		}
	}
	return changed, nil
}

func (w *inputWalk) operand(ctx context.Context, loc controlLocation, node *yaml.Node, kind inputKind) (bool, error) {
	raw := node.Value
	if kind == strategicInput || kind == pluginInput {
		if rm, err := provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes([]byte(raw)); err == nil {
			if kind == strategicInput {
				return false, nil
			}
			out, changed, err := w.plugins(ctx, loc, []byte(raw), rm)
			if changed {
				node.Value = string(out)
			}
			return changed, err
		}
	}
	key := ""
	if kind == generatorInput {
		if strings.Count(raw, "=") > 1 {
			return false, nil // Upstream MUST retain its invalid file-source grammar.
		}
		var explicit bool
		key, raw, explicit = strings.Cut(raw, "=")
		if !explicit {
			raw, key = key, filepath.Base(key)
		} else if key == "" || raw == "" {
			return false, nil
		}
	}
	root := loc.root
	if kind == resourceInput || kind == pluginInput {
		if spec, ok := isGitRemoteBase(raw); ok {
			spec.dir = root
			dest, err := fetchGitBase(ctx, w.cache, w.fs, spec)
			if err != nil {
				return false, err
			}
			w.injected = true
			if kind == pluginInput {
				file, _, err := w.pluginDirectory(ctx, filepath.Join(root, dest), root)
				if err != nil {
					return false, err
				}
				setPlainScalar(node, file)
				return true, nil
			}
			setPlainScalar(node, dest)
			return true, w.directory(ctx, filepath.Join(root, dest), loc.pluginRoot)
		}
	}
	file := raw
	if !filepath.IsAbs(file) {
		file = filepath.Join(root, file)
	}
	remote := false
	if _, _, ok := cutHTTPScheme(raw); ok {
		body, err := w.cache.FetchRemote(ctx, raw, w.fs.retry)
		if err != nil {
			return false, err
		}
		file, err = w.mount(body)
		if err != nil {
			return false, err
		}
		remote = true
		w.injected = true
	} else if (kind == resourceInput || kind == pluginInput) && w.fs.IsDir(file) {
		if kind == pluginInput {
			dir, changed, err := w.pluginDirectory(ctx, file, root)
			if changed {
				setPlainScalar(node, dir)
			}
			return changed, err
		}
		return false, w.directory(ctx, file, loc.pluginRoot)
	}
	if kind == pluginInput || kind == resourceInput && loc.pluginRoot != "" {
		body, err := w.fs.ReadFile(file)
		if err != nil {
			return false, err
		}
		pluginLoc := controlLocation{file: file, root: root, remote: remote}
		if loc.pluginRoot != "" && kind == resourceInput {
			pluginLoc.root = loc.pluginRoot
		}
		out, changed, err := w.plugins(ctx, pluginLoc, body, nil)
		if err != nil {
			return false, err
		}
		if changed {
			file, err = w.mount(out)
			if err != nil {
				return false, err
			}
			remote = true
			w.injected = true
		}
	}
	if !remote {
		return false, nil
	}
	if kind == generatorInput {
		file = key + "=" + file
	}
	setPlainScalar(node, file)
	return true, nil
}

func (w *inputWalk) plugins(ctx context.Context, loc controlLocation, body []byte, resources []*resource.Resource) ([]byte, bool, error) {
	key := w.location(loc)
	key.field = manifest.Fingerprint(body)
	if out, ok := w.visited[key]; ok {
		return out, out != nil, nil
	}
	if resources == nil {
		var err error
		resources, err = provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes(body)
		if err != nil {
			if !loc.remote {
				return body, false, nil
			}
			return nil, false, fmt.Errorf("%w: plugin %s: %w", manifest.ErrInput, loc.file, err)
		}
	}
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	changed := false
	for _, resource := range resources {
		doc := resource.YNode()
		if err := yaml.NewRNode(doc).DeAnchor(); err != nil {
			if !loc.remote {
				return body, false, nil
			}
			return nil, false, fmt.Errorf("%w: plugin %s: %w", manifest.ErrInput, loc.file, err)
		}
		var fields []inputField
		api, kind := findMappingValue(doc, "apiVersion"), findMappingValue(doc, "kind")
		if api != nil && api.Value == "builtin" && kind != nil {
			switch kind.Value {
			case "ConfigMapGenerator", "SecretGenerator":
				fields = generatorFields
			case "PatchTransformer", "PatchJson6902Transformer":
				fields = []inputField{{"path", fileInput}}
			case "PatchStrategicMergeTransformer":
				fields = []inputField{{"paths/*", strategicInput}}
			case "ReplacementTransformer":
				fields = []inputField{{"replacements/*/path", fileInput}}
			case "ValueAddTransformer":
				fields = []inputField{{"targetFilePath", fileInput}}
			}
		}
		c, err := w.fields(ctx, loc, doc, fields)
		if err != nil {
			return nil, false, err
		}
		changed = changed || c
		if err := encoder.Encode(doc); err != nil {
			return nil, false, err
		}
	}
	if err := encoder.Close(); err != nil {
		return nil, false, err
	}
	if !changed {
		w.visited[key] = nil
		return body, false, nil
	}
	w.visited[key] = out.Bytes()
	return out.Bytes(), true, nil
}

func (w *inputWalk) mount(body []byte) (string, error) {
	file := filepath.Join(w.fs.root, remoteResourcePrefix+manifest.Fingerprint(body)+".yaml")
	return file, w.fs.WriteFile(file, body)
}

func setPlainScalar(node *yaml.Node, value string) {
	node.Value = value
	node.Tag = "!!str"
	node.Style = 0
}

func findMappingValue(node *yaml.Node, key string) *yaml.Node {
	if node == nil {
		return nil
	}
	if node.Kind == yaml.DocumentNode && len(node.Content) != 0 {
		node = node.Content[0]
	}
	var found *yaml.Node
	matched := ""
	if node.Kind == yaml.MappingNode {
		// Upstream decodes YAML through JSON's sorted keys and case-insensitive
		// struct fields; the last matching spelling owns the operand.
		for pair := range slices.Chunk(node.Content, 2) {
			if len(pair) == 2 && strings.EqualFold(pair[0].Value, key) && pair[0].Value >= matched {
				matched, found = pair[0].Value, pair[1]
			}
		}
	}
	return found
}

// httpGetURL preserves per-attempt bounds beneath FetchRemote's shared acquisition.
func (c *TreeCache) httpGetURL(ctx context.Context, urlStr string, w io.Writer) (result ocispec.Descriptor, resultErr error) {
	fetchCtx, cancel := context.WithTimeout(ctx, remoteFetchTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, urlStr, nil)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, resp.Body.Close()) }()
	if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return ocispec.Descriptor{}, &fetcherr.ResponseError{StatusCode: resp.StatusCode, Op: http.MethodGet}
	}
	// The extra byte detects overflow without buffering an unbounded response.
	size, err := io.Copy(w, io.LimitReader(resp.Body, remoteFetchMaxBytes+1))
	if err != nil {
		return ocispec.Descriptor{}, err
	}
	if size > remoteFetchMaxBytes {
		return ocispec.Descriptor{}, fmt.Errorf("response body exceeds %d-byte cap", remoteFetchMaxBytes)
	}
	return ocispec.Descriptor{Size: size}, nil
}

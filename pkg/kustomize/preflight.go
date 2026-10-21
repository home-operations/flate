package kustomize

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net/http"
	"path/filepath"
	"slices"
	"strings"

	"sigs.k8s.io/kustomize/api/provider"
	"sigs.k8s.io/kustomize/api/resource"
	"sigs.k8s.io/kustomize/api/types"

	"sigs.k8s.io/kustomize/kyaml/yaml"

	"github.com/home-operations/flate/internal/fetcherr"
	"github.com/home-operations/flate/pkg/manifest"
	"github.com/home-operations/flate/pkg/source"
	"github.com/home-operations/flate/pkg/source/cache"
	"github.com/home-operations/flate/pkg/tree"
)

// remoteFetchMaxBytes caps each pre-flight response body. A
// kustomization resource is almost always under a megabyte of YAML;
// 64 MiB is several orders of magnitude of headroom that still
// bounds the OOM blast radius from a malicious or accidentally-huge
// URL endpoint.
const remoteFetchMaxBytes = 64 << 20 // 64 MiB

// Remote inputs live only in the private render overlay.
const remoteResourcePrefix = ".flate-remote-"

const (
	httpOriginalURLAnnotation  = "flate.http-original-url"
	httpETagAnnotation         = "flate.http-etag"
	httpLastModifiedAnnotation = "flate.http-last-modified"
)

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
	cache   *TreeCache
	fs      *treeFS
	visited map[controlLocation]controlObservation
}

type controlObservation struct {
	out     []byte
	remotes map[remoteInput]struct{}
	parent  controlLocation
}

type walkScope struct {
	controlLocation
	parents     []remoteInput
	observation controlLocation
}

type inputResult struct {
	rewritten, injected bool
}

func replaced(changed bool) inputResult {
	return inputResult{rewritten: changed, injected: changed}
}

func (r inputResult) merge(other inputResult) inputResult {
	return inputResult{rewritten: r.rewritten || other.rewritten, injected: r.injected || other.injected}
}

type inputPath struct {
	file, replacement string
	directory         bool
	descriptor        remoteInput
}

func preflightRemoteResources(ctx context.Context, cache *TreeCache, memFS *treeFS, subPath string) error {
	w := inputWalk{cache: cache, fs: memFS, visited: make(map[controlLocation]controlObservation)}
	_, err := w.directory(ctx, subPath, walkScope{})
	if err != nil {
		err = fmt.Errorf("%w: preflight: %w", manifest.ErrFlux, err)
	}
	return err
}

func (w *inputWalk) directory(ctx context.Context, dir string, scope walkScope) (inputResult, error) {
	canonical, _, err := w.fs.CleanedAbs(dir)
	if err != nil {
		return inputResult{}, err
	}
	file, ok := kustomizationFileIn(w.fs, canonical.String())
	if !ok {
		return inputResult{}, nil // Build owns missing and conflicting control-file diagnostics.
	}
	loc := scope
	loc.controlLocation = controlLocation{file: file, root: canonical.String(), pluginRoot: scope.pluginRoot}
	key := w.location(loc.controlLocation)
	if prior, ok := w.visited[key]; ok {
		w.reuse(prior, scope)
		return inputResult{}, nil // Preserve the graph so Build still rejects cycles.
	}
	body, err := w.fs.ReadFile(file)
	if err != nil {
		return inputResult{}, err
	}
	// Build owns local parse diagnostics for byte-for-byte parity.
	var k types.Kustomization
	if err := k.Unmarshal(body); err != nil {
		return inputResult{}, nil
	}
	var doc yaml.Node
	if err := yaml.Unmarshal(body, &doc); err != nil {
		return inputResult{}, nil
	}
	if err := yaml.NewRNode(&doc).DeAnchor(); err != nil {
		return inputResult{}, nil
	}
	observation := controlObservation{out: []byte(canonical.String()), parent: scope.observation}
	w.visited[key] = observation
	loc.observation = key
	result, err := w.fields(ctx, loc, &doc, controlFields)
	observation = w.visited[key]
	if loc.pluginRoot != "" && !result.injected {
		observation.out = []byte(key.root)
	}
	w.visited[key] = observation
	if err != nil || !result.rewritten {
		return result, err
	}
	out, err := yaml.Marshal(&doc)
	if err != nil {
		return inputResult{}, err
	}
	return result, w.fs.WriteFile(file, out)
}

// A plugin directory needs its own control view: the same directory may also
// emit ordinary resource data. Snapshot the whole tree to preserve parent paths;
// only a view containing rewritten remote inputs replaces the declared edge.
func (w *inputWalk) pluginDirectory(ctx context.Context, dir, owner string, scope walkScope) (string, inputResult, error) {
	canonical, _, err := w.fs.CleanedAbs(dir)
	if err != nil {
		return "", inputResult{}, err
	}
	selected := canonical.String()
	var injected bool
	if file, ok := kustomizationFileIn(w.fs, selected); ok {
		key := w.location(controlLocation{file: file, root: selected, pluginRoot: owner})
		if prior, ok := w.visited[key]; ok {
			if string(prior.out) != key.root {
				selected = string(prior.out)
				injected = true
			}
		} else {
			snapshot, err := w.fs.upper.Build()
			if err != nil {
				return "", inputResult{}, err
			}
			w.fs.upper = tree.NewBuilder(snapshot)
			name := remoteResourcePrefix + manifest.Fingerprint([]string{key.file, key.root, key.pluginRoot})
			if err := w.fs.upper.Mount(name, snapshot); err != nil {
				return "", inputResult{}, err
			}
			w.fs.origins = append(w.fs.origins, mountOrigin{path: name, source: "."})
			mount := filepath.Join(w.fs.root, name)
			rel, err := filepath.Rel(w.fs.root, key.root)
			if err != nil {
				return "", inputResult{}, err
			}
			selected = filepath.Join(mount, rel)
			scope.pluginRoot = owner
			result, err := w.directory(ctx, selected, scope)
			if err != nil {
				return "", inputResult{}, err
			}
			if !result.injected {
				selected = canonical.String()
			}
			observation := w.visited[key]
			observation.out = []byte(selected)
			w.visited[key] = observation
		}
	}
	rel, err := filepath.Rel(owner, selected)
	result := replaced(selected != canonical.String())
	result.injected = result.injected || injected
	return "./" + filepath.ToSlash(rel), result, err
}

func (w *inputWalk) location(loc controlLocation) controlLocation {
	for _, coordinate := range []*string{&loc.file, &loc.root, &loc.pluginRoot} {
		if *coordinate == "" {
			continue
		}
		if rel, err := w.fs.rel(*coordinate); err == nil {
			if origin, _ := w.fs.origin(rel); origin != rel {
				*coordinate = w.fs.mount(origin)
			}
		}
	}
	return loc
}

func (w *inputWalk) fields(ctx context.Context, loc walkScope, doc *yaml.Node, fields []inputField) (inputResult, error) {
	var result inputResult
	for _, field := range fields {
		c, err := w.selectField(ctx, loc, doc, field)
		if err != nil {
			return inputResult{}, err
		}
		result = result.merge(c)
	}
	return result, nil
}

func (w *inputWalk) selectField(ctx context.Context, loc walkScope, node *yaml.Node, field inputField) (inputResult, error) {
	if node == nil {
		return inputResult{}, nil
	}
	if field.path == "" {
		if node.Kind != yaml.ScalarNode || node.Value == "" {
			return inputResult{}, nil
		}
		result, err := w.operand(ctx, loc, node, field.kind)
		if err != nil {
			return inputResult{}, fmt.Errorf("%s %s %q (line %d): %w", loc.file, loc.field, node.Value, node.Line, err)
		}
		return result, nil
	}
	part, rest, _ := strings.Cut(field.path, "/")
	field.path = rest
	if part != "*" {
		loc.field += "." + part
		return w.selectField(ctx, loc, findMappingValue(node, part), field)
	}
	var result inputResult
	if node.Kind == yaml.SequenceNode {
		for i, child := range node.Content {
			next := loc
			next.field += fmt.Sprintf("[%d]", i)
			c, err := w.selectField(ctx, next, child, field)
			if err != nil {
				return inputResult{}, err
			}
			result = result.merge(c)
		}
	}
	return result, nil
}

func (w *inputWalk) operand(ctx context.Context, loc walkScope, node *yaml.Node, kind inputKind) (inputResult, error) {
	switch kind {
	case resourceInput:
		return w.resourceOperand(ctx, loc, node)
	case pluginInput:
		return w.pluginOperand(ctx, loc, node)
	case generatorInput:
		return w.generatorOperand(ctx, loc, node)
	case strategicInput:
		return w.strategicOperand(ctx, loc, node)
	default:
		return w.fileOperand(ctx, loc, node)
	}
}

func (w *inputWalk) fileOperand(ctx context.Context, loc walkScope, node *yaml.Node) (inputResult, error) {
	path, err := w.filePath(ctx, loc, node.Value)
	result := replaced(path.replacement != "")
	if err == nil && result.rewritten {
		setPlainScalar(node, path.replacement)
	}
	return result, err
}

func (w *inputWalk) generatorOperand(ctx context.Context, loc walkScope, node *yaml.Node) (inputResult, error) {
	if strings.Count(node.Value, "=") > 1 {
		return inputResult{}, nil // Upstream MUST retain its invalid file-source grammar.
	}
	key, raw, explicit := strings.Cut(node.Value, "=")
	if !explicit {
		raw, key = key, filepath.Base(key)
	} else if key == "" || raw == "" {
		return inputResult{}, nil
	}
	file := *node
	file.Value = raw
	result, err := w.fileOperand(ctx, loc, &file)
	if result.rewritten {
		setPlainScalar(node, key+"="+file.Value)
	}
	return result, err
}

func (w *inputWalk) strategicOperand(ctx context.Context, loc walkScope, node *yaml.Node) (inputResult, error) {
	if _, err := provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes([]byte(node.Value)); err == nil {
		return inputResult{}, nil
	}
	return w.fileOperand(ctx, loc, node)
}

func (w *inputWalk) resourceOperand(ctx context.Context, loc walkScope, node *yaml.Node) (inputResult, error) {
	path, err := w.resourcePath(ctx, loc, node.Value)
	if err != nil {
		return inputResult{}, err
	}
	result := replaced(path.replacement != "")
	if path.directory {
		if path.descriptor.URL != "" {
			loc.parents = append(loc.parents, path.descriptor)
		}
		if result.rewritten {
			setPlainScalar(node, path.replacement)
		}
		child, err := w.directory(ctx, path.file, loc)
		result.injected = result.injected || child.injected
		return result, err
	}
	if loc.pluginRoot != "" {
		loc.root = loc.pluginRoot
		return w.pluginFile(ctx, loc, node, path)
	}
	if result.rewritten {
		setPlainScalar(node, path.replacement)
	}
	return result, nil
}

func (w *inputWalk) pluginOperand(ctx context.Context, loc walkScope, node *yaml.Node) (inputResult, error) {
	if rm, err := provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes([]byte(node.Value)); err == nil {
		out, result, err := w.plugins(ctx, loc, []byte(node.Value), rm)
		if result.rewritten {
			node.Value = string(out)
		}
		return result, err
	}
	path, err := w.resourcePath(ctx, loc, node.Value)
	if err != nil {
		return inputResult{}, err
	}
	if !path.directory {
		return w.pluginFile(ctx, loc, node, path)
	}
	if path.descriptor.URL != "" {
		loc.parents = append(loc.parents, path.descriptor)
	}
	dir, result, err := w.pluginDirectory(ctx, path.file, loc.root, loc)
	result.rewritten = result.rewritten || path.replacement != ""
	result.injected = result.injected || path.replacement != ""
	if err == nil && result.rewritten {
		setPlainScalar(node, dir)
	}
	return result, err
}

func (w *inputWalk) acquire(ctx context.Context, descriptor remoteInput, scope walkScope) (fs.FS, error) {
	acquired, identity, err := descriptor.acquire(ctx, w.cache, w.fs)
	if err == nil {
		w.recordRemote(descriptor, identity, scope)
	}
	return acquired, err
}

func (w *inputWalk) recordRemote(descriptor remoteInput, identity remoteIdentity, scope walkScope) {
	if w.fs.rec == nil {
		return
	}
	w.fs.rec.recordRemote(descriptor, identity, scope.parents)
	for key := scope.observation; key.file != ""; {
		observation := w.visited[key]
		if observation.remotes == nil {
			observation.remotes = make(map[remoteInput]struct{})
		}
		observation.remotes[descriptor] = struct{}{}
		w.visited[key] = observation
		key = observation.parent
	}
}

func (w *inputWalk) reuse(prior controlObservation, scope walkScope) {
	if w.fs.rec == nil {
		return
	}
	for descriptor := range prior.remotes {
		w.recordRemote(descriptor, w.fs.rec.remotes[descriptor].identity, scope)
	}
}

func (w *inputWalk) filePath(ctx context.Context, loc walkScope, raw string) (inputPath, error) {
	file := raw
	if !filepath.IsAbs(file) {
		file = filepath.Join(loc.root, file)
	}
	if _, _, ok := cutHTTPScheme(raw); !ok {
		return inputPath{file: file}, nil
	}
	descriptor := remoteInput{Operation: remoteHTTP, URL: raw}
	acquired, err := w.acquire(ctx, descriptor, loc)
	if err != nil {
		return inputPath{}, err
	}
	body, err := fs.ReadFile(acquired, "content")
	if err != nil {
		return inputPath{}, err
	}
	file, err = w.mount(body)
	return inputPath{file: file, replacement: file, descriptor: descriptor}, err
}

func (w *inputWalk) resourcePath(ctx context.Context, loc walkScope, raw string) (inputPath, error) {
	if spec, ok := isGitRemoteBase(raw); ok {
		spec.dir = loc.root
		dest, err := w.fetchGitBase(ctx, spec, loc)
		return inputPath{file: filepath.Join(loc.root, dest), replacement: dest, directory: true, descriptor: remoteInput{Operation: remoteGit, URL: spec.repoURL, Ref: spec.ref}}, err
	}
	path, err := w.filePath(ctx, loc, raw)
	path.directory = err == nil && path.replacement == "" && w.fs.IsDir(path.file)
	return path, err
}

func (w *inputWalk) pluginFile(ctx context.Context, loc walkScope, node *yaml.Node, path inputPath) (inputResult, error) {
	loc.controlLocation = controlLocation{file: path.file, root: loc.root, remote: path.replacement != ""}
	if path.descriptor.URL != "" {
		loc.parents = append(loc.parents, path.descriptor)
	}
	body, err := w.fs.ReadFile(path.file)
	if err != nil {
		return inputResult{}, err
	}
	out, result, err := w.plugins(ctx, loc, body, nil)
	if err != nil {
		return inputResult{}, err
	}
	if result.rewritten {
		path.replacement, err = w.mount(out)
	}
	result.rewritten = path.replacement != ""
	result.injected = result.injected || result.rewritten
	if err == nil && result.rewritten {
		setPlainScalar(node, path.replacement)
	}
	return result, err
}

func (w *inputWalk) plugins(ctx context.Context, loc walkScope, body []byte, resources []*resource.Resource) ([]byte, inputResult, error) {
	key := w.location(loc.controlLocation)
	key.field = manifest.Fingerprint(body)
	if prior, ok := w.visited[key]; ok {
		w.reuse(prior, loc)
		return prior.out, replaced(prior.out != nil), nil
	}
	if resources == nil {
		var err error
		resources, err = provider.NewDefaultDepProvider().GetResourceFactory().SliceFromBytes(body)
		if err != nil {
			if !loc.remote {
				return body, inputResult{}, nil
			}
			return nil, inputResult{}, fmt.Errorf("%w: plugin %s: %w", manifest.ErrInput, loc.file, err)
		}
	}
	w.visited[key] = controlObservation{parent: loc.observation}
	loc.observation = key
	var out bytes.Buffer
	encoder := yaml.NewEncoder(&out)
	var result inputResult
	for _, resource := range resources {
		doc := resource.YNode()
		if err := yaml.NewRNode(doc).DeAnchor(); err != nil {
			if !loc.remote {
				return body, inputResult{}, nil
			}
			return nil, inputResult{}, fmt.Errorf("%w: plugin %s: %w", manifest.ErrInput, loc.file, err)
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
			return nil, inputResult{}, err
		}
		result = result.merge(c)
		if err := encoder.Encode(doc); err != nil {
			return nil, inputResult{}, err
		}
	}
	if err := encoder.Close(); err != nil {
		return nil, inputResult{}, err
	}
	observation := w.visited[key]
	if result.rewritten {
		observation.out = out.Bytes()
	}
	w.visited[key] = observation
	if !result.rewritten {
		return body, inputResult{}, nil
	}
	return out.Bytes(), result, nil
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

func (c *TreeCache) httpGetURL(ctx context.Context, urlStr string, w cache.ProducerInput) (result cache.ProducerResult, resultErr error) {
	fetchCtx, cancel := context.WithTimeout(ctx, source.HTTPResource.Timeout())
	defer cancel()
	previous, err := w.Previous(fetchCtx)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	req, err := http.NewRequestWithContext(fetchCtx, http.MethodGet, urlStr, nil)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	if previous != nil && previous.Annotations[httpOriginalURLAnnotation] == "true" {
		if etag := previous.Annotations[httpETagAnnotation]; etag != "" {
			req.Header.Set("If-None-Match", etag)
		}
		if modified := previous.Annotations[httpLastModifiedAnnotation]; modified != "" {
			req.Header.Set("If-Modified-Since", modified)
		}
	}
	resp, err := c.httpClient.Do(req)
	if err != nil {
		return cache.ProducerResult{}, err
	}
	defer func() { resultErr = errors.Join(resultErr, resp.Body.Close()) }()
	if resp.StatusCode == http.StatusNotModified && previous != nil && (resp.Request.Header.Get("If-None-Match") != "" || resp.Request.Header.Get("If-Modified-Since") != "") {
		result.Descriptor, result.Unchanged = *previous, true
	} else if resp.StatusCode < 200 || resp.StatusCode > 299 {
		return cache.ProducerResult{}, &fetcherr.ResponseError{StatusCode: resp.StatusCode, Op: http.MethodGet}
	} else {
		result.Annotations = make(map[string]string)
		requestURL, responseURL := *req.URL, *resp.Request.URL
		if requestURL.User != nil && responseURL.User != nil && *requestURL.User == *responseURL.User {
			responseURL.User = requestURL.User
		}
		if requestURL == responseURL {
			result.Annotations[httpOriginalURLAnnotation] = "true"
		}
	}
	for _, validator := range [...]struct{ header, annotation string }{{"ETag", httpETagAnnotation}, {"Last-Modified", httpLastModifiedAnnotation}} {
		if _, supplied := resp.Header[http.CanonicalHeaderKey(validator.header)]; supplied {
			result.Annotations[validator.annotation] = resp.Header.Get(validator.header)
		}
	}
	if result.Unchanged {
		return result, nil
	}
	// The extra byte detects overflow without buffering an unbounded response.
	size, err := io.Copy(w, io.LimitReader(resp.Body, remoteFetchMaxBytes+1))
	if err != nil {
		return cache.ProducerResult{}, err
	}
	if size > remoteFetchMaxBytes {
		return cache.ProducerResult{}, fmt.Errorf("response body exceeds %d-byte cap", remoteFetchMaxBytes)
	}
	result.Size = size
	return result, nil
}

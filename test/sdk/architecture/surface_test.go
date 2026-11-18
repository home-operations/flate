package architecture_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"maps"
	"os"
	"os/exec"
	"slices"
	"strings"
	"testing"

	"github.com/home-operations/flate/internal/testutil/assert"
)

const modulePath = "github.com/home-operations/flate/"

func TestPublicSurface_NoImplementationTypes(t *testing.T) {
	t.Parallel()
	paths := []string{modulePath + "pkg/manifest", modulePath + "pkg/orchestrator"}
	cmd := exec.CommandContext(t.Context(), "go",
		append([]string{"list", "-export", "-deps", "-json"}, paths...)...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	output, err := cmd.Output()
	assert.FatalIf(t, err != nil, "go list: %v\n%s", err, &stderr)
	archives := make(map[string]string)
	decoder := json.NewDecoder(bytes.NewReader(output))
	for {
		var pkg struct {
			ImportPath string `json:"ImportPath"`
			Export     string `json:"Export"`
		}
		err := decoder.Decode(&pkg)
		if err == io.EOF {
			break
		}
		assert.NoError(t, err)
		archives[pkg.ImportPath] = pkg.Export
	}
	imp := importer.ForCompiler(token.NewFileSet(), "gc", func(path string) (io.ReadCloser, error) {
		return os.Open(archives[path])
	})
	for _, path := range paths {
		pkg, err := imp.Import(path)
		assert.NoError(t, err)
		assert.Diff(t, surfaceLeaks(pkg), []string(nil))
	}
}

type surfaceWalker struct {
	seen  map[types.Type]bool
	leaks []string
}

func surfaceLeaks(pkg *types.Package) []string {
	w := &surfaceWalker{seen: make(map[types.Type]bool)}
	for _, name := range pkg.Scope().Names() {
		obj := pkg.Scope().Lookup(name)
		if obj.Exported() {
			w.walk(obj.Type(), pkg.Path()+"."+name)
		}
	}
	slices.Sort(w.leaks)
	return w.leaks
}

func (w *surfaceWalker) owner(obj *types.TypeName, path string) {
	pkg := obj.Pkg()
	if pkg != nil && strings.HasPrefix(pkg.Path(), modulePath) &&
		pkg.Path() != modulePath+"pkg/manifest" && pkg.Path() != modulePath+"pkg/orchestrator" {
		w.leaks = append(w.leaks, path+" -> "+pkg.Path()+"."+obj.Name())
	}
}

func (w *surfaceWalker) walk(typ types.Type, path string) {
	if typ == nil || w.seen[typ] {
		return
	}
	w.seen[typ] = true
	switch typ := typ.(type) {
	case *types.Alias:
		w.owner(typ.Obj(), path)
		w.parameters(typ.TypeParams(), path)
		w.arguments(typ.TypeArgs(), path)
		w.walk(typ.Rhs(), path+".alias")
	case *types.Named:
		w.owner(typ.Obj(), path)
		w.parameters(typ.TypeParams(), path)
		w.arguments(typ.TypeArgs(), path)
		if _, ok := typ.Underlying().(*types.Struct); ok {
			w.fields(typ, path)
		} else {
			w.walk(typ.Underlying(), path)
		}
		w.methods(typ, path)
		w.methods(types.NewPointer(typ), path)
	case *types.Pointer:
		w.walk(typ.Elem(), path+".element")
	case *types.Array:
		w.walk(typ.Elem(), path+".element")
	case *types.Slice:
		w.walk(typ.Elem(), path+".element")
	case *types.Chan:
		w.walk(typ.Elem(), path+".element")
	case *types.Map:
		w.walk(typ.Key(), path+".key")
		w.walk(typ.Elem(), path+".element")
	case *types.Struct:
		w.fields(typ, path)
	case *types.Signature:
		w.parameters(typ.RecvTypeParams(), path)
		w.parameters(typ.TypeParams(), path)
		w.walk(typ.Params(), path+".parameters")
		w.walk(typ.Results(), path+".results")
	case *types.Tuple:
		for i := range typ.Len() {
			w.walk(typ.At(i).Type(), fmt.Sprintf("%s[%d]", path, i))
		}
	case *types.Interface:
		w.methods(typ, path)
		for i := range typ.NumEmbeddeds() {
			w.walk(typ.EmbeddedType(i), fmt.Sprintf("%s.embedded[%d]", path, i))
		}
	case *types.TypeParam:
		w.walk(typ.Constraint(), path+".constraint")
	case *types.Union:
		for i := range typ.Len() {
			w.walk(typ.Term(i).Type(), fmt.Sprintf("%s.term[%d]", path, i))
		}
	}
}

func (w *surfaceWalker) methods(typ types.Type, path string) {
	for method := range types.NewMethodSet(typ).Methods() {
		if method.Obj().Exported() {
			w.walk(method.Type(), path+"."+method.Obj().Name())
		}
	}
}

func (w *surfaceWalker) parameters(params *types.TypeParamList, path string) {
	for i := range params.Len() {
		w.walk(params.At(i), fmt.Sprintf("%s.typeParameter[%d]", path, i))
	}
}

func (w *surfaceWalker) arguments(args *types.TypeList, path string) {
	for i := range args.Len() {
		w.walk(args.At(i), fmt.Sprintf("%s.typeArgument[%d]", path, i))
	}
}

func (w *surfaceWalker) fields(typ types.Type, path string) {
	names := make(map[string]bool)
	seen := make(map[types.Type]bool)
	var collect func(types.Type)
	collect = func(typ types.Type) {
		typ = types.Unalias(typ)
		if pointer, ok := typ.(*types.Pointer); ok {
			typ = types.Unalias(pointer.Elem())
		}
		if seen[typ] {
			return
		}
		seen[typ] = true
		fields, ok := typ.Underlying().(*types.Struct)
		if !ok {
			return
		}
		for field := range fields.Fields() {
			if field.Exported() {
				names[field.Name()] = true
			}
			if field.Embedded() {
				collect(field.Type())
			}
		}
	}
	collect(typ)
	for _, name := range slices.Sorted(maps.Keys(names)) {
		field, _, _ := types.LookupFieldOrMethod(typ, true, nil, name)
		if field, ok := field.(*types.Var); ok {
			w.walk(field.Type(), path+"."+name)
		}
	}
}

type fixtureImporter map[string]*types.Package

func (f fixtureImporter) Import(path string) (*types.Package, error) {
	return f[path], nil
}

func TestPublicSurface_Traversal(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, source, reach string
	}{
		{"private ownership", `type API struct { owner bad.Leak }`, ""},
		{"private embedding without exported members", `type hidden struct { owner bad.Leak }; type API struct { hidden }`, ""},
		{"promoted field", `type hidden struct { Value bad.Leak }; type API struct { hidden }`, ".API.Value"},
		{"promoted value method", `type hidden struct{}; func (hidden) Value() bad.Leak { panic(0) }; type API struct { hidden }`, ".API.Value.results[0]"},
		{"promoted pointer method", `type hidden struct{}; func (*hidden) Value() bad.Leak { panic(0) }; type API struct { hidden }`, ".API.Value.results[0]"},
		{"upstream generic argument", `type API struct { Value box.Box[bad.Leak] }`, ".API.Value.typeArgument[0]"},
		{"generic alias", `type API = box.Box[bad.Leak]`, ".API.alias.typeArgument[0]"},
		{"generic alias constraint", `type API[T interface { ~[]bad.Leak }] = box.Box[T]`, ".API.typeParameter[0].constraint.embedded[0].term[0].element"},
		{"generic constraint", `type API[T interface { ~[]bad.Leak }] struct{}`, ".API.typeParameter[0].constraint.embedded[0].term[0].element"},
		{"callback container", `func API() map[string][]func(chan<- *bad.Leak) { panic(0) }`, ".API.results[0].element.element.parameters[0].element.element"},
		{"array", `type API [2]bad.Leak`, ".API.element"},
		{"interface", `type API interface { Value() bad.Leak }`, ".API.Value.results[0]"},
		{"upstream allowed", `type API struct { Value box.Box[string] }`, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			fset := token.NewFileSet()
			imports := make(fixtureImporter)
			for _, fixture := range []struct{ path, source string }{
				{modulePath + "internal/engine/store", "package store; type Leak struct{}"},
				{"example.com/box", "package box; type Box[T any] struct { value T }"},
				{modulePath + "pkg/orchestrator", `package orchestrator; import bad "` + modulePath + `internal/engine/store"; import box "example.com/box"; ` + tc.source},
			} {
				file, err := parser.ParseFile(fset, "fixture.go", fixture.source, 0)
				assert.NoError(t, err)
				cfg := types.Config{Importer: imports, DisableUnusedImportCheck: true}
				pkg, err := cfg.Check(fixture.path, fset, []*ast.File{file}, nil)
				assert.NoError(t, err)
				imports[fixture.path] = pkg
			}
			var want []string
			if tc.reach != "" {
				want = []string{
					modulePath + "pkg/orchestrator" + tc.reach + " -> " + modulePath + "internal/engine/store.Leak",
				}
			}
			assert.Diff(t, surfaceLeaks(imports[modulePath+"pkg/orchestrator"]), want)
		})
	}
}

package artifactgenerator

import (
	"context"
	"fmt"
	"maps"
	"slices"

	"cel.dev/cel-go/cel"
	"cel.dev/cel-go/common/types"
	"cel.dev/cel-go/ext"

	"github.com/home-operations/flate/pkg/manifest"
)

const (
	nameVariable     = "ns"
	metadataVariable = "value"
)

type namespacePolicies struct {
	names       []stringAssertion
	labels      map[string]stringAssertion
	annotations map[string]stringAssertion
}

type stringAssertion struct {
	expression string
	program    cel.Program
}

func compileNamespacePolicies(ns *manifest.ArtifactGeneratorManagedNamespaces) (*namespacePolicies, error) {
	if ns == nil {
		return nil, nil //nolint:nilnil // An absent policy disables Namespace management.
	}
	if len(ns.AllowedNames) == 0 {
		return nil, fmt.Errorf(".spec.managedNamespaces.allowedNames must contain at least one expression")
	}
	env, err := namespacePolicyEnv(nameVariable)
	if err != nil {
		return nil, err
	}
	policies := &namespacePolicies{names: make([]stringAssertion, 0, len(ns.AllowedNames))}
	for _, expr := range ns.AllowedNames {
		assertion, compileErr := compileStringAssertion(env, expr)
		if compileErr != nil {
			return nil, fmt.Errorf("invalid .spec.managedNamespaces.allowedNames expression %q: %w", expr, compileErr)
		}
		policies.names = append(policies.names, assertion)
	}
	if ns.Metadata == nil || ns.Metadata.FromSource == nil {
		return policies, nil
	}
	source := ns.Metadata.FromSource
	if len(source.AllowedLabels)+len(source.AllowedAnnotations) == 0 {
		return policies, nil
	}
	env, err = namespacePolicyEnv(metadataVariable)
	if err != nil {
		return nil, err
	}
	policies.labels, err = compileMetadataAssertions(env, source.AllowedLabels, "label")
	if err != nil {
		return nil, err
	}
	policies.annotations, err = compileMetadataAssertions(env, source.AllowedAnnotations, "annotation")
	if err != nil {
		return nil, err
	}
	return policies, nil
}

func namespacePolicyEnv(variable string) (*cel.Env, error) {
	env, err := cel.NewEnv(
		cel.HomogeneousAggregateLiterals(),
		cel.EagerlyValidateDeclarations(true),
		cel.DefaultUTCTimeZone(true),
		cel.CrossTypeNumericComparisons(true),
		cel.OptionalTypes(),
		ext.Strings(),
		ext.Sets(),
		ext.Encoders(),
		cel.Variable(variable, cel.StringType),
	)
	if err != nil {
		return nil, fmt.Errorf("failed to create CEL environment: %w", err)
	}
	return env, nil
}

func compileStringAssertion(env *cel.Env, expression string) (stringAssertion, error) {
	checked, issues := env.Compile(expression)
	if issues != nil {
		return stringAssertion{}, fmt.Errorf("failed to parse the CEL expression '%s': %s", expression, issues)
	}
	if got := checked.OutputType(); got != cel.BoolType {
		return stringAssertion{}, fmt.Errorf(
			"CEL expression output type mismatch: expected %s, got %s",
			cel.BoolType,
			got,
		)
	}
	const interruptFrequency = 100
	program, err := env.Program(
		checked,
		cel.EvalOptions(cel.OptOptimize),
		cel.InterruptCheckFrequency(interruptFrequency),
	)
	if err != nil {
		return stringAssertion{}, fmt.Errorf("failed to create CEL program: %w", err)
	}
	return stringAssertion{expression: expression, program: program}, nil
}

func compileMetadataAssertions(
	env *cel.Env,
	expressions map[string]string,
	kind string,
) (map[string]stringAssertion, error) {
	if len(expressions) == 0 {
		return nil, nil
	}
	compiled := make(map[string]stringAssertion, len(expressions))
	for _, key := range slices.Sorted(maps.Keys(expressions)) {
		assertion, err := compileStringAssertion(env, expressions[key])
		if err != nil {
			return nil, fmt.Errorf("invalid allowed %s expression for key %q: %w", kind, key, err)
		}
		compiled[key] = assertion
	}
	return compiled, nil
}

func (e stringAssertion) evaluate(ctx context.Context, data map[string]any) (bool, error) {
	value, _, err := e.program.ContextEval(ctx, data)
	if err != nil {
		return false, fmt.Errorf("failed to evaluate the CEL expression '%s': %w", e.expression, err)
	}
	result, ok := value.(types.Bool)
	if !ok {
		return false, fmt.Errorf("failed to evaluate CEL expression as boolean: '%s'", e.expression)
	}
	return bool(result), nil
}

func (p *namespacePolicies) validateName(ctx context.Context, name string) error {
	switch name {
	case "default", "kube-system", "kube-public", "flux-system":
		return fmt.Errorf("namespace %q cannot be managed by an ArtifactGenerator", name)
	}
	data := map[string]any{nameVariable: name}
	for _, assertion := range p.names {
		allowed, err := assertion.evaluate(ctx, data)
		if err != nil {
			return fmt.Errorf(
				"failed to evaluate .spec.managedNamespaces.allowedNames expression %q: %w",
				assertion.expression,
				err,
			)
		}
		if allowed {
			return nil
		}
	}
	return fmt.Errorf("namespace %q is not allowed by .spec.managedNamespaces.allowedNames", name)
}

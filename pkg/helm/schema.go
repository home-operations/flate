package helm

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"sync"

	"github.com/santhosh-tekuri/jsonschema/v6"
	"helm.sh/helm/v4/pkg/chart/common/util"
	chart "helm.sh/helm/v4/pkg/chart/v2"

	"github.com/home-operations/flate/pkg/manifest"
)

// validateChartSchema reproduces helm's
// util.ToRenderValuesWithSchemaValidation validation step — coalesce the
// values, then validate the chart (and each subchart) against its
// values.schema.json — but compiles each schema exactly once (cached by
// schema-bytes hash) instead of helm's per-render recompile. The compile
// (meta-validating a large schema such as bjw-s app-template's) dominates
// cold-start allocation when one chart backs many HelmReleases; caching it
// removes that churn while keeping per-release value validation, the
// coalesce, and the surfaced error byte-identical to helm.
//
// A coalesce error returns nil: helm's RunWithContext re-coalesces and
// surfaces the identical error, so flate defers to it rather than risk a
// divergent message. The caller wraps a non-nil return like helm does.
func (c *Client) validateChartSchema(chrt *chart.Chart, chrtVals map[string]any) error {
	// Coalesce exactly as helm's RunWithContext does (same util.CoalesceValues
	// on the same values) so flate validates precisely what helm renders.
	// Coalesce fills missing keys from chart defaults — idempotent, so helm's
	// subsequent re-coalesce of the same values is a no-op (pinned by the
	// byte-identical render test).
	vals, err := util.CoalesceValues(chrt, chrtVals)
	if err != nil {
		return nil
	}
	if err := c.validateAgainstSchema(chrt, map[string]any(vals)); err != nil {
		return fmt.Errorf("values don't meet the specifications of the schema(s) in the following chart(s):\n%w", err)
	}
	return nil
}

// validateAgainstSchema mirrors helm util.ValidateAgainstSchema: validate
// the chart's own schema then recurse into each subchart with its slice of
// the coalesced values, accumulating per-chart messages.
func (c *Client) validateAgainstSchema(chrt *chart.Chart, vals map[string]any) error {
	var sb strings.Builder
	if len(chrt.Schema) > 0 {
		if err := c.validateAgainstSingleSchema(vals, chrt.Schema); err != nil {
			fmt.Fprintf(&sb, "%s:\n", chrt.Name())
			sb.WriteString(err.Error())
		}
	}
	for _, sub := range chrt.Dependencies() {
		raw, exists := vals[sub.Name()]
		if !exists || raw == nil {
			continue
		}
		subVals, ok := raw.(map[string]any)
		if !ok {
			fmt.Fprintf(&sb, "%s:\ninvalid type for values: expected object (map), got %T\n", sub.Name(), raw)
			continue
		}
		if err := c.validateAgainstSchema(sub, subVals); err != nil {
			sb.WriteString(err.Error())
		}
	}
	if sb.Len() > 0 {
		return errors.New(sb.String())
	}
	return nil
}

// validateAgainstSingleSchema mirrors helm util.ValidateAgainstSingleSchema
// (including its panic-to-error recover and error formatting) but resolves
// the compiled schema through the per-Client cache.
func (c *Client) validateAgainstSingleSchema(vals map[string]any, schemaJSON []byte) (reterr error) {
	defer func() {
		if r := recover(); r != nil {
			reterr = fmt.Errorf("unable to validate schema: %s", r)
		}
	}()
	validator, err := c.compileSchema(schemaJSON)
	if err != nil {
		return err
	}
	if err := validator.Validate(vals); err != nil {
		// Match helm util.JSONSchemaValidationError.Error() formatting so
		// the surfaced message is identical to helm's own validator (its
		// error type has an unexported field and can't be reused here).
		msg := strings.TrimPrefix(err.Error(), "jsonschema validation failed with 'file:///values.schema.json#'\n")
		return errors.New(msg + "\n")
	}
	return nil
}

// schemaEntry memoizes one compiled schema. sync.Once gives single-flight
// compilation per schema-bytes hash: N parallel HelmReleases sharing a
// chart compile its schema exactly once.
type schemaEntry struct {
	once     sync.Once
	compiled *jsonschema.Schema
	err      error
}

// compileSchema returns the compiled validator for schemaJSON, caching by
// content hash. The compiler setup mirrors helm's ValidateAgainstSingleSchema
// (UnmarshalJSON with number precision, the file:///values.schema.json
// resource). The http/https/urn loaders helm wires are omitted: flate is
// offline, so a remote $ref is unresolvable either way, and self-contained
// schemas (the overwhelming majority, including app-template) never touch a
// loader, so the compiled result is identical.
func (c *Client) compileSchema(schemaJSON []byte) (*jsonschema.Schema, error) {
	sum := sha256.Sum256(schemaJSON)
	v, _ := c.schemaCache.LoadOrStore(string(sum[:]), &schemaEntry{})
	e := v.(*schemaEntry)
	e.once.Do(func() {
		schema, err := jsonschema.UnmarshalJSON(bytes.NewReader(schemaJSON))
		if err != nil {
			e.err = err
			return
		}
		compiler := jsonschema.NewCompiler()
		compiler.UseLoader(jsonschema.SchemeURLLoader{"file": jsonschema.FileLoader{}})
		if err := compiler.AddResource("file:///values.schema.json", schema); err != nil {
			e.err = err
			return
		}
		e.compiled, e.err = compiler.Compile("file:///values.schema.json")
	})
	return e.compiled, e.err
}

// schemaValidationSkipped reports whether values.schema.json validation is
// bypassed for this render — the CLI flag, the HR opt-out, or a wipe
// placeholder in the values (which a DNS/URL/regex schema would reject for a
// value flate fabricated). Mirrors the original gate at the install site.
func schemaValidationSkipped(opts Options, hr *manifest.HelmRelease, hrValues map[string]any) bool {
	return opts.SkipSchemaValidation || hr.DisableSchemaValidation || manifest.ContainsValuePlaceholder(hrValues)
}

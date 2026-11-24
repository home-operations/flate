package selection

import (
	"context"
	"log/slog"
	"slices"

	"github.com/home-operations/flate/internal/engine/store"
	"github.com/home-operations/flate/pkg/manifest"
)

type declarationIdentity struct {
	occurrence         OccurrenceID
	authored           manifest.NamedResource
	artifact           *store.SourceArtifact
	declarationWitness any
}

func occurrenceIdentity(o *OccurrenceFacts, artifact *store.SourceArtifact) declarationIdentity {
	return declarationIdentity{
		occurrence: o.Declaration, authored: o.Authored, artifact: artifact, declarationWitness: o.Witness,
	}
}

type occurrencePreparation struct {
	key         acquisitionKey
	owner       manifest.NamedResource
	ownerOutput store.Artifact
	child       store.Child
	complete    bool
}

func (c *admissionClosure) matchesOccurrence(id manifest.NamedResource, key *acquisitionKey) bool {
	occurrence, ok := c.occurrence(id)
	return ok && occurrenceIdentity(&occurrence, occurrence.Mounted) == key.declarationIdentity
}

func (c *admissionClosure) prepareOccurrence(
	ctx context.Context,
	owner manifest.NamedResource,
	occurrence *OccurrenceFacts,
	hydrate bool,
) *occurrencePreparation {
	id := occurrence.Identity
	input, source := c.hydrationSource(id, occurrence.Mounted)
	if input == nil {
		return nil
	}
	demand := Demand{Identity: id, Occurrence: *occurrence, Source: source, Artifact: input}
	key := demand.key()
	ownerOutput := c.occurrenceOwnerOutput(id, owner)
	productions := c.productions(id)
	previous := c.proposal.occurrences[id]
	if previous == nil {
		previous = c.proposal.preparation.occurrences[id]
	}
	reason := "first use"
	switch {
	case previous == nil:
	case previous.key != key:
		reason = "occurrence changed"
	case previous.owner != owner || previous.ownerOutput != ownerOutput ||
		!slices.Equal(previous.child.Productions, productions):
		reason = "producer evidence changed"
	case !hydrate || previous.complete:
		c.logOccurrence(ctx, "reused", previous, "matching evidence")
		return previous
	default:
		reason = ""
	}

	result := &occurrencePreparation{key: key, owner: owner, ownerOutput: ownerOutput}
	if acquired := c.proposal.preparation.acquired[key]; acquired.fixedOccurrence() {
		result.child, result.complete = acquired.Child, true
	} else if !hydrate {
		result.child.Obj = c.orderingOccurrence(occurrence, &key)
	}
	if result.child.Obj == nil {
		acquired, known := c.acquire(demand)
		if !known || acquired.Child.Obj == nil {
			return nil
		}
		result.child, result.complete = acquired.Child, true
	}
	result.child.SourceSupport = source
	result.child.Productions = slices.Clone(productions)
	c.retainOccurrence(id, result)
	if reason != "" {
		c.logOccurrence(ctx, "derived", result, reason)
	}
	return result
}

func (c *admissionClosure) occurrenceOwnerOutput(id, owner manifest.NamedResource) store.Artifact {
	if owner == (manifest.NamedResource{}) || owner == id {
		return nil
	}
	return c.objects.GetArtifact(owner, store.ArtifactContent)
}

func (c *admissionClosure) retainOccurrence(id manifest.NamedResource, prepared *occurrencePreparation) {
	if c.proposal.occurrences == nil {
		c.proposal.occurrences = make(map[manifest.NamedResource]*occurrencePreparation)
	}
	c.proposal.occurrences[id] = prepared
}

func (c *admissionClosure) orderingOccurrence(occurrence *OccurrenceFacts, key *acquisitionKey) manifest.BaseManifest {
	prior := c.decision.orderingDeclarations[key.identity]
	if prior.source == key.source && prior.declarationIdentity == key.declarationIdentity {
		return prior.object
	}
	parsed := occurrence.Parsed
	if occurrence.Mounted != key.artifact || parsed == nil || parsed.Named() != occurrence.Authored {
		return nil
	}
	if key.identity.Namespace != occurrence.Authored.Namespace {
		parsed = parsed.WithNamespace(key.identity.Namespace)
	}
	return parsed
}

func (c *admissionClosure) logOccurrence(
	ctx context.Context,
	result string,
	prepared *occurrencePreparation,
	reason string,
) {
	if !slog.Default().Enabled(ctx, slog.LevelDebug) {
		return
	}
	slog.DebugContext(ctx, "selection: occurrence preparation",
		"result", result, "declaration", prepared.key.identity, "owner", prepared.owner, "side", c.context.Side,
		"source_id", prepared.key.source, "generation", prepared.key.artifact.Content,
		"coordinate", prepared.key.occurrence, "reason", reason)
}

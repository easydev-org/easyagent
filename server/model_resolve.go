package server

import (
	"encoding/json"
	"fmt"
	"sort"

	"github.com/easydev-org/easyagent/store"
)

/**
 * resolveModel decides which model a NEW SESSION gets, and it never answers "".
 *
 * WHY THIS EXISTS AT ALL. `CreateSession` used to accept an empty model when the caller passed none and the
 * tenant had no `default_model` configured. The row was written with `model = ''` and the failure surfaced LATER,
 * on the first turn, as `agent: no model configured for session …` — a session that looks fine in the list and
 * cannot answer. That is the worst shape a configuration error can take: it is silent at the moment it is made
 * and shows up as something else entirely when it does.
 *
 * THE CHAIN, in order, and the first answer wins:
 *
 *	1. what the CALLER asked for
 *	2. the tenant's `default_model` config knob
 *	3. the first TEXT model any registered provider advertises
 *
 * STEP 3 IS THE ONE THAT MAKES THIS TOTAL. A tenant with a provider registered has a usable model, and picking it
 * is better than refusing to create a session — the console's "new session" button must work on a deployment
 * where nobody set a default knob. The choice is deterministic (providers sorted by id, models in declared order)
 * so two replicas creating sessions at the same moment agree on the answer.
 *
 * IT REFUSES ONLY WHEN THERE IS NOTHING TO CHOOSE. A tenant with no text provider at all genuinely cannot run a
 * turn, and saying so at creation is the only honest moment: the message names what is missing instead of
 * appearing later as a mysterious turn failure.
 */
func (s *Server) resolveModel(tenant, requested string) (string, error) {
	if requested != "" {
		return requested, nil
	}
	if s.config != nil {
		if v, ok := s.config.Get(tenant, "default_model"); ok && v != "" {
			return v, nil
		}
	}
	if m := s.firstTextModel(tenant); m != "" {
		return m, nil
	}
	return "", fmt.Errorf(
		"no model available: this tenant has no default_model configured and no provider serving a text model; "+
			"register a text provider, or set the default_model config for tenant %q", tenant)
}

/**
 * firstTextModel returns a `provider/model` reference for the first text model registered for the tenant.
 *
 * EMPTY MEANS "THERE IS NONE", and the caller turns that into a refusal — this function does not invent a
 * placeholder name, because a session pointing at a model that does not exist fails exactly like one with no
 * model, only later and with a worse message.
 *
 * SORTED, so the answer is stable. `ListProviders` returns a map's values, whose order changes between calls; a
 * new session created on replica A and one created on replica B would otherwise get different models from the
 * same deployment.
 */
func (s *Server) firstTextModel(tenant string) string {
	providers, err := s.db.ListProviders(tenant)
	if err != nil {
		return ""
	}
	sort.Slice(providers, func(i, j int) bool { return providers[i].ProviderID < providers[j].ProviderID })
	for _, p := range providers {
		// ONLY A TEXT PROVIDER CAN SERVE A TURN. Image, speech, ASR and rerank providers advertise models too,
		// and picking one would produce a session that cannot answer — the same failure this function exists to
		// prevent, arrived at from the other side.
		if p.Capability != "" && p.Capability != "text" {
			continue
		}
		for _, m := range decodeModelRows(p.Models) {
			id, _ := m["id"].(string)
			if id == "" {
				continue
			}
			// Model rows carry a type too, and a text provider may still list a non-text model.
			if t, _ := m["model_type"].(string); t != "" && t != "text" {
				continue
			}
			return p.ProviderID + "/" + id
		}
	}
	return ""
}

// decodeModelRows reads a provider's model JSON, tolerating the shapes rows are stored in.
//
// A MALFORMED COLUMN YIELDS NO MODELS rather than an error: this is a fallback path, and a provider whose JSON
// cannot be read should not turn "create a session" into a 500 — the next provider in the list may be fine.
func decodeModelRows(raw string) []map[string]any {
	if raw == "" {
		return nil
	}
	var rows []map[string]any
	if err := json.Unmarshal([]byte(raw), &rows); err != nil {
		return nil
	}
	return rows
}

// ensureTextModelIsNonEmpty is the store-side half of the same invariant.
//
// THE SERVER RESOLVES A MODEL; THE STORE REFUSES TO WRITE A ROW WITHOUT ONE. Two layers, because the store is the
// choke point every creator goes through — a fork, a migration or a future RPC — and an invariant that only one
// caller enforces is a convention, not an invariant.
func ensureModelNonEmpty(model string) error {
	if model == "" {
		return fmt.Errorf("a session requires a model; refusing to store an empty one")
	}
	return nil
}

// storeModelProvider is the interface the model fallback needs from the store, kept narrow so the resolver can be
// exercised without a database.
type storeModelProvider interface {
	ListProviders(tenant string) ([]store.Provider, error)
}

package agent

// Reasoning variants: a selectable reasoning configuration for a model.
//
// A session carries the selected variant id; at turn time it is merged into the request body as the
// `reasoning` field. No variant selected ⇒ nothing is sent, and that IS "default": it is the absence of a
// choice, not a fourth level, which is why it is not in this list.
//
// THREE LEVELS, NOT SEVEN. The wire format accepts more (none/minimal/xhigh/max), but a picker offering them
// asks the user to choose between settings whose difference no one can predict and whose effect they cannot
// check — and every extra row makes the four that matter harder to find. The three below span the range a
// person actually reasons about: cheap, balanced, thorough.
var ReasoningLevels = []string{"low", "medium", "high"}

// ModelVariant is a selectable variant as surfaced over the API.
type ModelVariant struct {
	ID          string
	Name        string
	Description string
}

// VariantDef is a variant plus the request-body fragment that applies it.
type VariantDef struct {
	ModelVariant
	// Body is merged verbatim into the model request (e.g. {"reasoning":"high"}).
	Body map[string]any
}

// VariantsFor returns the variants a model offers. A model that does not
// support reasoning offers none (the client shows no picker).
func VariantsFor(supportsReasoning bool) []ModelVariant {
	if !supportsReasoning {
		return nil
	}
	out := make([]ModelVariant, 0, len(ReasoningLevels))
	for _, lvl := range ReasoningLevels {
		out = append(out, ModelVariant{ID: lvl, Name: lvl, Description: "reasoning effort: " + lvl})
	}
	return out
}

// ResolveVariant maps a variant id to its request-body fragment. Returns false
// for an unknown/empty id (the caller then sends no variant).
func ResolveVariant(id string) (map[string]any, bool) {
	if id == "" {
		return nil, false
	}
	for _, lvl := range ReasoningLevels {
		if lvl == id {
			return map[string]any{"reasoning": id}, true
		}
	}
	return nil, false
}

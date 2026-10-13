package agent

// LocalizeSchema returns a copy of a JSON-schema object with per-node
// `descriptions` maps resolved into a single localized `description` (and the
// `descriptions` key dropped), mirroring the TS agent's `localizeSchema`. The
// fallback description is kept when no localized value exists.
func LocalizeSchema(schema map[string]any, locale string) map[string]any {
	if schema == nil {
		return nil
	}
	return localizeNode(schema, locale).(map[string]any)
}

func localizeNode(node any, locale string) any {
	switch v := node.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for k, val := range v {
			if k == "descriptions" {
				continue // dropped: folded into `description`
			}
			out[k] = localizeNode(val, locale)
		}
		if descs, ok := v["descriptions"].(map[string]any); ok {
			if s, ok := descs[locale].(string); ok && s != "" {
				out["description"] = s
			}
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, e := range v {
			out[i] = localizeNode(e, locale)
		}
		return out
	default:
		return node
	}
}

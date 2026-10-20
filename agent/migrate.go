package agent

import (
	"log"
	"strings"
)

// MigrateConfigNamespaces moves values out of the OLD flat namespace into the
// per-extension one.
//
// Before this change a knob lived at `cfg.<tenant>.<name>` — one global space.
// Every stored value is therefore ambiguous: a `sandboxes` there might have been
// set from the git page or the sandbox page, and they were the same cell.
//
// The move is a COPY, not a move: a name declared by several extensions is
// written to EACH of them. That is the only choice that keeps every extension
// working — picking one would silently empty the others, which is worse than a
// duplicated value the user can then diverge.
//
// Idempotent: the legacy key is deleted once copied, so a second run finds
// nothing to do. Keys that no extension declares are left alone (they are
// tenant-wide tier: locale, default_model, ...).
func (c *Config) MigrateConfigNamespaces(declared map[string][]string) (moved int) {
	// declared maps knob name -> extension ids that declare it.
	if len(declared) == 0 {
		return 0
	}
	keys := c.kv.Keys("cfg.")
	for _, k := range keys {
		rest := strings.TrimPrefix(k, "cfg.")
		tenant, name, ok := splitLegacy(rest)
		if !ok {
			continue // already namespaced (cfg.<t>.<ext>.<...>)
		}
		exts := declared[name]
		if len(exts) == 0 {
			continue // tenant-wide knob: not an extension's
		}
		v, ok := c.kv.Get(k)
		if !ok {
			continue
		}
		for _, extID := range exts {
			// Do not clobber a value that already lives in the new address (a
			// later user edit wins over a migration).
			if _, exists := c.GetExt(tenant, extID, c.knobNamespace(extID, name), name); exists {
				continue
			}
			c.SetExt(tenant, extID, c.knobNamespace(extID, name), name, v)
		}
		c.kv.Delete(k)
		moved++
	}
	if moved > 0 {
		log.Printf("config: migrated %d legacy value(s) into per-extension namespaces", moved)
	}
	return moved
}

// splitLegacy reports (tenant, knob) when key is the OLD `cfg.<tenant>.<knob>`
// form. A namespaced key has more segments and returns ok=false.
//
// A tenant id may not contain a dot, so exactly two segments is unambiguous.
func splitLegacy(rest string) (tenant, name string, ok bool) {
	i := strings.IndexByte(rest, '.')
	if i <= 0 || i == len(rest)-1 {
		return "", "", false
	}
	tenant, name = rest[:i], rest[i+1:]
	if strings.Contains(name, ".") {
		return "", "", false
	}
	return tenant, name, true
}

package bundled

import "testing"

/**
 * A BUNDLED TOOL MUST SEE THE VALUE THE CONSOLE SAVED.
 *
 * The two are stored in DIFFERENT KEYSPACES. `SetExtensionConfig` writes through `config.SetExt`,
 * whose key carries the extension id; `ConfigFn` reads the tenant-wide keyspace. A resolver that only
 * had `ConfigFn` therefore never found a knob the user had set for this extension — the console showed
 * it as set and the code read "".
 *
 * FOUND BY PINNING A PROVIDER IN THE UI AND WATCHING THE OTHER ONE RUN. The value was written, the
 * config page showed it, and the tool behaved as if it were empty.
 */
func TestResolveConfigReadsTheExtensionScopedValue(t *testing.T) {
	d := &StoreDeps{
		// The tenant-wide keyspace deliberately holds something DIFFERENT, because a resolver that
		// checked it first would find it and return the wrong value.
		ConfigFn: func(_, _ string) (string, bool) { return "tavily", true },
		ExtConfigFn: func(_, name, _ string) (string, bool) {
			if name == "search_provider" {
				return "tinyfish", true
			}
			return "", false
		},
	}
	if got := d.ResolveConfig("search_provider", "", "t1"); got != "tinyfish" {
		t.Fatalf("resolved %q; the extension-scoped value must win over the tenant-wide one", got)
	}
}

// An unset extension knob falls through to the tenant-wide keyspace rather than returning nothing.
func TestResolveConfigFallsBackToTenantWide(t *testing.T) {
	d := &StoreDeps{
		ConfigFn:    func(_, _ string) (string, bool) { return "fallback", true },
		ExtConfigFn: func(_, _, _ string) (string, bool) { return "", false },
	}
	if got := d.ResolveConfig("anything", "", "t1"); got != "fallback" {
		t.Fatalf("resolved %q, want the tenant-wide fallback", got)
	}
}

// With no extension reader wired, the resolver still works — a deployment that has not adopted it must
// not lose its tenant-wide knobs.
func TestResolveConfigWorksWithoutTheExtensionReader(t *testing.T) {
	d := &StoreDeps{ConfigFn: func(_, _ string) (string, bool) { return "only-tenant", true }}
	if got := d.ResolveConfig("k", "", "t1"); got != "only-tenant" {
		t.Fatalf("resolved %q", got)
	}
}

package server

import (
	"context"
	"testing"

	"github.com/easydev-org/easyagent/agent"
	"github.com/easydev-org/easyagent/bundled"
	agentv1 "github.com/easydev-org/easyagent/gen/agent/v1"
	"google.golang.org/protobuf/types/known/structpb"
)

/**
 * A BUNDLED TOOL'S CONFIG MUST COME BACK, JUST LIKE AN EXTENSION'S.
 *
 * `GetToolConfig` used to walk only the DISCOVERED extension tools. Bundled tools are in-process, so
 * every one of them reported "no config" — the console showed their required knobs as unset, and the
 * preset editor treated the tool as unable to run, for a tool that worked fine.
 *
 * The set this RPC answers for has to match the set `ListTools` advertises, which is why both now read
 * `allToolDefs`.
 */
func TestGetToolConfigIncludesBundledTools(t *testing.T) {
	ts, db, _ := newTestServerWith(t, func(d *Deps) {
		d.BundledTools = fixedTools{defs: []agent.ToolDef{{
			ExtID:          "bundled",
			Name:           "web-search",
			RequiredConfig: []string{"search_provider"},
		}}}
		d.BundledFields = stubFields{fields: []bundled.ConfigField{
			{Name: "search_provider", Type: "string", Kind: bundled.ConfigKindValue},
		}}
	})
	tok := seedTenant(t, db, "acme")
	c := clientFor(t, ts, tok)
	ctx := context.Background()

	// While unset, the tool is absent from the map — which is exactly what the console reads as
	// "required config unmet". It must be absent, not present-and-empty: the map's absence is the
	// signal, and a `{search_provider: ""}` entry would look like a knob that had been cleared.
	res, err := c.GetToolConfig(ctx, &agentv1.GetToolConfigRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(res.Config.Values) != 0 {
		t.Fatalf("values before setting = %v, want empty", res.Config.Values)
	}

	// Set it the way the settings page does, then read it back THROUGH THE TOOL-CONFIG RPC.
	if _, err := c.SetExtensionConfig(ctx, &agentv1.SetExtensionConfigRequest{
		ExtId: "bundled", Name: "search_provider", Value: structpb.NewStringValue("tavily"),
	}); err != nil {
		t.Fatal(err)
	}
	res, err = c.GetToolConfig(ctx, &agentv1.GetToolConfigRequest{})
	if err != nil {
		t.Fatal(err)
	}
	got := res.Config.Values["web-search"]
	if got == nil {
		t.Fatalf("bundled tool missing from tool config: %v", res.Config.Values)
	}
	if v := got.GetStructValue().Fields["search_provider"]; v == nil || v.GetStringValue() != "tavily" {
		t.Errorf("search_provider = %v, want tavily", v)
	}
}

// fixedTools is a ToolSource that returns a fixed list.
type fixedTools struct{ defs []agent.ToolDef }

func (f fixedTools) Tools() []agent.ToolDef { return f.defs }

package bundled

import (
	_ "embed"
	"fmt"

	"gopkg.in/yaml.v3"

	"github.com/easydev-org/easyagent/agent"
)

// manifestYAML is the SAME manifest.yaml as the TS extension (source of truth
// for tool schemas + zh/en descriptions). Copied verbatim so the tool set and
// i18n stay in lockstep with the TS extension.
//
//go:embed manifest.yaml
var manifestYAML []byte

type manifestTool struct {
	Name           string            `yaml:"name"`
	Description    string            `yaml:"description"`
	Descriptions   map[string]string `yaml:"descriptions"`
	InputSchema    map[string]any    `yaml:"input_schema"`
	RequiredConfig []string          `yaml:"required_config"`
	/**
	 * Permission keys the tool needs before it may run.
	 *
	 * IT WAS NOT PARSED BEFORE, and that mattered once a tool started requiring one: a YAML key the
	 * struct does not know is silently dropped, so `requires: [group.create]` would have declared
	 * nothing and the tool would have been callable by anyone. The requirement has to survive this
	 * layer or it does not exist.
	 */
	Requires []string `yaml:"requires"`
	/**
	 * RequiresScope names the argument that says WHICH object a requirement is scoped to (see agent.ToolDef).
	 *
	 * OMITTED MEANS UNSCOPED, which is the right default: a capability that is not about a particular object should
	 * not acquire an argument-shaped hole in its check just because the field exists.
	 */
	RequiresScope string `yaml:"requires_scope"`
	// Effect is no | low | mid | high (see agent.Effect). REQUIRED: leaving it out would let a tool
	// default to something nobody chose, and a `high` tool declared as `no` is a risk the user is never
	// told about.
	Effect string `yaml:"effect"`
}

// manifestPermission is one declared permission.
type manifestPermission struct {
	Key          string            `yaml:"key"`
	Label        string            `yaml:"label"`
	Descriptions map[string]string `yaml:"descriptions"`
	Risk         string            `yaml:"risk"`
}

type manifestConfig struct {
	Name         string            `yaml:"name"`
	Description  string            `yaml:"description"`
	Descriptions map[string]string `yaml:"descriptions"`
	Type         string            `yaml:"type"`
	Kind         string            `yaml:"kind"`
	Capability   string            `yaml:"capability"`
	Scope        string            `yaml:"scope"`
	Default      any               `yaml:"default"`
	EnumValues   []string          `yaml:"enum_values"`
}

type manifestDoc struct {
	ID          string               `yaml:"id"`
	Version     string               `yaml:"version"`
	Tools       []manifestTool       `yaml:"tools"`
	Config      []manifestConfig     `yaml:"config"`
	Permissions []manifestPermission `yaml:"permissions"`
}

// Manifest is the parsed bundled-extension manifest.
type Manifest struct {
	ID     string
	Tools  []agent.ToolDef
	Config []ConfigKnob
	// Permissions are the grants this extension can require. A tool's `requires` naming a key that is
	// not declared here is unusable: the console has nothing to offer the user, so nobody can grant it.
	Permissions []PermissionDecl
}

// ConfigKnob is one declared config knob.
type ConfigKnob struct {
	Name         string
	Description  string
	Descriptions map[string]string
	Type         string
	Kind         string
	Capability   string
	Default      string
	// EnumValues are the allowed choices when Type is "enum". They decide whether a client renders
	// a picker or a text box, so losing them here turns a declared set of choices into free text.
	EnumValues []string
}

// ParseManifest parses the embedded manifest (equivalent to the TS
// parseManifest + manifestConfig).
func ParseManifest() (Manifest, error) {
	return parseManifestBytes(manifestYAML)
}

/**
 * parseManifestBytes is the parser WITHOUT the embedded file, so a test can prove that a malformed
 * declaration is REJECTED rather than silently defaulted.
 *
 * The embedded manifest is the one that ships, and a test that can only feed it the good copy can only
 * assert that the good copy is good. The rules that matter here — a missing effect, an unknown effect, a
 * risk that understates its tool — are exactly the ones that must fail on bad input.
 */
func parseManifestBytes(data []byte) (Manifest, error) {
	var doc manifestDoc
	if err := yaml.Unmarshal(data, &doc); err != nil {
		return Manifest{}, fmt.Errorf("bundled: manifest: %w", err)
	}
	m := Manifest{ID: doc.ID}
	for _, t := range doc.Tools {
		// EFFECT IS REQUIRED AND VALIDATED HERE, at parse time, because a wrong or missing one is
		// UNDETECTABLE later: no test can tell that a destructive tool was declared harmless. The
		// failure has to be at the moment the declaration is read.
		eff := agent.Effect(t.Effect)
		if !eff.Valid() {
			return Manifest{}, fmt.Errorf(
				"bundled: manifest: tool %q declares effect %q; want one of none|low|medium|high",
				t.Name, t.Effect)
		}
		m.Tools = append(m.Tools, agent.ToolDef{
			Name:           t.Name,
			ExtID:          doc.ID,
			Description:    t.Description,
			Descriptions:   t.Descriptions,
			InputSchema:    t.InputSchema,
			RequiredConfig: t.RequiredConfig,
			Requires:       t.Requires,
			RequiresScope:  t.RequiresScope,
			Effect:         eff,
		})
	}
	for _, p := range doc.Permissions {
		m.Permissions = append(m.Permissions, PermissionDecl{
			Key: p.Key, Label: p.Label, Descriptions: p.Descriptions, Risk: p.Risk,
		})
	}
	/**
	 * A PERMISSION'S `risk` MAY NOT UNDERSTATE THE EFFECT OF A TOOL IT GATES.
	 *
	 * The two are different axes — one is "how dangerous is granting this", the other is "what does the
	 * call do" — but they cannot contradict each other. A `high`-impact tool behind a permission labelled
	 * `read` is a prompt to approve something while being told it is harmless.
	 */
	/**
	 * THE TWO SCALES MUST BE COMPARABLE, and they are not the same length.
	 *
	 * `risk` has THREE values and `effect` has FOUR, so ranking them 0,1,2 against 0,1,2,3 made
	 * `destructive` unable to cover `high` — the strictest permission looked weaker than the worst tool.
	 * Each risk level is therefore mapped to the effect it is allowed to cover at minimum:
	 *
	 *	risk=read         covers effect=none
	 *	risk=write        covers effect=low
	 *	risk=destructive  covers effect=medium AND high
	 *
	 * `destructive` is the top of its scale, so it covers both of the top effects. There is no risk level
	 * that means "medium only" — and inventing one would be a fourth permission tier nothing else uses.
	 */
	riskRank := map[string]int{"read": 0, "write": 1, "destructive": 3}
	for _, t := range m.Tools {
		if len(t.Requires) == 0 {
			continue
		}
		for _, p := range m.Permissions {
			found := false
			for _, need := range t.Requires {
				if need == p.Key {
					found = true
				}
			}
			if !found {
				continue
			}
			if riskRank[p.Risk] < t.Effect.Rank() {
				return Manifest{}, fmt.Errorf(
					"bundled: manifest: permission %q is risk=%q but gates tool %q with effect=%q; "+
						"a grant must not look safer than what it unlocks",
					p.Key, p.Risk, t.Name, t.Effect)
			}
		}
	}
	for _, c := range doc.Config {
		def := ""
		if c.Default != nil {
			def = fmt.Sprintf("%v", c.Default)
		}
		m.Config = append(m.Config, ConfigKnob{
			Name: c.Name, Description: c.Description, Descriptions: c.Descriptions,
			Type: c.Type, Kind: c.Kind, Capability: c.Capability, Default: def,
			EnumValues: c.EnumValues,
		})
	}
	return m, nil
}

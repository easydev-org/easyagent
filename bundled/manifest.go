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
	ID      string           `yaml:"id"`
	Version string           `yaml:"version"`
	Tools   []manifestTool   `yaml:"tools"`
	Config  []manifestConfig `yaml:"config"`
}

// Manifest is the parsed bundled-extension manifest.
type Manifest struct {
	ID     string
	Tools  []agent.ToolDef
	Config []ConfigKnob
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
	var doc manifestDoc
	if err := yaml.Unmarshal(manifestYAML, &doc); err != nil {
		return Manifest{}, fmt.Errorf("bundled: manifest: %w", err)
	}
	m := Manifest{ID: doc.ID}
	for _, t := range doc.Tools {
		m.Tools = append(m.Tools, agent.ToolDef{
			Name:           t.Name,
			ExtID:          doc.ID,
			Description:    t.Description,
			Descriptions:   t.Descriptions,
			InputSchema:    t.InputSchema,
			RequiredConfig: t.RequiredConfig,
		})
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

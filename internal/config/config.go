// Package config is easyagent's single, file-based configuration surface.
//
// Everything the process needs is expressed in ONE YAML file (see
// config.example.yaml), loaded via --config. There are no environment
// variables in the configuration path: the only exception is that secrets may
// be pointed at a file (the k8s-Secret pattern) with the `*_file` keys, and
// the replica identity defaults to os.Hostname() rather than an injected env
// var.
package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// Config is the whole process configuration.
type Config struct {
	Server   Server   `yaml:"server"`
	Auth     Auth     `yaml:"auth"`
	Tenant   Tenant   `yaml:"tenant"`
	Database Database `yaml:"database"`
	NATS     NATS     `yaml:"nats"`
	Agent    Agent    `yaml:"agent"`
	Blob     Blob     `yaml:"blob"`
	Seed     Seed     `yaml:"seed"`
	Log      Log      `yaml:"log"`
}

// Server is the RPC + health listener.
type Server struct {
	Addr         string `yaml:"addr"`
	HealthAddr   string `yaml:"health_addr"`
	CORSOrigin   string `yaml:"cors_origin"`
	MaxBodyBytes int    `yaml:"max_body_bytes"`
}

// Auth configures the request authentication.
type Auth struct {
	// Mode is "required" (bearer token) or "none" (no auth; dev only).
	Mode string `yaml:"mode"`
	// AdminToken guards AdminService. Plaintext, or use AdminTokenFile.
	AdminToken string `yaml:"admin_token"`
	// AdminTokenFile reads the token from a file (k8s Secret); wins over
	// AdminToken when set.
	AdminTokenFile string `yaml:"admin_token_file"`
	// AdminUsername is the platform admin's login name (paired with
	// admin_token). Default "admin".
	AdminUsername string `yaml:"admin_username"`
}

// Tenant holds tenant defaults.
type Tenant struct {
	Default string `yaml:"default"`
}

// Database selects the persistence backend.
type Database struct {
	// Backend is "sqlite", "postgres" (or "pg"), or "mysql".
	Backend string `yaml:"backend"`
	// DSN is the connection string / sqlite path.
	DSN string `yaml:"dsn"`
}

// NATS configures the transport. An empty URL runs in degraded mode
// (in-memory KV + the in-process bundled tools only).
type NATS struct {
	URL       string `yaml:"url"`
	ServerBin string `yaml:"server_bin"`
	// ReplicaID identifies this instance among replicas. Empty = os.Hostname().
	ReplicaID string `yaml:"replica_id"`
}

// Agent tunes the turn loop.
type Agent struct {
	DefaultMaxTurns     int           `yaml:"default_max_turns"`
	ToolTimeout         time.Duration `yaml:"tool_timeout"`
	CompactAtFraction   float64       `yaml:"compact_at_fraction"`
	ForceCompactTokens  int           `yaml:"force_compact_tokens"`
	ResumeWatchInterval time.Duration `yaml:"resume_watch_interval"`
	// DisabledTools is the host hard-denylist of qualified `<extId>.<name>` ids.
	DisabledTools []string `yaml:"disabled_tools"`
}

// Blob configures durable file-byte storage.
type Blob struct {
	// Backend is "" (in-memory) or "s3".
	Backend string `yaml:"backend"`
	S3      S3     `yaml:"s3"`
}

// S3 is the S3-compatible backend for durable file bytes.
type S3 struct {
	Bucket         string `yaml:"bucket"`
	Region         string `yaml:"region"`
	Endpoint       string `yaml:"endpoint"`
	AccessKey      string `yaml:"access_key"`
	SecretKey      string `yaml:"secret_key"`
	SecretKeyFile  string `yaml:"secret_key_file"`
	ForcePathStyle bool   `yaml:"force_path_style"`
	Prefix         string `yaml:"prefix"`
}

// Seed is create-if-absent boot data (never clobbers a user value).
type Seed struct {
	// SystemPresetsFile points at a JSON file of immutable system presets.
	SystemPresetsFile string `yaml:"system_presets_file"`
	// Providers are seeded only into tenants that have NONE yet.
	Providers []SeedProvider `yaml:"providers"`
	// Config are tenant config rows, create-if-absent.
	Config []SeedConfigRow `yaml:"config"`
	// ExtensionConfig are extension config rows, create-if-absent.
	ExtensionConfig []SeedExtConfigRow `yaml:"extension_config"`
	// ExtensionAllowlist restricts which extension servers a delegation subtree
	// may use: scope_id -> allowed extIds. A tenant's subtree root picks its
	// entry; a scope with NO entry is unrestricted. Enforced at turn time (the
	// model is never offered tools from a non-allowed extension).
	ExtensionAllowlist map[string][]string `yaml:"extension_allowlist"`
}

// SeedProvider mirrors one AGENT_PROVIDER_SEED entry.
type SeedProvider struct {
	Tenant     string              `yaml:"tenant"`
	ProviderID string              `yaml:"provider_id"`
	Capability string              `yaml:"capability"`
	APIType    string              `yaml:"api_type"`
	BaseURL    string              `yaml:"base_url"`
	APIKey     string              `yaml:"api_key"`
	Models     []SeedProviderModel `yaml:"models"`
}

// SeedProviderModel is one model under a seeded provider.
type SeedProviderModel struct {
	ID           string `yaml:"id"`
	Name         string `yaml:"name"`
	ContextLimit int64  `yaml:"context_limit"`
	ModelType    string `yaml:"model_type"`
}

// SeedConfigRow is one tenant config seed.
type SeedConfigRow struct {
	Tenant string `yaml:"tenant"`
	Key    string `yaml:"key"`
	Value  string `yaml:"value"`
}

// SeedExtConfigRow is one extension config seed.
type SeedExtConfigRow struct {
	Tenant string `yaml:"tenant"`
	ExtID  string `yaml:"ext_id"`
	Name   string `yaml:"name"`
	Value  string `yaml:"value"`
}

// Log configures logging.
type Log struct {
	Level  string `yaml:"level"`
	Format string `yaml:"format"`
}

// Default returns the built-in defaults (every field, so a partial YAML file
// only overrides what it sets).
func Default() Config {
	return Config{
		Server: Server{
			Addr:         ":8080",
			HealthAddr:   ":8081",
			CORSOrigin:   "*",
			MaxBodyBytes: 64 << 20,
		},
		Auth:   Auth{Mode: "required", AdminUsername: "admin"},
		Tenant: Tenant{Default: "default"},
		Database: Database{
			Backend: "sqlite",
			DSN:     "/data/agent.db",
		},
		Agent: Agent{
			DefaultMaxTurns:    8,
			ToolTimeout:        600 * time.Second,
			CompactAtFraction:  0.9,
			ForceCompactTokens: 900_000,
		},
		Log: Log{Level: "info", Format: "text"},
	}
}

// Load reads path (a YAML file) over the built-in defaults, then normalizes
// and validates. An empty path returns the defaults.
func Load(path string) (Config, error) {
	c := Default()
	if path != "" {
		raw, err := os.ReadFile(path)
		if err != nil {
			return Config{}, fmt.Errorf("read config %q: %w", path, err)
		}
		dec := yaml.NewDecoder(strings.NewReader(string(raw)))
		dec.KnownFields(true) // reject typos: an unknown key is a config error
		if err := dec.Decode(&c); err != nil {
			return Config{}, fmt.Errorf("parse config %q: %w", path, err)
		}
	}
	if err := c.resolveSecrets(); err != nil {
		return Config{}, err
	}
	c.normalize()
	if err := c.Validate(); err != nil {
		return Config{}, err
	}
	return c, nil
}

// resolveSecrets loads `*_file` secrets into their plaintext fields.
func (c *Config) resolveSecrets() error {
	if c.Auth.AdminTokenFile != "" {
		v, err := readSecret(c.Auth.AdminTokenFile)
		if err != nil {
			return err
		}
		c.Auth.AdminToken = v
	}
	if c.Blob.S3.SecretKeyFile != "" {
		v, err := readSecret(c.Blob.S3.SecretKeyFile)
		if err != nil {
			return err
		}
		c.Blob.S3.SecretKey = v
	}
	return nil
}

func readSecret(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret %q: %w", path, err)
	}
	return strings.TrimSpace(string(raw)), nil
}

// normalize applies the small coercions the old env parser did.
func (c *Config) normalize() {
	if c.Server.Addr != "" && !strings.HasPrefix(c.Server.Addr, ":") &&
		!strings.Contains(c.Server.Addr, ":") {
		c.Server.Addr = ":" + c.Server.Addr
	}
	if c.Database.Backend == "pg" {
		c.Database.Backend = "postgres"
	}
	if c.Database.Backend == "" {
		c.Database.Backend = "sqlite"
	}
	if c.Auth.Mode == "" {
		c.Auth.Mode = "required"
	}
	if c.Auth.AdminUsername == "" {
		c.Auth.AdminUsername = "admin"
	}
	if c.Tenant.Default == "" {
		c.Tenant.Default = "default"
	}
	if c.Agent.CompactAtFraction <= 0 {
		c.Agent.CompactAtFraction = 0.9
	}
	if c.Agent.CompactAtFraction > 1 {
		c.Agent.CompactAtFraction = 1
	}
}

// Validate rejects an unusable configuration (fail fast at startup).
func (c Config) Validate() error {
	switch c.Database.Backend {
	case "sqlite", "postgres", "mysql":
	default:
		return fmt.Errorf("database.backend %q: want sqlite|postgres|mysql", c.Database.Backend)
	}
	if c.Database.DSN == "" {
		return fmt.Errorf("database.dsn is required")
	}
	if c.Database.Backend != "sqlite" && c.Database.DSN == "/data/agent.db" {
		return fmt.Errorf("database.dsn must be set for backend %q", c.Database.Backend)
	}
	switch c.Auth.Mode {
	case "required", "none":
	default:
		return fmt.Errorf("auth.mode %q: want required|none", c.Auth.Mode)
	}
	if c.Auth.Mode == "required" && c.Auth.AdminToken == "" {
		// Not fatal for the tenant RPC surface, but warn-worthy; the AdminService
		// simply stays locked. Kept non-fatal to preserve current behavior.
	}
	if c.Blob.Backend != "" && c.Blob.Backend != "s3" {
		return fmt.Errorf("blob.backend %q: want \"\" or s3", c.Blob.Backend)
	}
	if c.Blob.Backend == "s3" && c.Blob.S3.Bucket == "" {
		return fmt.Errorf("blob.s3.bucket is required when blob.backend is s3")
	}
	return nil
}

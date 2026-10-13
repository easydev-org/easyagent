package config

import (
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func write(t *testing.T, name, body string) string {
	t.Helper()
	p := filepath.Join(t.TempDir(), name)
	if err := os.WriteFile(p, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return p
}

// A partial file overrides only what it sets; the rest is the built-in default.
func TestLoadPartialOverridesDefaults(t *testing.T) {
	p := write(t, "c.yaml", `
server:
  addr: "127.0.0.1:9090"
agent:
  default_max_turns: 3
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != "127.0.0.1:9090" {
		t.Errorf("addr = %q", c.Server.Addr)
	}
	if c.Agent.DefaultMaxTurns != 3 {
		t.Errorf("max_turns = %d", c.Agent.DefaultMaxTurns)
	}
	// untouched fields keep their defaults
	if c.Server.HealthAddr != ":8081" {
		t.Errorf("health_addr default lost: %q", c.Server.HealthAddr)
	}
	if c.Agent.ToolTimeout != 600*time.Second {
		t.Errorf("tool_timeout default lost: %v", c.Agent.ToolTimeout)
	}
	if c.Database.Backend != "sqlite" || c.Database.DSN != "/data/agent.db" {
		t.Errorf("db defaults lost: %+v", c.Database)
	}
}

// An empty path yields the pure defaults.
func TestLoadNoFile(t *testing.T) {
	c, err := Load("")
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != ":8080" || c.Auth.Mode != "required" {
		t.Fatalf("defaults wrong: %+v", c)
	}
}

// Durations accept Go's "600s"/"10m" syntax.
func TestDurationParsing(t *testing.T) {
	p := write(t, "c.yaml", `
agent:
  tool_timeout: 90s
  idlewatch_interval: 5m
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Agent.ToolTimeout != 90*time.Second {
		t.Errorf("tool_timeout = %v", c.Agent.ToolTimeout)
	}
	if c.Agent.IdleWatchInterval != 5*time.Minute {
		t.Errorf("idlewatch = %v", c.Agent.IdleWatchInterval)
	}
}

// An unknown key is a hard error (catches typos, not a silent no-op).
func TestUnknownKeyRejected(t *testing.T) {
	p := write(t, "c.yaml", "server:\n  adr: \":1\"\n") // typo: adr
	if _, err := Load(p); err == nil {
		t.Fatal("an unknown key should be rejected")
	}
}

// Secrets may be read from a file (the k8s Secret pattern).
func TestSecretFile(t *testing.T) {
	tok := write(t, "token", "s3cr3t\n")
	p := write(t, "c.yaml", `
auth:
  mode: required
  admin_token_file: `+tok+`
`)
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Auth.AdminToken != "s3cr3t" {
		t.Errorf("admin_token = %q, want trimmed file contents", c.Auth.AdminToken)
	}
}

// compact_at_fraction is clamped to (0, 1].
func TestFractionClamped(t *testing.T) {
	for _, tc := range []struct {
		in   float64
		want float64
	}{{0, 0.9}, {-1, 0.9}, {1.5, 1}, {0.5, 0.5}} {
		p := write(t, "c.yaml", "agent:\n  compact_at_fraction: "+ftoa(tc.in)+"\n")
		c, err := Load(p)
		if err != nil {
			t.Fatal(err)
		}
		if c.Agent.CompactAtFraction != tc.want {
			t.Errorf("fraction %v -> %v, want %v", tc.in, c.Agent.CompactAtFraction, tc.want)
		}
	}
}

// Validation fails fast on an unusable config.
func TestValidate(t *testing.T) {
	bad := []string{
		"database:\n  backend: oracle\n",   // unknown backend
		"database:\n  backend: postgres\n", // non-sqlite with the default dsn
		"auth:\n  mode: sometimes\n",       // unknown auth mode
		"blob:\n  backend: gcs\n",          // unknown blob backend
		"blob:\n  backend: s3\n",           // s3 without a bucket
	}
	for _, body := range bad {
		p := write(t, "c.yaml", body)
		if _, err := Load(p); err == nil {
			t.Errorf("config should be rejected:\n%s", body)
		}
	}
	// A valid postgres config with an explicit DSN passes.
	ok := write(t, "c.yaml", "database:\n  backend: postgres\n  dsn: host=db dbname=a\n")
	if _, err := Load(ok); err != nil {
		t.Errorf("valid postgres config rejected: %v", err)
	}
}

// normalize maps "pg" -> "postgres" and prefixes a bare addr.
func TestNormalize(t *testing.T) {
	p := write(t, "c.yaml", "server:\n  addr: \"9090\"\ndatabase:\n  backend: pg\n  dsn: x\n")
	c, err := Load(p)
	if err != nil {
		t.Fatal(err)
	}
	if c.Server.Addr != ":9090" {
		t.Errorf("addr = %q, want :9090", c.Server.Addr)
	}
	if c.Database.Backend != "postgres" {
		t.Errorf("backend = %q, want postgres", c.Database.Backend)
	}
}

func ftoa(f float64) string {
	return strconv.FormatFloat(f, 'g', -1, 64)
}

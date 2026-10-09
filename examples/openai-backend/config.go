package main

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Config is the whole configuration of the server, resolved once at startup.
//
// Every field has a default, so the server runs with no environment at all, and
// every override is validated, so a typo is a startup failure rather than a
// silently-ignored setting. The second property is the one that matters: a
// configuration that is quietly ignored is worse than one that is loudly
// rejected, because the operator believes something is in effect that is not.
type Config struct {
	// Addr is the listen address.
	Addr string

	// Tenants maps a bearer token to a tenant. Tokens never appear in logs.
	Tenants map[string]*Tenant

	// MaxStreams bounds concurrent streams globally. This is photon's derived
	// default when zero, which is derived from the host's detected memory budget
	// rather than from a constant.
	MaxStreams int

	// MaxStreamBufferBytes is the per-stream buffer cap. Every concurrent stream
	// holds one, so the product is what bounds memory.
	MaxStreamBufferBytes int

	// StallTimeout is how long a full buffer blocks before the overflow policy
	// applies. Too short and a client on a slow network is disconnected
	// mid-answer; too long and a stalled client holds a producer for that long.
	StallTimeout time.Duration

	// PerTenantStreams bounds concurrent streams for one tenant. This is a
	// fairness control: photon bounds the total, and this stops one tenant from
	// consuming all of it.
	PerTenantStreams int

	// PerTenantRPM bounds requests per minute for one tenant.
	PerTenantRPM int

	// UpstreamURL is the provider endpoint the token proxying adapter calls.
	UpstreamURL string

	// UpstreamAPIKey authenticates to the provider. Never logged.
	UpstreamAPIKey string

	// ModelName is the single model this server advertises.
	ModelName string
}

// DefaultConfig returns a configuration that runs with no environment set.
//
// The values are chosen for a small deployment that is safe by default rather
// than permissive. A server that needs configuration to be secure is a server
// that will be deployed insecurely.
func DefaultConfig() Config {
	return Config{
		Addr:                 ":8080",
		Tenants:              map[string]*Tenant{},
		MaxStreams:           0, // derived from the host's memory budget
		MaxStreamBufferBytes: 64 << 10,
		StallTimeout:         30 * time.Second,
		PerTenantStreams:     8,
		PerTenantRPM:         60,
		UpstreamURL:          "",
		UpstreamAPIKey:       "",
		ModelName:            "photon-demo",
	}
}

// LoadConfig reads configuration from the environment.
//
// getenv is injected rather than os.Getenv called directly, so a test can supply
// a map instead of mutating the process environment. Mutating the environment in
// a test makes tests order-dependent, and an order-dependent test suite fails
// intermittently on machines nobody is debugging.
func LoadConfig(getenv func(string) string) (Config, error) {
	cfg := DefaultConfig()

	if v := strings.TrimSpace(getenv("PHOTON_ADDR")); v != "" {
		cfg.Addr = v
	}

	// Tenants are configured as "token:tenant,token:tenant". A comma-separated
	// string in an environment variable is not elegant, and it is used here
	// because a configuration file for a demo is a worse trade: it is another
	// file to keep in sync, and another place for a token to be committed.
	//
	// A real deployment reads these from a secret store. That is stated in the
	// README rather than hidden, because a token in an environment variable is
	// visible in `ps` output and in every crash dump.
	if v := strings.TrimSpace(getenv("PHOTON_TENANTS")); v != "" {
		tenants, err := parseTenants(v)
		if err != nil {
			return Config{}, err
		}
		cfg.Tenants = tenants
	}

	var err error
	if cfg.MaxStreams, err = envInt(getenv, "PHOTON_MAX_STREAMS", cfg.MaxStreams); err != nil {
		return Config{}, err
	}
	if cfg.PerTenantStreams, err = envInt(getenv, "PHOTON_TENANT_STREAMS", cfg.PerTenantStreams); err != nil {
		return Config{}, err
	}
	if cfg.PerTenantRPM, err = envInt(getenv, "PHOTON_TENANT_RPM", cfg.PerTenantRPM); err != nil {
		return Config{}, err
	}
	if cfg.MaxStreamBufferBytes, err = envInt(getenv, "PHOTON_STREAM_BUFFER", cfg.MaxStreamBufferBytes); err != nil {
		return Config{}, err
	}
	if cfg.StallTimeout, err = envDuration(getenv, "PHOTON_STALL_TIMEOUT", cfg.StallTimeout); err != nil {
		return Config{}, err
	}
	if v := strings.TrimSpace(getenv("PHOTON_UPSTREAM_URL")); v != "" {
		cfg.UpstreamURL = v
	}
	cfg.UpstreamAPIKey = getenv("PHOTON_UPSTREAM_KEY")
	if v := strings.TrimSpace(getenv("PHOTON_MODEL")); v != "" {
		cfg.ModelName = v
	}

	if err := cfg.validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}

// parseTenants reads "token:tenant,token:tenant".
func parseTenants(s string) (map[string]*Tenant, error) {
	out := make(map[string]*Tenant)
	for i, pair := range strings.Split(s, ",") {
		pair = strings.TrimSpace(pair)
		if pair == "" {
			continue
		}
		token, name, found := strings.Cut(pair, ":")
		token, name = strings.TrimSpace(token), strings.TrimSpace(name)
		if !found || token == "" || name == "" {
			return nil, fmt.Errorf("PHOTON_TENANTS entry %d is %q, want token:tenant", i+1, pair)
		}
		if len(token) < 8 {
			// A short token is almost always a placeholder that shipped to
			// production. Refusing it at startup is a courtesy, not a guarantee.
			return nil, fmt.Errorf("PHOTON_TENANTS entry %d has a token shorter than 8 characters", i+1)
		}
		if _, dup := out[token]; dup {
			return nil, fmt.Errorf("PHOTON_TENANTS entry %d repeats a token", i+1)
		}
		out[token] = &Tenant{Name: name}
	}
	if len(out) == 0 {
		return nil, fmt.Errorf("PHOTON_TENANTS is set but contains no usable entries")
	}
	return out, nil
}

// validate rejects a configuration that is self-contradictory or unsafe.
//
// The negative-value checks matter more than they look. A MaxStreams of -1 read
// as "unlimited" would turn a typo into an unbounded server, and the failure
// would appear under load rather than at startup.
func (c Config) validate() error {
	if c.Addr == "" {
		return fmt.Errorf("addr is empty")
	}
	switch {
	case c.MaxStreams < 0:
		return fmt.Errorf("PHOTON_MAX_STREAMS is %d: use 0 for the derived default", c.MaxStreams)
	case c.MaxStreamBufferBytes < 0:
		return fmt.Errorf("PHOTON_STREAM_BUFFER is %d: use 0 for unlimited", c.MaxStreamBufferBytes)
	case c.StallTimeout < 0:
		return fmt.Errorf("PHOTON_STALL_TIMEOUT is %s: use 0 for no timeout", c.StallTimeout)
	case c.PerTenantStreams < 0:
		return fmt.Errorf("PHOTON_TENANT_STREAMS is %d: use 0 for unlimited", c.PerTenantStreams)
	case c.PerTenantRPM < 0:
		return fmt.Errorf("PHOTON_TENANT_RPM is %d: use 0 for unlimited", c.PerTenantRPM)
	}

	// The per-tenant stream limit above the global one would be a limit that can
	// never be reached, which is a configuration mistake rather than a strict
	// policy. Caught here so it is a startup error instead of a number that
	// silently does nothing.
	if c.MaxStreams > 0 && c.PerTenantStreams > c.MaxStreams {
		return fmt.Errorf("PHOTON_TENANT_STREAMS (%d) exceeds PHOTON_MAX_STREAMS (%d): "+
			"the per-tenant limit could never be reached",
			c.PerTenantStreams, c.MaxStreams)
	}
	return nil
}

func envInt(getenv func(string) string, key string, fallback int) (int, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return fallback, nil
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return 0, fmt.Errorf("%s is %q, want an integer", key, v)
	}
	return n, nil
}

func envDuration(getenv func(string) string, key string, fallback time.Duration) (time.Duration, error) {
	v := strings.TrimSpace(getenv(key))
	if v == "" {
		return fallback, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s is %q, want a duration like 30s", key, v)
	}
	return d, nil
}

// lookupTenant finds a tenant by bearer token.
//
// A map lookup is O(1) and not constant-time in the key, but the alternative -
// scanning every token with subtle.ConstantTimeCompare - costs O(tenants) on
// every request. For the tenant counts a real deployment has (tens to hundreds)
// the map is the right trade, and auth.go says so where the decision is made
// rather than burying it here.
func (c Config) lookupTenant(token string) (*Tenant, bool) {
	t, ok := c.Tenants[token]
	return t, ok
}

// Redacted is the Config as it may appear in a log. Tokens and keys are
// replaced rather than omitted, because a struct printed with %+v will include
// them and a field that is missing is a field that gets logged.
func (c Config) Redacted() map[string]any {
	names := make([]string, 0, len(c.Tenants))
	for _, t := range c.Tenants {
		names = append(names, t.Name)
	}
	// Sorted so the log line is stable between runs; a log that reorders itself
	// is a log that is hard to diff.
	sort.Strings(names)
	return map[string]any{
		"addr":                    c.Addr,
		"tenants":                 names,
		"max_streams":             c.MaxStreams,
		"max_stream_buffer_bytes": c.MaxStreamBufferBytes,
		"stall_timeout":           c.StallTimeout.String(),
		"per_tenant_streams":      c.PerTenantStreams,
		"per_tenant_rpm":          c.PerTenantRPM,
		"upstream":                c.UpstreamURL,
		"upstream_key_set":        c.UpstreamAPIKey != "",
		"model":                   c.ModelName,
	}
}

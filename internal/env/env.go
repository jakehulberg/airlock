// Package env reads configuration from environment variables.
package env

import (
	"fmt"
	"os"
	"strings"
	"time"
)

func String(key, def string) string {
	if v, ok := os.LookupEnv(key); ok && v != "" {
		return v
	}
	return def
}

func Duration(key string, def time.Duration) (time.Duration, error) {
	v := String(key, "")
	if v == "" {
		return def, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return d, nil
}

func List(key string) []string {
	var out []string
	for _, s := range strings.Split(String(key, ""), ",") {
		if s = strings.TrimSpace(s); s != "" {
			out = append(out, s)
		}
	}
	return out
}

// Required collects the missing keys so they can all be reported at once.
type Required struct{ Missing []string }

func (r *Required) String(key string) string {
	v := String(key, "")
	if v == "" {
		r.Missing = append(r.Missing, key)
	}
	return v
}

func (r *Required) Err() error {
	if len(r.Missing) == 0 {
		return nil
	}
	return fmt.Errorf("missing required environment: %s", strings.Join(r.Missing, ", "))
}

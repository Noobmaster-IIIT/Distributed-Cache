package config

import "os"

// GetEnv retrieves an environment variable by key, returning a fallback if not set.
func GetEnv(key, fallback string) string {
	if value, ok := os.LookupEnv(key); ok {
		return value
	}
	return fallback
}

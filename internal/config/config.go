// Package config reads 12-factor style environment variables.
package config

import (
	"os"
	"strconv"
	"strings"
	"time"
)

func String(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

func Int(key string, def int) int {
	if v, err := strconv.Atoi(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func Duration(key string, def time.Duration) time.Duration {
	if v, err := time.ParseDuration(os.Getenv(key)); err == nil {
		return v
	}
	return def
}

func Brokers() []string    { return strings.Split(String("KAFKA_BROKERS", "localhost:9092"), ",") }
func EventsTopic() string  { return String("EVENTS_TOPIC", "agent.events") }
func ControlTopic() string { return String("CONTROL_TOPIC", "agent.control") }

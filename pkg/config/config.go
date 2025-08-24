package config

import (
	"os"
	"strconv"
)

type BrokerConfig struct {
	Host              string
	Port              int
	MaxQueueSize      int
	PersistenceDir    string
	EnablePersistence bool
	LogLevel          string
}

func LoadBrokerConfig() *BrokerConfig {
	return &BrokerConfig{
		Host:              getEnv("BROKER_HOST", "localhost"),
		Port:              getEnvAsInt("BROKER_PORT", 8080),
		MaxQueueSize:      getEnvAsInt("MAX_QUEUE_SIZE", 1000),
		PersistenceDir:    getEnv("PERSISTENCE_DIR", "./data"),
		EnablePersistence: getEnvAsBool("ENABLE_PERSISTENCE", false),
		LogLevel:          getEnv("LOG_LEVEL", "INFO"),
	}
}

func getEnv(key, defaultVal string) string {
	if val, exists := os.LookupEnv(key); exists {
		return val
	}

	return defaultVal
}

func getEnvAsInt(name string, defaultVal int) int {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}

	if val, err := strconv.Atoi(valStr); err == nil {
		return val
	}

	return defaultVal
}

func getEnvAsBool(name string, defaultVal bool) bool {
	valStr := getEnv(name, "")
	if valStr == "" {
		return defaultVal
	}

	if val, err := strconv.ParseBool(valStr); err == nil {
		return val
	}

	return defaultVal
}

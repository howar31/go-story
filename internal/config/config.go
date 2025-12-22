package config

import (
	"fmt"
	"os"
	"strconv"
)

// Config holds runtime configuration from environment.
type Config struct {
	// DATABASE_URL: Postgres 連線字串 (必填)
	DatabaseURL string
	// STATICS_HOST: 靜態圖片 host，例如 https://v3-statics-dev.mirrormedia.mg/images (必填)
	StaticsHost string
	// PORT: 服務監聽埠，未設定時預設 8080 (選填)
	Port string
	// REDIS_ENABLED: 是否啟用 Redis cache，預設為 false (選填)
	RedisEnabled bool
	// REDIS_URL: Redis 連線字串，例如 redis://localhost:6379/0 (選填，當 REDIS_ENABLED=true 時建議設定)
	RedisURL string
	// REDIS_TTL: Cache TTL (秒)，預設為 3600 (選填)
	RedisTTL int
}

// Load reads required environment variables.
// DATABASE_URL and STATICS_HOST are mandatory.
// PORT is optional; defaults to "8080".
// REDIS_ENABLED is optional; defaults to false.
// REDIS_URL is optional; required if REDIS_ENABLED=true.
// REDIS_TTL is optional; defaults to 3600 seconds.
func Load() (Config, error) {
	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		StaticsHost: os.Getenv("STATICS_HOST"),
		Port:        os.Getenv("PORT"),
		RedisURL:    os.Getenv("REDIS_URL"),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL not set")
	}
	if cfg.StaticsHost == "" {
		return Config{}, fmt.Errorf("STATICS_HOST not set")
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}

	// 解析 REDIS_ENABLED，預設為 false
	redisEnabledStr := os.Getenv("REDIS_ENABLED")
	if redisEnabledStr != "" {
		enabled, err := strconv.ParseBool(redisEnabledStr)
		if err != nil {
			return Config{}, fmt.Errorf("invalid REDIS_ENABLED value: %v", err)
		}
		cfg.RedisEnabled = enabled
	}

	// 解析 REDIS_TTL，預設為 3600 秒
	redisTTLStr := os.Getenv("REDIS_TTL")
	if redisTTLStr != "" {
		ttl, err := strconv.Atoi(redisTTLStr)
		if err != nil {
			return Config{}, fmt.Errorf("invalid REDIS_TTL value: %v", err)
		}
		cfg.RedisTTL = ttl
	} else {
		cfg.RedisTTL = 3600 // 預設 1 小時
	}

	return cfg, nil
}

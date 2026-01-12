package config

import (
	"fmt"
	"net/url"
	"os"
	"strconv"
	"strings"

	"github.com/joho/godotenv"
)

// Config holds runtime configuration from environment.
type Config struct {
	// DATABASE_URL: Postgres 連線字串 (必填)
	DatabaseURL string
	// STATICS_HOST: 靜態圖片 host，例如 https://v3-statics-dev.mirrormedia.mg/images (必填)
	StaticsHost string
	// PORT: 服務監聽埠，未設定時預設 8080 (選填)
	Port string
	// GO_ENV: 執行環境 (dev/staging/prod)，預設為 dev (選填)
	GoEnv string
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
// GO_ENV is optional; defaults to "dev".
// REDIS_ENABLED is optional; defaults to false.
// REDIS_URL is optional; required if REDIS_ENABLED=true.
// REDIS_TTL is optional; defaults to 3600 seconds.
func Load() (Config, error) {
	_ = godotenv.Load()

	cfg := Config{
		DatabaseURL: os.Getenv("DATABASE_URL"),
		StaticsHost: os.Getenv("STATICS_HOST"),
		Port:        os.Getenv("PORT"),
		GoEnv:       os.Getenv("GO_ENV"),
		RedisURL:    os.Getenv("REDIS_URL"),
	}

	if cfg.DatabaseURL == "" {
		return Config{}, fmt.Errorf("DATABASE_URL not set")
	}

	// 自動處理 DATABASE_URL 的編碼
	encodedURL, err := encodeDatabaseURL(cfg.DatabaseURL)
	if err != nil {
		return Config{}, fmt.Errorf("failed to encode DATABASE_URL: %w", err)
	}
	cfg.DatabaseURL = encodedURL

	if cfg.StaticsHost == "" {
		return Config{}, fmt.Errorf("STATICS_HOST not set")
	}
	if cfg.Port == "" {
		cfg.Port = "8080"
	}
	if cfg.GoEnv == "" {
		cfg.GoEnv = "dev"
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

// encodeDatabaseURL 自動處理 DATABASE_URL 的編碼
// 如果 URL 中的密碼尚未編碼，會自動進行 URL 編碼
func encodeDatabaseURL(rawURL string) (string, error) {
	// 先嘗試用 url.Parse 解析
	parsed, err := url.Parse(rawURL)
	if err != nil {
		// 如果解析失敗，可能是因為密碼包含特殊字符，手動解析
		return encodeDatabaseURLManual(rawURL)
	}

	// 檢查是否需要編碼密碼部分
	if parsed.User != nil {
		password, hasPassword := parsed.User.Password()
		if hasPassword && password != "" {
			// 嘗試解碼，如果解碼後與原值不同，表示已經編碼過，直接返回
			decodedPassword, decodeErr := url.QueryUnescape(password)
			if decodeErr == nil && decodedPassword != password {
				return rawURL, nil
			}

			// 未編碼，直接對密碼做 URL 編碼
			encodedPassword := url.QueryEscape(password)
			// QueryEscape 會把空格變成 +，PostgreSQL URL 習慣使用 %20
			encodedPassword = strings.ReplaceAll(encodedPassword, "+", "%20")

			// 重新構建 UserInfo
			userInfo := url.UserPassword(parsed.User.Username(), encodedPassword)
			parsed.User = userInfo

			// 返回編碼後的完整 URL
			return parsed.String(), nil
		}
	}

	// 不需要編碼，返回原值
	return rawURL, nil
}

// encodeDatabaseURLManual 手動解析並編碼 DATABASE_URL
// 當 url.Parse 失敗時使用（通常是因為密碼包含特殊字符）
func encodeDatabaseURLManual(rawURL string) (string, error) {
	// 格式：postgres://user:password@host:port/database?params
	// 或：postgresql://user:password@host:port/database?params

	// 找到協議部分
	schemeEnd := strings.Index(rawURL, "://")
	if schemeEnd == -1 {
		return rawURL, nil
	}
	scheme := rawURL[:schemeEnd]
	rest := rawURL[schemeEnd+3:]

	// 找到 @ 符號，分隔 userinfo 和 host
	atIndex := strings.LastIndex(rest, "@")
	if atIndex == -1 {
		// 沒有 userinfo，返回原值
		return rawURL, nil
	}

	userinfo := rest[:atIndex]
	hostAndPath := rest[atIndex+1:]

	// 解析 userinfo：user:password
	colonIndex := strings.Index(userinfo, ":")
	if colonIndex == -1 {
		// 沒有密碼，返回原值
		return rawURL, nil
	}

	username := userinfo[:colonIndex]
	password := userinfo[colonIndex+1:]

	// 檢查密碼是否已經編碼
	decodedPassword, decodeErr := url.QueryUnescape(password)
	if decodeErr == nil && decodedPassword != password {
		// 已經編碼過，不需要再處理
		return rawURL, nil
	}

	// 未編碼，直接對密碼進行 URL 編碼
	encodedPassword := url.QueryEscape(password)
	// QueryEscape 會把空格變成 +，但 PostgreSQL URL 需要 %20
	encodedPassword = strings.ReplaceAll(encodedPassword, "+", "%20")

	// 重新構建 URL
	encodedURL := fmt.Sprintf("%s://%s:%s@%s", scheme, username, encodedPassword, hostAndPath)
	return encodedURL, nil
}

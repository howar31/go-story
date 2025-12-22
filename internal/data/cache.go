package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Cache wraps Redis client with enabled flag.
// If Redis connection fails, Enabled will be set to false.
type Cache struct {
	client  *redis.Client
	enabled bool
	ttl     time.Duration
}

// NewCache creates a new cache instance.
// If Redis connection fails, enabled will be set to false.
func NewCache(redisURL string, enabled bool, ttlSeconds int) (*Cache, error) {
	cache := &Cache{
		enabled: false,
		ttl:     time.Duration(ttlSeconds) * time.Second,
	}

	if !enabled || redisURL == "" {
		return cache, nil
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		// 如果解析 URL 失敗，返回 disabled cache
		return cache, nil
	}

	client := redis.NewClient(opt)

	// 測試連線，如果失敗則將 enabled 設為 false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		// 連線失敗，關閉 client 並返回 disabled cache
		_ = client.Close()
		return cache, nil
	}

	cache.client = client
	cache.enabled = true
	return cache, nil
}

// Enabled returns whether cache is enabled.
func (c *Cache) Enabled() bool {
	return c.enabled && c.client != nil
}

// Close closes the Redis client.
func (c *Cache) Close() error {
	if c.client != nil {
		return c.client.Close()
	}
	return nil
}

// Get retrieves a value from cache.
func (c *Cache) Get(ctx context.Context, key string, dest interface{}) (bool, error) {
	if !c.Enabled() {
		return false, nil
	}

	val, err := c.client.Get(ctx, key).Result()
	if errors.Is(err, redis.Nil) {
		return false, nil
	}
	if err != nil {
		// 如果讀取失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return false, nil
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		return false, fmt.Errorf("unmarshal cache value: %w", err)
	}

	return true, nil
}

// Set stores a value in cache.
func (c *Cache) Set(ctx context.Context, key string, value interface{}) error {
	if !c.Enabled() {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		return fmt.Errorf("marshal cache value: %w", err)
	}

	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		// 如果寫入失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil // 不返回錯誤，讓查詢繼續進行
	}

	return nil
}

// Delete removes a key from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if !c.Enabled() {
		return nil
	}

	if err := c.client.Del(ctx, key).Err(); err != nil {
		// 如果刪除失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil
	}

	return nil
}

// GenerateCacheKey generates a cache key from query parameters.
func GenerateCacheKey(prefix string, params interface{}) string {
	data, err := json.Marshal(params)
	if err != nil {
		// 如果序列化失敗，使用簡單的 key
		return fmt.Sprintf("%s:fallback", prefix)
	}

	hash := sha256.Sum256(data)
	hashStr := hex.EncodeToString(hash[:])
	return fmt.Sprintf("%s:%s", prefix, hashStr)
}

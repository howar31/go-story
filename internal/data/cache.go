package data

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log"
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

	if !enabled {
		log.Printf("[Redis] Cache disabled (REDIS_ENABLED=false)")
		return cache, nil
	}

	if redisURL == "" {
		log.Printf("[Redis] Cache disabled (REDIS_URL not set)")
		return cache, nil
	}

	log.Printf("[Redis] Initializing cache with URL: %s, TTL: %d seconds", redisURL, ttlSeconds)

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Printf("[Redis] Failed to parse Redis URL: %v", err)
		return cache, nil
	}

	client := redis.NewClient(opt)

	// 測試連線，如果失敗則將 enabled 設為 false
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		log.Printf("[Redis] Connection failed: %v", err)
		_ = client.Close()
		return cache, nil
	}

	cache.client = client
	cache.enabled = true
	log.Printf("[Redis] Cache enabled and connected successfully")
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
		log.Printf("[Redis] Cache miss: %s", key)
		return false, nil
	}
	if err != nil {
		log.Printf("[Redis] Get error for key %s: %v (disabling cache)", key, err)
		// 如果讀取失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return false, nil
	}

	if err := json.Unmarshal([]byte(val), dest); err != nil {
		log.Printf("[Redis] Unmarshal error for key %s: %v", key, err)
		return false, fmt.Errorf("unmarshal cache value: %w", err)
	}

	log.Printf("[Redis] Cache hit: %s", key)
	return true, nil
}

// Set stores a value in cache.
func (c *Cache) Set(ctx context.Context, key string, value interface{}) error {
	if !c.Enabled() {
		return nil
	}

	data, err := json.Marshal(value)
	if err != nil {
		log.Printf("[Redis] Marshal error for key %s: %v", key, err)
		return fmt.Errorf("marshal cache value: %w", err)
	}

	if err := c.client.Set(ctx, key, data, c.ttl).Err(); err != nil {
		log.Printf("[Redis] Set error for key %s: %v (disabling cache)", key, err)
		// 如果寫入失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil // 不返回錯誤，讓查詢繼續進行
	}

	log.Printf("[Redis] Cache set: %s (TTL: %v)", key, c.ttl)
	return nil
}

// Delete removes a key from cache.
func (c *Cache) Delete(ctx context.Context, key string) error {
	if !c.Enabled() {
		return nil
	}

	if err := c.client.Del(ctx, key).Err(); err != nil {
		log.Printf("[Redis] Delete error for key %s: %v (disabling cache)", key, err)
		// 如果刪除失敗，可能是連線問題，將 enabled 設為 false
		c.enabled = false
		return nil
	}

	log.Printf("[Redis] Cache deleted: %s", key)
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

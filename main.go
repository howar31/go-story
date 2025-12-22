package main

import (
	"log"
	"net/http"

	"go-story/internal/config"
	"go-story/internal/data"
	"go-story/internal/schema"
	"go-story/internal/server"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	db, err := data.NewDB(cfg.DatabaseURL)
	if err != nil {
		log.Fatalf("failed to connect db: %v", err)
	}
	defer db.Close()

	// 初始化 Redis cache
	cache, err := data.NewCache(cfg.RedisURL, cfg.RedisEnabled, cfg.RedisTTL)
	if err != nil {
		log.Printf("warning: failed to initialize cache: %v", err)
	}
	defer cache.Close()

	if cache.Enabled() {
		log.Printf("Redis cache enabled (TTL: %d seconds)", cfg.RedisTTL)
	} else {
		log.Printf("Redis cache disabled")
	}

	repo := data.NewRepo(db, cfg.StaticsHost, cache)
	gqlSchema, err := schema.Build(repo)
	if err != nil {
		log.Fatalf("failed to build schema: %v", err)
	}

	http.Handle("/api/graphql", server.NewGraphQLHandler(gqlSchema))
	http.HandleFunc("/probe", server.ProbeHandler)
	http.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("GraphQL endpoint is available at POST /api/graphql"))
	})

	addr := ":" + cfg.Port
	log.Printf("GraphQL server listening on %s (POST /api/graphql)", addr)
	log.Fatal(http.ListenAndServe(addr, nil))
}

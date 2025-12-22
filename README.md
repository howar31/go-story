# go-story

以 Go 實作的 GraphQL 伺服器，提供 Mirror Media CMS 的 `post` / `external` 查詢能力，並內建 `/probe` 端點可對既有 GQL 服務執行測試查詢。

## 環境需求
- **必填**
  - `DATABASE_URL`：Postgres 連線字串
  - `STATICS_HOST`：靜態圖片 host，例如 `https://v3-statics-dev.mirrormedia.mg/images`
- **選填**
  - `PORT`：服務監聽埠，預設 `8080`
  - `REDIS_ENABLED`：是否啟用 Redis cache，預設 `false`
  - `REDIS_URL`：Redis 連線字串，例如 `redis://localhost:6379/0`（當 `REDIS_ENABLED=true` 時建議設定）
  - `REDIS_TTL`：Cache TTL（秒），預設 `3600`（1 小時）

## 主要端點
- `POST /api/graphql`：GraphQL 端點
- `POST /probe`：接受 payload `{"url": "<target gql url>"}`，會同時對「目標 GQL」與「目前這個 server 的 /api/graphql」跑內建測試（posts list、post by slug、externals list、external by slug），只回傳是否一致與各自 status/error，不回傳目標 GQL 的資料內容。
- `GET /`：簡易說明

## 專案結構
- `main.go`：啟動入口，載入 config、建立 DB、建構 schema，啟動 server。
- `internal/config`：環境參數讀取 (`DATABASE_URL`、`STATICS_HOST`、`PORT`)。
- `internal/data`：DB 連線 (`NewDB`)、`Repo`（posts/externals 查詢與關聯組裝、圖片 URL 拼接）。
- `internal/schema`：GraphQL schema 建置（型別/輸入/enum、resolver 連接 `Repo`）。
- `internal/server`：HTTP handlers（`/api/graphql`、`/probe`）。
- `Dockerfile`：多階段建置（Go 1.22 → distroless）。
- `cloudbuild.yaml`：Cloud Build，建置並推送 `gcr.io/$PROJECT_ID/${_IMAGE_NAME}:$COMMIT_SHA`。

## 本機開發
```bash
export DATABASE_URL="postgres://user:pass@host/db?sslmode=disable"
export STATICS_HOST="https://v3-statics-dev.mirrormedia.mg/images"
export PORT=8080

# 可選：啟用 Redis cache
export REDIS_ENABLED=true
export REDIS_URL="redis://localhost:6379/0"
export REDIS_TTL=3600

go run .
```

**注意**：如果 `REDIS_ENABLED=true` 但 Redis 連線失敗，系統會自動將 cache 設為 disabled，不會影響服務運作。

測試 `/probe` 範例：
```bash
curl -X POST http://localhost:8080/probe \
  -H 'content-type: application/json' \
  -d '{"url":"https://mirror-cms-gql-dev-983956931553.asia-east1.run.app/api/graphql"}'
```

## Docker
```bash
docker build -t go-story:local .
docker run --rm -p 8080:8080 \
  -e DATABASE_URL="postgres://user:pass@host/db?sslmode=disable" \
  -e STATICS_HOST="https://v3-statics-dev.mirrormedia.mg/images" \
  -e REDIS_ENABLED=true \
  -e REDIS_URL="redis://redis-host:6379/0" \
  -e REDIS_TTL=3600 \
  go-story:local
```

## 注意事項
- `/api/graphql` 路徑與 KeystoneJS 對齊。
- 預設會將 posts / externals 的 `state` 套用 `published` 過濾。
- externals 預設排序過濾掉 `publishedDate` 為 null。
- relateds/relatedsOne/relatedsTwo 會依 `_Post_relateds` 雙向關聯填入。


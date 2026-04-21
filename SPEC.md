# go-story — Spec

AI-readable, load-bearing spec. Keep this file accurate when code changes.

## 1. Goal

Read-only GraphQL facade over Mirror Media's KeystoneJS Postgres database. Frontend queries go-story instead of hitting Keystone directly, gaining:

- Predictable field shapes aligned with Keystone's GraphQL schema
- Redis caching with graceful degradation
- Parallel relationship enrichment
- `/probe` endpoint for cross-server parity tests

Source of truth for schema shape: `/opt/projects/_mirror/Lilith/packages/mirrormedia/schema.graphql` (Keystone). go-story must mirror field names, types, and nullability of exposed entities.

## 2. Architecture

```
frontend ──► go-story /api/graphql ──► Repo ──► Postgres (written by Keystone)
                                       │
                                       └─► Redis (optional cache)
```

- No HTTP calls back to Keystone. All data is read from the shared Postgres.
- Keystone owns writes + migrations. go-story is read-only.

## 3. Layout

| Path | Role |
|---|---|
| `main.go` | Wire config → DB → cache → repo → schema → HTTP |
| `internal/config/config.go` | Env var loading; URL-encodes DB password automatically |
| `internal/data/repo.go` | Domain models, SQL queries, relationship enrichment, filter/order builders |
| `internal/data/cache.go` | Redis wrapper; SHA256-keyed; enabled flag; graceful degradation |
| `internal/schema/schema.go` | GraphQL types, inputs, enums, resolvers |
| `internal/schema/utils.go` | Schema helpers (`normalizePost`, filter matchers, etc.) |
| `internal/server/server.go` | `/api/graphql` handler + `/probe` |
| `Dockerfile` | Multi-stage (Go 1.22 → distroless) |
| `cloudbuild.yaml` | Cloud Build to `gcr.io/$PROJECT_ID/${_IMAGE_NAME}:$COMMIT_SHA` |

## 4. Exposed GraphQL Root

```graphql
type Query {
  posts(take: Int, skip: Int, orderBy: [PostOrderByInput!], where: PostWhereInput): [Post]
  postsCount(where: PostWhereInput): Int
  post(where: PostWhereUniqueInput!): Post

  topics(...): [Topic]
  topicsCount(...): Int
  topic(where: TopicWhereUniqueInput!): Topic

  externals(...): [External]
  externalsCount(...): Int
}
```

Custom scalars: `JSON` (arbitrary), `DateTime` (ISO 8601 string).

## 5. Post — Keystone-aligned field map

Source: `Lilith/packages/mirrormedia/schema.graphql`.

Exposed fields (exhaustive list of what go-story mirrors):

| GraphQL field | GraphQL type | Go (`data.Post`) | DB column |
|---|---|---|---|
| `id` | `ID` | `ID string` | `id` |
| `slug` | `String` | `Slug` | `slug` |
| `title` | `String` | `Title` | `title` |
| `subtitle` | `String` | `Subtitle` | `subtitle` |
| `state` | `String` | `State` | `state` |
| `style` | `String` | `Style` | `style` |
| `publishedDate` | `DateTime` | `PublishedDate` | `publishedDate` |
| `updatedAt` | `DateTime` | `UpdatedAt` | `updatedAt` |
| `isMember` | `Boolean` | `IsMember` | `isMember` |
| `isAdult` | `Boolean` | `IsAdult` | `isAdult` |
| `isFeatured` | `Boolean` | `IsFeatured` | `isFeatured` |
| `isAdvertised` | `Boolean` | `IsAdvertised` | `isAdvertised` |
| `hiddenAdvertised` | `Boolean` | `HiddenAdvertised` | `hiddenAdvertised` |
| `auto_faq` | `Boolean` | `AutoFAQ` | `auto_faq` |
| `faqs_algo` | `JSON` | `FAQsAlgo any` | `faqs_algo` (jsonb) |
| `heroCaption` | `String` | `HeroCaption` | `heroCaption` |
| `extend_byline` | `String` | `ExtendByline` | `extend_byline` |
| `redirect` | `String` | `Redirect` | `redirect` |
| `og_title` / `og_description` | `String` | `OgTitle`, `OgDescription` | `og_title`, `og_description` |
| `brief` / `content` / `trimmedContent` | `JSON` | `map[string]any` | `brief`, `content` |
| `heroImage` / `heroVideo` / `og_image` | `Photo` / `Video` | pointer relations | FK columns |
| `sections` / `categories` | relation list | enriched | junction tables |
| `writers` / `photographers` / `camera_man` / `designers` / `engineers` / `vocals` | `[Contact]` | enriched | role junction tables |
| `tags` / `tags_algo` | `[Tag]` | enriched | junction tables |
| `topics` | `Topic` | FK | `topics` |
| `relateds` / `relatedsOne` / `relatedsTwo` / `relatedsInInputOrder` | `[Post]` / `Post` | enriched | `_Post_relateds` / FK |
| `related_videos` / `relatedVideosInInputOrder` | `[Video]` | enriched | junction |
| `manualOrderOfSections` / `...Categories` / `...Writers` / `...Relateds` / `...RelatedVideos` | `JSON` | `[]map[string]any` | same columns |

`PostWhereInput` exposes filters for: `slug`, `state`, `sections`, `categories`, `isAdult`, `isMember`, `isFeatured`, `auto_faq`, `topics`. JSON fields (`faqs_algo`, `brief`, `content`) are not filterable — matches Keystone.

`PostOrderByInput` allows: `publishedDate`, `updatedAt`, `title`, `auto_faq`. Anything else falls back to `publishedDate DESC` in `buildOrderClause`.

`PostWhereUniqueInput`: `id` or `slug`.

## 6. Known Schema Drift (not yet mirrored)

Fields present in Keystone `Post` but not yet surfaced in go-story. Add them only when frontend needs them:

- `memberFeed` (Boolean)
- `adTrace` / `css` (String)
- `groups` (relationship, needs `_Post_groups` junction)
- `from_External_relateds` (relationship)
- `createdAt` / `createdBy` / `updatedBy` (tracking)
- `lockBy` / `lockExpireAt` / `updateTimeStamp` / `publishedDateString` / `preview` / `apiData` / `apiDataBrief` / `trimmedApiData` — CMS/internal/virtual fields that frontend should not need

## 7. Read Path (Posts)

`QueryPosts(ctx, where, orders, take, skip)`:

1. `ensurePostPublished(where)` forces `state IN ('published', 'invisible')` at read time.
2. Cache lookup keyed on SHA256 of `{where, orders, take, skip}`.
3. Build parameterized SQL against `"Post" p`. Filters supported in SQL: `slug`, `state`, `isAdult`, `isMember`, `auto_faq`, `sections.some.{slug,state}`, `categories.some.{slug,state,isMemberOnly}`.
4. Scan rows → `[]Post` with raw JSON columns, FK IDs stashed in `p.Metadata`.
5. `enrichPosts(ctx, posts)` fans out goroutines to populate sections, categories, writers (+ each contact role), tags, heroImage, heroVideo, ogImage, topics, relateds (+ one/two/input-order), related_videos, images for nested posts. Each fetch joins the relevant `_Post_*` junction table.
6. Cache write on success.

`QueryPostByUnique` is the same path minus pagination/orderBy; enforces `state IN ('published','invisible')`.

`QueryPostsCount` has its own WHERE builder — **keep filters in sync with `QueryPosts`** or counts diverge from list results.

## 8. JSON Decoding Helpers (`repo.go`)

- `decodeJSONBytes(raw) map[string]any` — object-shaped JSON (`brief`, `content`).
- `decodeJSONArray(raw) []map[string]any` — array-of-objects (`manualOrderOfSections`, etc.).
- `decodeJSONAny(raw) any` — generic JSON for fields with unknown shape (`faqs_algo`). Use this when the JSON may be either object or array and is passed through untouched to GraphQL.

## 9. Cache (`internal/data/cache.go`)

- Enabled only when `REDIS_ENABLED=true` and Redis reachable at init.
- If Redis fails mid-flight, cache is flipped off for the rest of the process (never crashes).
- Keys: `posts`, `posts:count`, `post:unique`, `topics:v2`, etc., suffixed by SHA256 of params.
- TTL: `REDIS_TTL` seconds (default 3600).
- **Invalidation**: none. Deploys that change `Post` shape must flush Redis (`FLUSHDB` or `posts:*` / `post:unique:*`), otherwise deserialized `Post` values are missing new fields until TTL expires.

## 10. `/probe`

POST `{"url": "<target gql>"}`. Runs a fixed set of queries (posts list, post by slug, externals list, external by slug) against both the target and this server's `/api/graphql`, reports per-query status/error + JSON-equality (`reflect.DeepEqual` on decoded bodies). Does **not** return the target's body content.

## 11. Adding a Keystone-aligned field (workflow)

1. Confirm field in `Lilith/.../schema.graphql` → note GraphQL name, type, nullability, whether it appears in `WhereInput` / `OrderByInput`.
2. `internal/data/repo.go`:
   - Add field to `Post` struct with `json` tag matching Keystone's GraphQL name.
   - If filterable: add to `PostWhereInput` with `mapstructure` tag.
   - SELECT column in both `QueryPosts` and `QueryPostByUnique`; add matching `Scan` target.
   - SQL WHERE branch in `QueryPosts` **and** `QueryPostsCount`.
   - If orderable: add case to `buildOrderClause`.
   - For JSON columns with unknown shape, use `decodeJSONAny`.
3. `internal/schema/schema.go`:
   - Add field to `postType` (resolver reads from `normalizePost(p.Source)`).
   - If filterable: add to `postWhereInputType`.
   - If orderable: add to `postOrderByInput`.
4. Keep field name **identical** to Keystone (usually `snake_case` like `auto_faq`, `og_title`). Do not camelCase.
5. `go build ./... && go vet ./...`; introspection should match Keystone's type line-for-line.
6. If cache is enabled in any target env: flush Redis on deploy.

## 12. Environment

Required: `DATABASE_URL`, `STATICS_HOST`.
Optional: `PORT` (8080), `GO_ENV` (`dev`/`staging`/`prod`), `REDIS_ENABLED` (false), `REDIS_URL`, `REDIS_TTL` (3600), `DB_MAX_OPEN_CONNS` (20), `DB_MAX_IDLE_CONNS` (10), `DB_CONN_MAX_IDLE_SECONDS` (300). `.env` auto-loaded via `godotenv`.

## 13. Verification

- `go build ./...` and `go vet ./...` must stay clean.
- GraphQL introspection diff: `Post`, `PostWhereInput`, `PostOrderByInput` field set should be a subset of Keystone's, and each overlapping field should have identical type + nullability.
- `/probe` against a known Keystone endpoint should report `equal: true` for the built-in queries when both services point at the same DB.

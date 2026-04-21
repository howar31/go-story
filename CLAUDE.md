# CLAUDE.md — go-story

Index for Claude Code. Detailed spec lives in `SPEC.md`; human-facing docs in `README.md`.

## What this is
Read-only GraphQL facade over Mirror Media's KeystoneJS Postgres. Frontend hits this instead of Keystone directly. See `SPEC.md` §1–§2 for goals and architecture.

## Run / build
```bash
# required env
export DATABASE_URL="postgres://user:pass@host/db?sslmode=disable"
export STATICS_HOST="https://v3-statics-dev.mirrormedia.mg/images"

go run .                # dev
go build ./...          # compile check
go vet ./...            # static analysis
docker build -t go-story:local .
```

Full env var list: `SPEC.md` §12 or `README.md`.

## Where things live
- `main.go` — wiring
- `internal/config/` — env loading, `DATABASE_URL` password auto-encoding
- `internal/data/repo.go` — domain models, SQL, `enrichPosts` fan-out
- `internal/data/cache.go` — Redis with graceful degradation
- `internal/schema/schema.go` — GraphQL types + resolvers
- `internal/server/server.go` — HTTP handlers (`/api/graphql`, `/probe`)

## Source of truth for schema
`/opt/projects/_mirror/Lilith/packages/mirrormedia/schema.graphql` (Keystone). Field names, types, nullability on `Post` / `Topic` / `External` must mirror Keystone exactly, including `snake_case` (e.g. `auto_faq`, `faqs_algo`, `og_title`). Known drift list: `SPEC.md` §6.

## Conventions
- Code comments in English. User-facing responses in Traditional Chinese (zh-TW).
- Add fields only when frontend needs them — do not pre-emptively sync every Keystone field.
- Adding a Keystone-aligned field: follow the 6-step workflow in `SPEC.md` §11 (struct, WHERE in both `QueryPosts` **and** `QueryPostsCount`, order whitelist, GraphQL type/where/orderBy, cache flush).
- Use `decodeJSONAny` for JSON columns of unknown shape (`faqs_algo`); `decodeJSONBytes` for object-shaped; `decodeJSONArray` for array-of-objects.
- `QueryPostsCount` has a separate WHERE builder from `QueryPosts` — keep them in sync or counts diverge from list results.

## Commit / release
- One feature per commit (see user's global `~/.claude/CLAUDE.md`). Never commit without explicit user approval.
- Use `/commit` skill. It syncs `CLAUDE.md` / `README.md` / `SPEC.md` in the same commit as code changes.
- Post-deploy checklist when `Post` shape changes: `FLUSHDB` (or purge `posts:*` / `post:unique:*`) on every cache-enabled environment; otherwise stale deserialized `Post` objects drop the new fields until TTL.

## Endpoints
- `POST /api/graphql` — GraphQL
- `POST /probe` — cross-server parity check; payload `{"url":"<target gql>"}`
- `GET /` — info string

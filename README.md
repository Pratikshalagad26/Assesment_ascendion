# Cohort Insights API

Backend service that ingests documents, runs them through a two-stage async
pipeline (processing → enriching), and serves results to the submitting user
and to an external partner that keys documents by `client_doc_ref`.

Stack: **Python 3.11+ · FastAPI · MongoDB · Redis · Docker Compose**

---

## Quick start

```bash
cp .env.example .env
docker compose up --build
```

API: `http://localhost:8000`  
OpenAPI docs: `http://localhost:8000/docs`

Local development (MongoDB + Redis already running):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements-dev.txt
uvicorn app.main:app --reload
# in another terminal:
python -m app.workers.pipeline_worker
```

Tests (use in-memory Mongo/Redis mocks — no Docker required):

```bash
pip install -r requirements-dev.txt
pytest -v
```

---

## Architecture

```
┌────────────┐     ┌─────────────┐     ┌──────────────┐
│  FastAPI   │────▶│   MongoDB   │◀────│   Worker     │
│  (API)     │     │  documents  │     │  (poll loop) │
└─────┬──────┘     └─────────────┘     └──────┬───────┘
      │                                       │
      └──────────────┬────────────────────────┘
                     ▼
               ┌──────────┐
               │  Redis   │
               │ cache +  │
               │ rate lim │
               └──────────┘
```

- **API process** — validates input, enforces ownership/rate limits, reads/writes documents, applies cache hits.
- **Worker process** — same codebase, different entrypoint. Polls MongoDB, claims jobs with optimistic locks, runs the two simulated stages.
- **No Celery/Kafka** — a single worker loop is enough for the assignment and stays within the requested stack.

---

## API endpoints

| Method | Path | Description |
|--------|------|-------------|
| `POST` | `/documents` | Submit a document (201). `user_id`, `title`, `content`, optional `client_doc_ref`. |
| `PATCH` | `/documents/{document_id}` | Replace content; requeues both stages. Requires `user_id`. Optional `expected_version`. |
| `GET` | `/documents/{document_id}` | Poll status / results. Requires `user_id`. |
| `GET` | `/documents/by-ref/{client_doc_ref}` | Crosswalk lookup. Requires `user_id`. |
| `GET` | `/users/{user_id}/documents` | Paginated list (`page`, `page_size`, optional `status`). |
| `GET` | `/health` | MongoDB + Redis connectivity. |

Ownership is passed as `?user_id=` or the `X-User-Id` header. Non-owners receive **404** (same as missing), so existence is not leaked.

### Example: create

```bash
curl -s -X POST http://localhost:8000/documents \
  -H 'Content-Type: application/json' \
  -d '{
    "user_id": "alice",
    "title": "Weekly cohort notes",
    "content": "Retention improved for the March signup cohort...",
    "client_doc_ref": "cms-9981"
  }'
```

```json
{ "document_id": "66f1...", "status": "queued" }
```

### Example: poll

```bash
curl -s 'http://localhost:8000/documents/66f1...?user_id=alice'
```

```json
{
  "document_id": "66f1...",
  "user_id": "alice",
  "title": "Weekly cohort notes",
  "content": "Retention improved...",
  "content_hash": "a3f2...",
  "content_version": 1,
  "client_doc_ref": "cms-9981",
  "status": "completed",
  "failed_stage": null,
  "summary": "Summary (12 words): Retention improved ...",
  "summary_for_version": 1,
  "tags": ["retention", "cohort", "march"],
  "tags_for_version": 1,
  "result_matches_content": true,
  "created_at": "...",
  "updated_at": "..."
}
```

---

## MongoDB schema

```text
documents
  _id                  ObjectId
  user_id              string
  title                string
  content              string
  content_hash         string          # sha256(content)
  content_version      int             # starts at 1; +1 on every PATCH
  client_doc_ref       string | null   # unique when present
  status               queued | processing | enriching | completed | failed
  failed_stage         processing | enriching | null
  error_message        string | null
  summary              string | null
  summary_for_version  int | null      # content_version that produced summary
  tags                 [string] | null
  tags_for_version     int | null      # content_version that produced tags
  stage_lock           string | null   # worker claim token
  created_at           datetime
  updated_at           datetime
```

### Indexes

| Index | Purpose |
|-------|---------|
| `{user_id: 1, created_at: -1}` | List newest-first |
| `{user_id: 1, status: 1}` | Filtered list |
| `{content_hash: 1}` | Identical-content lookups |
| `{client_doc_ref: 1}` unique sparse | Crosswalk; allows many nulls |
| `{status: 1, updated_at: 1}` | Worker job claiming |

---

## Redis responsibilities

Two separate namespaces — do not conflate them.

| Key | Role | TTL |
|-----|------|-----|
| `active_pipelines:{user_id}` | Integer counter of docs in `queued`/`processing`/`enriching` | none (explicit incr/decr) |
| `content:{content_hash}` | JSON `{summary, tags}` for successfully completed content | `CACHE_TTL_SECONDS` (default 24h) |

**Redis down:**
- Cache: miss and continue (pipeline still runs).
- Rate limit: default **fail-open** (`RATE_LIMIT_ON_REDIS_FAILURE=open`) with an error log; set `closed` to return 503 instead.

---

## Pipeline flow

```
queued → processing → enriching → completed
                 ↘ failed (failed_stage=processing)
                         ↘ failed (failed_stage=enriching; summary kept)
```

1. **Processing** (10–20s simulated, ~10% random fail): mock summary from content.
2. **Enriching** (5–15s simulated, ~10% random fail): mock tags from the stage-1 summary.

Enriching failure does **not** re-run processing. `failed_stage` tells the caller which stage failed. Delays and failure rate are configurable via env (tests set them to 0).

Workers claim jobs with `find_one_and_update` + a `stage_lock` UUID so two workers cannot run the same stage on the same document.

---

## Schema & Staleness Design

This is the core correctness mechanism.

### Fields that matter

- `content_version` — monotonic integer on the document. Starts at 1. Every successful PATCH does `$inc: {content_version: 1}` atomically with content replacement.
- `summary_for_version` / `tags_for_version` — stored **alongside** the derived fields, recording which `content_version` produced them.
- `content_hash` — sha256 of the current content body (used for caching, not for staleness gating).

### What PATCH does (one atomic update)

1. `$inc` `content_version`
2. `$set` new `content` + `content_hash`
3. Clear `summary`, `tags`, `summary_for_version`, `tags_for_version`
4. Clear `stage_lock`, `failed_stage`, `error_message`
5. Set `status = queued`

A reader polling mid-reprocess therefore sees the new content version with **no** derived fields, never the old summary labeled as current.

### How workers bind to a version

When a worker claims a job it reads `content_version` (and `stage_lock`) from the claimed document. Every stage write includes those values in the filter:

```text
{ _id, content_version: <claimed>, status: <expected>, stage_lock: <claimed> }
```

If a PATCH landed meanwhile, `content_version` and/or `stage_lock` no longer match → `matched_count = 0` → the stale worker stops. It cannot overwrite newer results.

### How GET makes mismatches unobservable

`to_response()` only exposes `summary`/`tags` when **both**:

- `summary_for_version == content_version`, and
- `tags_for_version == content_version`

Otherwise both are returned as `null` and `result_matches_content = false`.

Exception for transparency: if status is `failed` with `failed_stage=enriching` and the summary version matches, the summary alone is returned (partial progress) — still never paired with tags from another version.

### Why a reader cannot observe mixed versions

| Forbidden observation | Why it cannot happen |
|-----------------------|----------------------|
| new content + old summary | PATCH clears summary; GET requires `summary_for_version == content_version` |
| old content + new tags | tags are only written for the version the worker claimed; old content implies old version, so a new-version write's filter would not match — and GET would hide mismatched tags anyway |
| summary@v1 + tags@v2 | GET requires **both** version markers to equal current `content_version`; a split pair is hidden entirely |

A boolean `is_stale` flag is not used. The version integers are the source of truth and are compared on every read and every write.

---

## Crosswalk decision

**Strategy: reject duplicates with HTTP 409.**

`client_doc_ref` is unique (sparse index). A second `POST` with the same ref — whether the content matches or not — returns 409 with guidance to:

1. `GET /documents/by-ref/{ref}` to fetch the existing document, or
2. `PATCH /documents/{document_id}` to change content.

**Why reject rather than overwrite or version-in-place on POST:**  
The partner ref is their stable identity. Silently replacing content on POST would hide data loss if retries arrive out of order with stale bodies. Creating a second row would break uniqueness. Explicit 409 forces the partner to choose: retry-as-read vs intentional update via PATCH. Lookup always uses the unique index (never a collection scan).

---

## Ownership decision

Only the submitting `user_id` may `GET`/`PATCH` a document or resolve it by ref. Cross-user access returns **404**, not 403, so the API does not confirm whether the id/ref exists. Listing is already scoped by the path `user_id`.

---

## Rate limiting

Each user may have at most **3** documents concurrently in `{queued, processing, enriching}`.

- On submit (and on PATCH of a non-active doc): `INCR active_pipelines:{user_id}`; if over limit, `DECR` and return **429**.
- On leaving an active state (completed, failed, or cache fast-path): `DECR` (clamped at 0).
- PATCH of an already-active document does **not** consume an extra slot.

---

## Caching

Cache key = `content:{sha256(content)}`, never `document_id`.

- On successful enriching (or create/PATCH cache hit), store `{summary, tags}` with TTL.
- Identical content submitted later completes immediately with `summary_for_version`/`tags_for_version` set to the **new** document's version.
- After PATCH to different content, the old hash entry is irrelevant to the new hash — it cannot leak into the new result.

---

## Concurrency / race handling

| Race | Handling |
|------|----------|
| Two workers, same queued doc | Only one `queued→processing` claim succeeds |
| Two workers, same enriching doc | Only one acquires `stage_lock` |
| PATCH vs in-flight worker | Version + lock filters on write; stale worker no-ops |
| Two PATCHes | Optional `expected_version` → 409 on conflict; otherwise last write wins with monotonically increasing versions |
| Two submits hitting rate limit | Each INCR sees the true count; overshooting callers DECR themselves |

---

## Configuration

See `.env.example`. Important variables:

| Variable | Meaning |
|----------|---------|
| `MONGODB_URL` / `MONGODB_DATABASE` | Mongo connection |
| `REDIS_URL` | Redis connection |
| `CACHE_TTL_SECONDS` | Content cache TTL |
| `MAX_ACTIVE_PIPELINES` | Per-user active limit (default 3) |
| `RATE_LIMIT_ON_REDIS_FAILURE` | `open` or `closed` |
| `PROCESSING_DELAY_*` / `ENRICHING_DELAY_*` | Stage simulation windows |
| `STAGE_FAILURE_RATE` | Approx. per-stage failure probability |

---

## Testing

```bash
pytest -v
```

Coverage includes: create/validation, ownership 404, list + status filter, hashing, two-stage success, processing failure, enriching failure without re-processing, PATCH, mixed-version unobservability, stale worker cannot overwrite, optimistic concurrency, crosswalk + duplicate ref (same and different content), rate limit at 3, cache hit on identical content, health.

Stage delays are set to 0 in tests; failures are injected via mocks — no real 20-second sleeps.

---

## Assumptions

- Caller identity for ownership is supplied as `user_id` query param or `X-User-Id` header (no full auth system in scope).
- Mock summary/tags are deterministic string heuristics, not ML.
- One worker process is assumed for local compose; horizontal worker scale is safe because of claim locks.
- Fail-open rate limiting when Redis is down is the default (availability over strict enforcement).

---

## Limitations / what I'd improve with more time

- Replace skip/limit listing with cursor pagination (`created_at + _id`).
- Dead-letter / retry-with-backoff for failed stages (bonus in the brief).
- Metric for rate-limit fail-open events and cache hit ratio.
- Stronger lease expiry for `stage_lock` if a worker crashes mid-stage.
- Integration tests against real Mongo/Redis in CI (compose profile), keeping mocks for unit speed.
- AuthN/AuthZ instead of trusted `user_id` headers.

---

## At 100×

At one hundred times today’s submit volume, four design points bend first.

**1. `user_id`-based indexes with a 500K-document user.**  
Compound indexes `{user_id, created_at}` and `{user_id, status}` remain correct, but they become hot partitions. A single power-user’s working set may exceed RAM; list queries with large `skip` walk many index keys; `count_documents` for totals gets expensive. Write amplification on that user’s documents also contending on the same index pages. Mitigations: cap page depth, maintain an approximate counter, and consider splitting very large tenants into sub-collections or time-partitioned collections (`documents_2026_q1`) while keeping the same logical schema.

**2. Shard key for horizontal scaling.**  
I would **not** shard solely on `user_id` — that recreates the hot-tenant problem on one shard. Prefer a compound shard key such as `{user_id: 1, _id: 1}` (or `{user_id: hashed, created_at: 1}` depending on query mix): targeted scatter is avoided for the dominant access pattern (per-user reads), while a mega-user’s documents can still spread across chunks as `_id`/`created_at` ranges grow. Crosswalk lookups by `client_doc_ref` would need either a hashed secondary shard-aware index with a fan-out, or a small dedicated `crosswalk` collection mapping `client_doc_ref → {document_id, user_id}` sharded by hashed `client_doc_ref`. Worker claiming by `status` is global and does not shard cleanly — at 100× I would move the work queue to a per-shard or Redis stream based on `user_id` hash so claim traffic is partitioned.

**3. Redis active-pipeline counter at 100× submit QPS.**  
The per-user `INCR`/`DECR` key remains fine for correctness under concurrency — Redis single-threaded command execution serializes increments on one key. The failure mode is not races; it is **hot keys** and connection load. A celebrity user submitting frantically still hits one key (acceptable), but cluster-wide submit QPS stresses the Redis CPU and network. Changes: move to Redis Cluster with hash tags only if multi-key atomics appear; add a short local token-bucket in the API process as a first line of defense; consider `INCR` with TTL heartbeat leases per document instead of a single counter if crash-recovery of counts becomes noisy; and switch fail-open vs fail-closed deliberately under load (fail-closed with cached last-known counts is safer when abuse is the threat).

**4. Skip/limit pagination.**  
`skip((page-1)*page_size)` is O(offset) in the index. At page 5,000 of a 500K-doc user it is unusable. Replace with **cursor pagination**: `GET ...?limit=20&cursor=<created_at>_<id>` implemented as `find({user_id, created_at/id tuple < cursor}).sort(...).limit(n)`. Return an opaque next cursor. Drop exact `total` from the hot path or approximate it. This keeps list latency flat regardless of depth and matches how the compound `{user_id, created_at}` index is meant to be used.

---

## Project layout

```text
app/
  main.py                 # FastAPI app + lifespan
  config.py               # pydantic-settings
  database.py             # Motor + Redis clients
  dependencies.py         # DI helpers, ownership
  routers/                # documents, users, health
  models/                 # enums + Pydantic schemas
  services/               # document, pipeline, cache, rate limit
  repositories/           # MongoDB access
  workers/                # pipeline worker entrypoint
  utils/hashing.py
tests/
docker-compose.yml
Dockerfile
.env.example
requirements.txt
requirements-dev.txt
```

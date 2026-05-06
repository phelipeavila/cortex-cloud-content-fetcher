# local_compliance_fetcher.py - Technical Deep Dive

This document explains the runtime behavior, data model, API contract, and performance characteristics of `local_compliance_fetcher.py`.


## 1) Purpose and Scope

`local_compliance_fetcher.py` is an end-to-end data pipeline that:

1. Discovers latest assessment anchors for `(standard, profile)` pairs.
2. Builds a controls catalog for all requested standards.
3. Fetches all compliance assets for each anchor with paginated parallel calls.
4. Enriches raw asset rows with control metadata.
5. Writes local outputs (CSV/JSON/NDJSON).
6. Optionally pushes assets and control summaries to Cortex HTTP Collector.
7. Optionally performs dedup checks (fail-open) against Cortex datasets before push.

The script is optimized for high-volume collection while keeping operational visibility via retry logging and progress output.

## 2) Execution Model

## Entry point

- `main()` orchestrates all stages.
- CLI args:
  - `--debug` / `-d`: enables verbose diagnostics.
  - `--dry-run`: overrides config and disables push.
  - `--force-push`: bypasses dedup checks.

## Stage order

1. **Stage 1 - Anchors**: `get_all_anchors()`
2. **Stage 2 - Controls**: `get_all_controls()`
3. **Stage 3 - Assets**: `fetch_all_assets()`
4. **Stage 4 - Persist outputs**: `save_all_outputs()`
5. **Stage 5 - Push assets**: `push_assets_to_collector()`
6. **Stage 6 - Build summaries**: `create_control_summary()`
7. **Stage 7 - Push summaries**: `push_summaries_to_collector()`

Final metrics are emitted by `Output.final_summary()`.

## 3) Configuration Contract (`config.env`)

Configuration is loaded from `config.env` next to the script (`dotenv`), then validated.

## Required

- `API_URL`
- `API_ID` (sent as `x-xdr-auth-id`)
- `API_KEY` (sent as `Authorization`)
- `ASSESSMENTS` (JSON array, format below)

## Optional but common

- `TENANT_EXTERNAL_URL` -> derives `COLLECTOR_URL = https://api-{TENANT_EXTERNAL_URL}/logs/v1/event`
- `COMPLIANCE_API_KEY` -> asset push key
- `CONTROLS_API_KEY` -> summary push key
- `PAGE_SIZE` (default `100`)
- `BATCH_SIZE` (default `500`)
- `PARALLEL_WORKERS` (default `5`)
- `MAX_ANCHORS` (default `0`, means no cap)
- `DRY_RUN` (default `true`)
- `OUTPUT_DIR` (default `./output`)

## Dedup controls

- `DEDUP_ENABLED` (default `true`)
- `DEDUP_TIMEFRAME_DAYS` (default `30`)
- `ASSETS_DATASET_NAME` (default `panw_compliance_assets_raw`)
- `SUMMARY_DATASET_NAME` (default `panw_compliance_summary_raw`)
- `DEDUP_TIMEZONE` (default `UTC`, must be a valid IANA timezone like `America/New_York`)

## ETA tuning

- `EST_API_WAVE_SECONDS` (default `7.0`)
  - Used by pre-flight ETA model.
  - Represents mean seconds per parallel "wave" (up to `PARALLEL_WORKERS` page calls).

## Temporary payload dump controls (debug instrumentation)

- `DUMP_GET_ASSETS_PAYLOAD` (`true|false`)
- `DUMP_GET_ASSETS_PAYLOAD_FILE` (NDJSON path)
- `DUMP_GET_ASSETS_CURL` (`true|false`)
- `DUMP_DEDUP_XQL_REQUESTS` (`true|false`)
- `DUMP_DEDUP_XQL_REQUESTS_FILE` (NDJSON path)
- `DUMP_DEDUP_XQL_CURL` (`true|false`)

These are temporary diagnostics and are clearly marked in code.

## ASSESSMENTS shape

```json
[
  {
    "standard": "CIS Critical Security Controls v8.1",
    "profiles": [
      "CIS Critical Security Controls v8.1-AWS",
      "CIS Critical Security Controls v8.1-Azure"
    ]
  }
]
```

## 4) API Client Behavior and Retry Strategy

The script uses a shared `requests.Session` in `CortexAPIClient`.

## Headers

- `x-xdr-auth-id: <API_ID>`
- `Authorization: <API_KEY>`
- `Content-Type: application/json`

## Retry policy

- Max retries: `5` (`MAX_RETRIES`)
- Backoff: exponential with jitter:
  - `delay = BASE_DELAY * (2 ** attempt) + random(0.1..1.0)`
  - `BASE_DELAY = 2.0`
- Retryable status codes: `{429, 500, 502, 503, 504}`
- Retryable exceptions: connection timeout/transport variants.
- Non-retryable HTTP errors are raised immediately.

## Logging nuances

- Retry attempt lines are shown only in debug mode.
- Final exhaustion (`Failed after ... retries`) is always printed.
- On HTTP errors in debug mode, raw response body (up to 2000 chars) is printed.
- Request context can be attached (`request_context`) to make logs traceable by profile/standard/page range.

## 5) Stage 1: Anchor Discovery

Functions:

- `get_all_assessment_results()`
- `find_anchor_for_pair()`
- `get_all_anchors()`

Flow:

1. Query `/public_api/v1/compliance/get_assessment_results`.
2. For each requested `(profile, standard)`:
   - Filter to records where:
     - `TYPE == "profile"` (case-insensitive)
     - profile and standard match exactly after trim
     - `ASSESSMENT_PROFILE_REVISION` is present
3. Select newest by `LAST_EVALUATION_TIME`.
4. Emit anchor:
   - `profile_name`
   - `standard_name`
   - `assessment_profile_revision`
   - `last_evaluation_time`
   - `asset_group_id`

No anchor -> pair is skipped.

## 6) Stage 2: Controls Catalog Construction

Functions:

- `get_controls_for_standard()`
- `fetch_single_control()`
- `get_all_controls()`

Flow:

1. For each unique standard:
   - call `/public_api/v1/compliance/get_standards` filtered by standard name.
   - extract `controls_ids`.
2. For each control ID (parallelized):
   - call `/public_api/v1/compliance/get_control`.
   - accept only `STATUS == "ACTIVE"`.
   - normalize severity enum to human labels.
   - derive:
     - revision
     - control id/name
     - category/subcategory
     - mitigation
     - rule names/count
     - scannable asset types

Catalog structure:

```python
{
  "<standard_name>": {
    "standard_name": "...",
    "total_controls": <int>,
    "controls": [ ...normalized control dict... ]
  }
}
```

## 7) Stage 3: Asset Fetch Pipeline

Functions:

- `get_total_asset_count()`
- `fetch_asset_page()`
- `get_paginated_assets_parallel()`
- `enrich_assets()`
- `fetch_all_assets()`

## API request shape (`/public_api/v1/compliance/get_assets`)

```json
{
  "request_data": {
    "assessment_profile_revision": 123456,
    "last_evaluation_time": 1738108800000,
    "filters": [
      { "field": "status", "operator": "neq", "value": "Not Assessed" }
    ],
    "search_from": 0,
    "search_to": 100
  }
}
```

Count call uses range `0..1`.

## Parallelization strategy

- **Within one anchor**: page calls are parallelized via `ThreadPoolExecutor(max_workers=PARALLEL_WORKERS)`.
- **Across anchors**: processed sequentially in current implementation.

This means total runtime depends heavily on:

- pages per anchor distribution,
- `PARALLEL_WORKERS`,
- API latency/retry rate.

## Enrichment mapping

Each raw asset row is joined to control metadata by:

- asset `CONTROL_REVISION_ID` -> controls catalog `revision`

Enriched record includes anchor context and normalized control data:

- profile/standard/revision/evaluation datetime
- asset identity and cloud metadata
- rule/control fields
- severity/category/subcategory/mitigation

Outputs:

```python
{
  "<profile>|<standard>": [ enriched_asset_rows... ]
}
```

## Pre-flight estimate model

Pre-flight displays:

- per-anchor asset/page counts,
- total assets/pages,
- ETA in minutes.

ETA formula:

1. `pages_i = ceil(asset_count_i / PAGE_SIZE)`
2. `waves_i = ceil(pages_i / PARALLEL_WORKERS)`
3. `total_waves = sum(waves_i)`
4. `eta_seconds = total_waves * EST_API_WAVE_SECONDS`

This model is more realistic than flat request/sec because anchors run sequentially.

## 8) Stage 4: Local Output Persistence

`save_all_outputs()` writes:

- `assets_<profile_standard>_<timestamp>.csv` (one per profile/standard pair with data)
- `controls_catalog_<timestamp>.json`
- `anchors_<timestamp>.json`
- `summary_<timestamp>.json` (meta summary)

All files are written under `OUTPUT_DIR`.

## 9) Stage 5 and Stage 7: Collector Push

`push_to_collector()` handles generic NDJSON batch push:

- Input body: NDJSON (`Content-Type: text/plain`)
- Batch slicing by `BATCH_SIZE`
- Per-batch `requests.post` with timeout `120s`
- Returns counts of pushed records and batches

Assets push (`push_assets_to_collector`) and summaries push (`push_summaries_to_collector`) both:

1. Save NDJSON first (`*_collector_<timestamp>.json`).
2. Print a manual `curl` hint.
3. Skip push when URL/key missing.
4. Respect `DRY_RUN`.

## 10) Summary Generation Logic

`create_control_summary()` computes one summary row per control revision per anchor.

Grouping and metrics:

- Group enriched assets by `CONTROL_REVISION`.
- Count `PASSED`, `FAILED`, `NOT_ASSESSED`.
- Derive:
  - `STATUS`:
    - `FAILED` if any failed
    - else `PASSED` if any passed
    - else `NOT_ASSESSED` if any not assessed
    - else `NOT_EVALUATED`
  - `SCORE = passed / (passed + failed)` (safe 0 when denominator 0)

Summary rows include counts, score, control metadata, anchor metadata.

## 11) Dedup Architecture (Fail-Open)

Dedup is composite-key based and checked separately for:

- assets dataset
- summaries dataset

Core function: `check_existing_anchors()`

Flow:

1. Extract anchor pairs and bucket by day:
   - key = `(assessment_profile_revision, calendar_day_in_DEDUP_TIMEZONE)`
2. Query target dataset through XQL runtime APIs:
   - `start_xql_query`
   - poll with `get_query_results`
3. Build map:
   - `{ (revision_string, day_start_epoch_ms): record_count }`

Query template:

```xql
config timeframe = <N>d
| dataset = <dataset_name>
| filter ASSESSMENT_PROFILE_REVISION in ("rev1","rev2",...)
| filter to_epoch(date_floor(EVALUATION_DATETIME, "d", "<DEDUP_TIMEZONE>"), "millis") = <day_start_epoch_ms>
| comp count() as record_count by ASSESSMENT_PROFILE_REVISION
```

Timezone behavior:

- Python-side day floor and XQL-side day floor both use `DEDUP_TIMEZONE`.
- `DEDUP_TIMEZONE` is validated at startup via `ZoneInfo(...)`.
- Invalid timezone values fail fast with a clear configuration error.

Fail-open rules:

- If query start fails, query fails, or times out -> return `{}`.
- `{}` means no dedup skipping; push/fetch proceeds.

Execution points:

- Before Stage 3 fetch (assets dedup)
- Before Stage 7 push (summary dedup)

`--force-push` bypasses both dedup checks.

## 12) Operational Characteristics and Tradeoffs

## Throughput bottlenecks

- `get_assets` backend capacity and retry frequency dominate runtime.
- Very high `PARALLEL_WORKERS` can increase `429/5xx` rates, harming net throughput.

## Memory profile

- Assets are accumulated in memory before persistence/push.
- For very large assessments, peak RAM can be substantial.

## Idempotency

- Dedup is coarse (revision + calendar-day in `DEDUP_TIMEZONE`), not per-record hash.
- If fail-open path is triggered, duplicates can be ingested.

## Error tolerance

- Asset page failures are isolated in `fetch_asset_page()` and return empty rows for that page.
- This prevents total run abort but can undercount assets for affected pages.

## 13) Performance Tuning Guidelines

1. Start moderate:
   - `PARALLEL_WORKERS=5..15`
   - `PAGE_SIZE=100..300`
2. Increase slowly while monitoring:
   - retry count,
   - 429/500 frequency,
   - wall-clock time.
3. Recalibrate ETA:
   - collect `elapsed_seconds` and `total_waves` from real runs,
   - set `EST_API_WAVE_SECONDS = elapsed_seconds / total_waves`.

Practical note: lower retries and steadier backend behavior usually outperform aggressive burst settings.

## 14) Debugging Playbook

## Enable verbose diagnostics

```bash
python local_compliance_fetcher.py --debug
```

## Inspect payload generation for Postman parity

Enable temporary payload dump options in `config.env`:

- `DUMP_GET_ASSETS_PAYLOAD=true`
- `DUMP_GET_ASSETS_PAYLOAD_FILE=./output/get_assets_payloads.ndjson`
- `DUMP_GET_ASSETS_CURL=true`
- `DUMP_DEDUP_XQL_REQUESTS=true`
- `DUMP_DEDUP_XQL_REQUESTS_FILE=./output/dedup_xql_requests.ndjson`
- `DUMP_DEDUP_XQL_CURL=true`

This emits exact payloads and optional curl forms used by the script. Dedup dump
captures both request and response events for:

- `/public_api/v1/xql/start_xql_query`
- `/public_api/v1/xql/get_query_results`

## Common failure classes

- `HTTP 429/500/502/503/504`: transient backend or throttling, retried automatically.
- non-retryable HTTP errors: surfaced immediately.
- collector push failures: logged per failed batch, run continues.

## 15) Security and Handling Notes

- `config.env` contains sensitive credentials. Do not commit.
- Debug payload/curl output may include auth context and assessment metadata.
- NDJSON outputs can contain large and sensitive compliance inventory data.

## 16) Current Known Temporary Code Areas

The script currently includes a clearly marked temporary section:

- `TEMP DEBUG BLOCK` for get-assets payload dump.
- `TEMP DEBUG BLOCK` for dedup XQL request/response dump.

This block is designed to be removable with minimal impact once troubleshooting is complete.

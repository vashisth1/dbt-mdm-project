# Snowflake + dbt Real-Time MDM Skill

## Purpose

Build a production-oriented, near-real-time customer MDM platform in Snowflake using dbt, with source files from multiple systems such as CRM, ERP, Stripe, etc. arriving in an AWS S3 external stage.

The skill must generate a complete, deployable project structure and SQL/dbt code for:

- S3 external stage ingestion
- file discovery and ingestion metadata
- raw landing tables
- source-specific staging
- canonical customer model
- normalization
- deterministic and fuzzy match candidates
- match scoring and match decisions
- persistent Golden ID / entity identity
- Reltio-like crosswalk
- attribute-level survivorship
- Golden Customer record
- match/merge/unmerge history
- suspect/exception queue
- Snowflake Streams and Tasks for near-real-time processing
- dbt incremental models
- dbt tests
- reconciliation and audit models
- optional Airflow orchestration for ingestion, backfills, reconciliation, and dbt execution

The implementation should NOT depend on synchronized daily batches. Each source file/event must be independently traceable and idempotent.

---

# 0. Discovery (Required Before Any Design Work)

Before generating any project structure, DDL, or dbt code, the skill must first determine what raw data already exists. Never assume a raw layer, file format, or source schema — ask.

## Step 0.1: Ask if raw tables already exist

Ask the user directly:

"Do you already have raw/landing tables in Snowflake for these source systems (CRM, ERP, Stripe, etc.), or does this need to start from files landing in a stage?"

- If **raw tables already exist**, go to Step 0.2 (Inspect Existing Raw Tables).
- If **raw tables do NOT exist** (files only, or nothing built yet), go to Step 0.3 (Collect File Structure / Sample Files).
- If the user is unsure, help them check before proceeding:

```sql
SHOW SCHEMAS IN DATABASE <database>;
SHOW TABLES LIKE '%RAW%' IN DATABASE <database>;
SELECT table_schema, table_name, row_count, bytes
FROM <database>.INFORMATION_SCHEMA.TABLES
ORDER BY table_schema, table_name;
```

## Step 0.2: Inspect Existing Raw Tables

If raw tables exist, do NOT skip inspection — never assume column names or types match this skill's canonical fields.

For each candidate raw table:

```sql
DESCRIBE TABLE <database>.<schema>.<table>;
SELECT * FROM <database>.<schema>.<table> LIMIT 10;
```

Capture and confirm with the user:

- Table name and fully qualified location
- Source system it represents (CRM / ERP / Stripe / other)
- Column names, types, and which columns map to canonical MDM fields (customer id, name, email, phone, DOB, address, etc.)
- Whether the table is append-only/raw (VARIANT payload, event-based) or already a cleaned/overwritten table
- Presence of a natural source record identifier and an update/event timestamp
- Approximate row count and update frequency (batch vs streaming)

Only after this is confirmed should staging models (Section 6) be written against the existing raw tables as dbt `source()` definitions — do not recreate a raw landing layer that already exists.

## Step 0.3: Collect File Structure / Sample Files (No Raw Tables Yet)

If no raw tables exist yet, the skill must NOT invent a file format, S3 path convention, or schema. Ask the user for one of the following, in order of preference:

1. **A sample file** (JSON/CSV/Parquet) for each source system — request it be shared or pointed to on a stage/S3 path.
2. If no sample file is available, ask for the **file structure**: field names, data types, nesting, delimiters/format (CSV, JSON, Parquet, etc.), and one example row per field.
3. If neither is available, ask targeted questions to reconstruct the contract:
   - What fields does each source system export for a customer record (e.g., id, name, email, phone, address, timestamps)?
   - What file format and where do files land (S3 bucket/prefix, or another mechanism)?
   - How often do files arrive, and are they full snapshots or incremental/event-based?
   - Is there a stable, immutable identifier per record, or only a mutable customer ID?

## Step 0.3a: Loading Chat-Uploaded Sample Files Into Raw Tables

When the sample files were provided as chat attachments (not files already living on a stage/S3), do not assume `COPY FILES INTO @stage FROM 'snow://workspace/...'` will find them — chat attachments are staged in a temporary upload location and are **not** part of the persisted workspace snapshot, so a `COPY FILES`/`LIST` against the workspace live version will silently return 0 files even though the attachment was read successfully.

- Read the attachment contents directly (they are accessible as regular files) to confirm the schema/sample rows for Step 0.3.
- For loading into raw tables: if row counts are small (tens of rows), generate the raw table DDL and load it via explicit `INSERT ... SELECT` statements built from the file contents rather than fighting the stage-copy path — this is a legitimate one-time load mechanism, not a workaround to hide.
- For larger files, tell the user the attachment must be saved into the actual workspace (via the file browser/`cortex ws` upload) or an external stage before `COPY FILES`/`COPY INTO` will see it, and pause until that's done — do not silently fall back to partial or truncated loads.
- Always still register the load in `FILE_MANIFEST` (Section 4) regardless of which loading mechanism was used, so idempotency/audit tracking is consistent.

## Step 0.4: Confirm Survivorship Source Priority

Once the source systems present in the raw data have been identified (via Step 0.2 inspection or Step 0.3 file/structure collection), the skill must confirm the survivorship priority order for those exact source systems before generating `seeds/mdm_source_priority.csv` or any survivorship model (Section 13). Never default to alphabetical order, first-seen order, or the order sources happened to be discussed in — priority must be an explicit, confirmed decision.

1. **Check for an existing priority source first.**
   - Look for `seeds/mdm_source_priority.csv` in the project, or an already-populated `MDM_CONTROL.SOURCE_PRIORITY` table in Snowflake.
   - If found, read it and display the current `SOURCE_SYSTEM | PRIORITY` order back to the user, scoped to the source systems actually present in the raw data being worked on. Ask the user to confirm it as-is or request changes.
   - If a source system appears in the raw data but has no entry in the existing priority file/table, explicitly flag it and ask where it should rank before continuing.

2. **If no seed file or priority table exists, collect priorities interactively via a checkbox-style GUI.**
   - Use the `ask_user_question` tool to present the discovered source systems as selectable options — this renders as a checkbox/choice UI to the user rather than requiring free-text ranking.
   - Because ranking requires an explicit order and the question tool does not support drag-and-drop ranking, build the ranking by asking one single-select question per priority slot, removing already-assigned sources from the option list each time:
     - "Which source system should be the highest priority (Priority 1) for survivorship — i.e. whose values win when attributes conflict?" (options = all discovered source systems)
     - "Which source system should be Priority 2?" (options = remaining source systems)
     - Repeat until one source system remains (it is automatically the lowest priority — no question needed for the last one).
   - For 2–6 source systems this is a small number of questions; batch them using multiple questions in a single `ask_user_question` call when the tool allows it, rather than one question per turn.
   - Optionally allow a per-attribute override in the same flow (e.g., "Should any attribute use a different priority order than the default, such as EMAIL always preferring a verified source regardless of system priority?") — capture this as an exception list rather than changing the base priority order.

3. **Persist the confirmed priorities.**
   - Write the confirmed order to `seeds/mdm_source_priority.csv` with columns `source_system,priority` (1 = highest priority).
   - If a live `MDM_CONTROL.SOURCE_PRIORITY` table is in use instead of a seed, generate the corresponding `INSERT`/`MERGE` statements instead of a CSV.
   - Record any attribute-level exceptions separately (e.g., `seeds/mdm_attribute_rules.csv`) rather than embedding them only in prose.

4. **Never silently regenerate or reorder priorities on a later run.** If new source systems are discovered afterward, re-run Step 0.4 for the new systems only — do not re-ask for systems whose priority was already confirmed.

Do not proceed to Section 1 (Architecture) or generate any DDL/dbt code until either:

- an existing raw table has been inspected and its schema confirmed (Step 0.2), or
- a sample file or explicit file structure has been provided for each source system (Step 0.3).

Survivorship priority (Step 0.4) must be confirmed before Section 13 (Survivorship) models or the `mdm_source_priority` seed are generated, but may be resolved after Step 0.2/0.3 once the source system list is known.

If the user wants to proceed with placeholders anyway, explicitly confirm that the generated source contract is a placeholder and must be revisited once real files/tables are available — never present placeholder columns as if they were confirmed.

---

# 1. Architecture

Use this logical architecture:

AWS S3
  |
  | CRM / ERP / Stripe / other files
  v
Snowflake External Stage
  |
  v
File Manifest / Ingestion Control
  |
  v
RAW landing tables
  |
  v
Streams / CDC detection
  |
  v
Canonical staging
  |
  v
Candidate generation
  |
  v
Match scoring
  |
  v
Match decision
  |
  +------------------------------+
  |                              |
  v                              v
ENTITY_IDENTITY              SUSPECT_QUEUE
  |
  +-------------------+
  |                   |
  v                   v
CROSSWALK         ATTRIBUTE LINEAGE
                      |
                      v
                 SURVIVORSHIP
                      |
                      v
                GOLDEN_CUSTOMER

dbt:
- transformations
- tests
- documentation
- reconciliation
- snapshots/history
- deterministic models

Snowflake:
- stages
- file formats
- COPY/Snowpipe-style ingestion
- streams
- tasks
- stored procedures where atomic identity assignment is required

Airflow:
- operational orchestration
- file/backfill controls
- dbt build
- reconciliation
- alerting
- replay
- not the per-record real-time processing engine


# 2. Required Project Structure

Generate this dbt project structure:

models/
  staging/
    crm/
      stg_crm_customer.sql
    erp/
      stg_erp_customer.sql
    stripe/
      stg_stripe_customer.sql
    stg_customer.sql

  intermediate/
    int_customer_normalized.sql
    int_customer_match_candidates.sql
    int_customer_match_scores.sql
    int_customer_match_decisions.sql
    int_customer_entity_assignments.sql
    int_customer_attribute_candidates.sql

  marts/
    mdm/
      customer_identity.sql
      customer_crosswalk.sql
      customer_attribute_lineage.sql
      customer_golden.sql
      customer_suspect_queue.sql
      # NOTE: customer_merge_history and customer_unmerge_history are NOT dbt models —
      # see Section 15 "Implementation note: these are not dbt models". They are plain
      # DDL tables created alongside the control tables (Section 4), written to only
      # by explicit merge/unmerge operations.

  audit/
    customer_reconciliation.sql
    customer_pipeline_audit.sql

snapshots/
  customer_golden_snapshot.sql

tests/
  assert_no_duplicate_active_crosswalk.sql
  assert_one_active_golden_per_source_record.sql
  assert_no_duplicate_golden_ids.sql
  assert_identity_crosswalk_consistency.sql

macros/
  normalize_email.sql
  normalize_phone.sql
  normalize_name.sql
  generate_source_record_id.sql
  generate_match_key.sql
  generate_golden_id.sql
  safe_hash.sql
  generate_schema_name.sql

seeds/
  mdm_source_priority.csv
  mdm_match_thresholds.csv
  mdm_attribute_rules.csv

analyses/
  mdm_reconciliation.sql

config/
  profiles example only; never hardcode credentials


# 3. Snowflake Object Layers

Before running any `CREATE DATABASE`/`CREATE SCHEMA` DDL, check that the active role actually has the privilege — the session's reported/default role (e.g., from workspace context) is not guaranteed to have `CREATE DATABASE` on the account. If a `CREATE DATABASE` or `CREATE SCHEMA` statement fails with an insufficient-privileges error:

- Try `USE ROLE ACCOUNTADMIN;` (or another role the user confirms has the privilege) and retry once.
- If that also fails, stop and ask the user which role/database to use — do not silently fall back to a different database than what was confirmed, and do not keep retrying roles speculatively.

Create these databases/schemas as configurable variables:

RAW database/schema:
  MDM_RAW

STAGING:
  MDM_STG

INTERMEDIATE:
  MDM_INT

MASTER / MART:
  MDM

CONTROL:
  MDM_CONTROL

AUDIT:
  MDM_AUDIT

## dbt custom schema naming

When the dbt project models are configured with `+schema:` overrides to land in these exact schema names (e.g. `+schema: MDM_STG`), dbt's default `generate_schema_name` macro will prefix them as `<target_schema>_<custom_schema>` instead of using the schema name exactly as given. Always add a `macros/generate_schema_name.sql` override so custom schemas resolve to exactly the configured name:

```sql
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
```

Without this override, models will land in unexpected schemas (e.g. `MDM_MDM_STG`) and downstream `source()`/`ref()` lookups against the intended `MDM_RAW`/`MDM_STG`/etc. schemas will be confusing to debug.

All names must be configurable through dbt vars/environment variables.

Never hardcode AWS credentials in SQL or dbt files.

Use a Snowflake storage integration for S3 access.

Example object pattern:

CREATE STORAGE INTEGRATION <integration_name>
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = S3
  ENABLED = TRUE
  STORAGE_AWS_ROLE_ARN = '<role>';

CREATE FILE FORMAT <database>.<schema>.FF_MDM_JSON
  TYPE = JSON
  STRIP_OUTER_ARRAY = FALSE;

CREATE STAGE <database>.<schema>.STG_S3_MDM
  URL = 's3://<bucket>/<prefix>/'
  STORAGE_INTEGRATION = <integration_name>
  FILE_FORMAT = <file_format>;

Do not put secret keys into the repository.


# 4. File Manifest / Ingestion Control

Create:

MDM_CONTROL.FILE_MANIFEST

Columns:

FILE_ID
SOURCE_SYSTEM
S3_PATH
FILE_NAME
FILE_SIZE
FILE_LAST_MODIFIED
FILE_CONTENT_HASH
LOAD_STATUS
RECORD_COUNT
FIRST_SEEN_TS
LOAD_START_TS
LOAD_END_TS
ERROR_MESSAGE
RAW_TABLE
REPLAY_COUNT
IS_CURRENT
CREATED_TS
UPDATED_TS

Use a deterministic FILE_ID, preferably a hash of:

SOURCE_SYSTEM + S3_PATH + FILE_SIZE + LAST_MODIFIED

If an upstream system supplies a true immutable file/event ID, prefer that.

Statuses:

DISCOVERED
LOADING
LOADED
VALIDATED
FAILED
QUARANTINED
REPLAYED

The manifest is the idempotency boundary for file ingestion.

Never use Airflow execution timestamp as the identity of a source file.


# 5. Raw Landing

Create a raw envelope table:

MDM_RAW.CUSTOMER_FILE_RAW

Columns:

FILE_ID
SOURCE_SYSTEM
SOURCE_RECORD_ID
RAW_PAYLOAD VARIANT
SOURCE_EVENT_TS
INGESTION_TS
FILE_NAME
S3_PATH
ROW_HASH
RECORD_STATUS
CREATED_TS

Keep the original payload whenever practical.

Source-specific tables may also be generated:

MDM_RAW.CRM_CUSTOMER_RAW
MDM_RAW.ERP_CUSTOMER_RAW
MDM_RAW.STRIPE_CUSTOMER_RAW

The raw layer must be append-oriented and auditable.

Do not overwrite raw source records.

Use ROW_HASH for duplicate detection.


# 6. Canonical Customer Model

Normalize all source systems into a canonical structure:

SOURCE_SYSTEM
SOURCE_RECORD_ID
SOURCE_EVENT_ID
FIRST_NAME
LAST_NAME
FULL_NAME
EMAIL
PHONE
DATE_OF_BIRTH
ADDRESS_LINE_1
ADDRESS_LINE_2
CITY
STATE
POSTAL_CODE
COUNTRY
CUSTOMER_STATUS
SOURCE_UPDATED_TS
INGESTION_TS
FILE_ID
ROW_HASH

Also create normalized fields:

EMAIL_NORM
PHONE_NORM
NAME_NORM
NAME_DOB_KEY
PHONE_HASH
EMAIL_HASH
NAME_DOB_HASH
ADDRESS_HASH

Use macros for normalization.

Examples:

Email:
- trim
- lowercase
- remove surrounding whitespace
- do NOT remove dots/plus-tags unless a business rule explicitly enables it

Phone:
- remove punctuation
- normalize country code when country is known
- preserve a canonical E.164 representation where possible

Name:
- uppercase/lowercase consistently
- trim
- collapse whitespace
- remove configurable punctuation
- use transliteration only if explicitly configured

Do not silently implement aggressive transformations that can cause false matches.


# 7. Match Candidate Generation

Never perform an unrestricted Cartesian comparison of every customer.

Generate candidates using blocking keys:

1. exact normalized email
2. exact normalized phone
3. exact name + date of birth
4. exact postal code + normalized surname
5. configurable address key
6. optional fuzzy candidate generation only after blocking

Create:

MDM_INT.INT_CUSTOMER_MATCH_CANDIDATES

Columns:

LEFT_SOURCE_RECORD_ID
RIGHT_SOURCE_RECORD_ID
LEFT_SOURCE_SYSTEM
RIGHT_SOURCE_SYSTEM
CANDIDATE_KEY
CANDIDATE_RULE
CREATED_TS

Exclude self-matches.

Avoid duplicate A-B/B-A pairs by canonicalizing:

LEAST(source_record_id_a, source_record_id_b)
GREATEST(source_record_id_a, source_record_id_b)


# 8. Match Scoring

Create:

MDM_INT.INT_CUSTOMER_MATCH_SCORES

Calculate attribute-level evidence:

EMAIL_MATCH
PHONE_MATCH
NAME_MATCH
DOB_MATCH
ADDRESS_MATCH

Then calculate configurable score.

Do not hardcode thresholds in multiple models.

Use:

MDM_CONTROL.MATCH_RULES

Example configuration:

RULE_NAME | WEIGHT | AUTO_MATCH_MIN | SUSPECT_MIN
EMAIL_EXACT | 100 | 100 | 90
PHONE_EXACT | 90 | 95 | 80
NAME_DOB | 80 | 90 | 75
MULTI_ATTRIBUTE | configurable | configurable | configurable

The actual rule engine may combine evidence.

Output:

MATCH_SCORE
MATCH_RULE
MATCH_EXPLANATION
MATCH_STATUS

Statuses:

AUTO_MATCH
SUSPECT
NO_MATCH

Persist the rule version:

MATCH_RULE_VERSION


# 9. Match Decision

Create:

MDM_INT.INT_CUSTOMER_MATCH_DECISIONS

Every decision must be auditable.

Columns:

MATCH_ID
SOURCE_RECORD_ID_A
SOURCE_RECORD_ID_B
MATCH_SCORE
MATCH_STATUS
MATCH_RULE
MATCH_RULE_VERSION
MATCH_EXPLANATION
DECISION_TS
DECISION_ENGINE_VERSION

AUTO_MATCH means the identity engine may automatically connect entities.

SUSPECT must NOT automatically merge entities.

NO_MATCH may be retained for audit, but avoid storing unlimited low-value comparisons.


# 10. Persistent Entity Identity

This is the most important MDM object.

Create:

MDM.CUSTOMER_IDENTITY

Columns:

ENTITY_ID
GOLDEN_ID
SOURCE_SYSTEM
SOURCE_RECORD_ID
SOURCE_CUSTOMER_ID
IDENTITY_STATUS
MATCH_ID
MATCH_RULE
MATCH_SCORE
MATCH_RULE_VERSION
FIRST_SEEN_TS
LAST_SEEN_TS
ACTIVE_FROM_TS
ACTIVE_TO_TS
IS_CURRENT
CREATED_TS
UPDATED_TS

Constraints:

- One source_system + source_record_id may have only one active Golden ID.
- Golden ID must remain stable across future source arrivals.
- Identity assignment must be idempotent.
- Concurrent events must not create duplicate Golden IDs.

Use a deterministic candidate identity key where possible.

For first-time entities, generate a UUID or deterministic hash-backed ID.

Do not regenerate Golden IDs on every dbt run.

For true real-time atomic assignment, use a Snowflake transaction/procedure/controlled merge mechanism rather than relying solely on a dbt SELECT.

## Entity assignment must resolve transitive matches, not just direct pairs

A naive approach that assigns a golden_id by joining directly on a single blocking key (email, phone, or name+DOB) will miss valid transitive matches: e.g., source A matches source B on NAME_DOB, and source B matches source C on EMAIL_EXACT, but A and C share no attribute directly (different email, no DOB on C). Treating A/B and B/C as two separate entities — or worse, only linking whichever pair was joined last — produces incorrect, order-dependent Golden IDs.

Entity assignment must instead compute **connected components over AUTO_MATCH edges only** (never SUSPECT or NO_MATCH edges — SUSPECT must not influence entity assignment, per rule #9):

1. Build a symmetric edge list from `int_customer_match_decisions` filtered to `match_status = 'AUTO_MATCH'`, using `source_system || ':' || source_record_id` as node identifiers.
2. Run bounded label propagation: start each node's label as itself, then repeatedly set each node's label to `min(self_label, min(neighbor_labels))` for a number of rounds >= the number of source systems being mastered (e.g., 3 rounds for 3 source systems is enough to converge any chain up to length 3). This can be done with plain self-joins/aggregation in a dbt model — Snowflake also supports `WITH RECURSIVE` CTEs if a variable/unbounded number of source systems requires it instead of a fixed round count.
3. The final converged label is the entity key; derive `golden_id` deterministically from it (e.g., a hash of the label), so it is stable across runs as long as the same records keep matching.
4. Validate this logic against a test case with an intentional transitive chain (A-B match on one rule, B-C match on a different rule, A-C matching on neither) before considering entity assignment complete — do not assume single-hop blocking joins are sufficient.


# 11. Crosswalk

Create:

MDM.CUSTOMER_CROSSWALK

The Crosswalk is the Reltio-like source-to-Golden lineage.

Columns:

GOLDEN_ID
ENTITY_ID
SOURCE_SYSTEM
SOURCE_RECORD_ID
SOURCE_CUSTOMER_ID
SOURCE_EVENT_ID
FILE_ID
MATCH_ID
MATCH_RULE
MATCH_SCORE
MATCH_STATUS
FIRST_SEEN_TS
LAST_SEEN_TS
ACTIVE_FROM_TS
ACTIVE_TO_TS
IS_CURRENT
CREATED_TS
UPDATED_TS

Business rule:

GOLDEN_ID + SOURCE_SYSTEM + SOURCE_RECORD_ID identifies the active source relationship.

Reverse lookup must be supported:

source_system + source_record_id -> golden_id

Golden-to-source lookup must also be supported:

golden_id -> all active source records

The Crosswalk must never be derived only from the current Golden attributes.
It must preserve source lineage independently of survivorship.


# 12. Attribute-Level Lineage

Create:

MDM.CUSTOMER_ATTRIBUTE_LINEAGE

Columns:

GOLDEN_ID
ATTRIBUTE_NAME
ATTRIBUTE_VALUE
ATTRIBUTE_HASH
SOURCE_SYSTEM
SOURCE_RECORD_ID
SOURCE_EVENT_ID
SOURCE_UPDATED_TS
SOURCE_PRIORITY
SURVIVORSHIP_RULE
SURVIVORSHIP_RULE_VERSION
IS_SURVIVOR
ACTIVE_FROM_TS
ACTIVE_TO_TS
IS_CURRENT
CREATED_TS
UPDATED_TS

Every mastered attribute must be traceable to its winning source.

This allows the system to answer:

"Why does the Golden Customer contain this value?"


# 13. Survivorship

Create configurable source priority:

MDM_CONTROL.SOURCE_PRIORITY

Priority must come from Step 0.4 (Confirm Survivorship Source Priority), not be invented here. If Step 0.4 has not yet been completed for the source systems present in the raw data, stop and complete it first — check for an existing `seeds/mdm_source_priority.csv`/`SOURCE_PRIORITY` table, and if none exists, collect priorities from the user via the checkbox-style `ask_user_question` flow described in Step 0.4.

Illustrative example only (do not treat as a default):

SOURCE_SYSTEM | PRIORITY
CRM            | 1
ERP            | 2
STRIPE         | 3

Do not assume the above order is correct; it must be confirmed configuration from Step 0.4.

Survivorship must be attribute-specific.

Example:

EMAIL:
- prefer verified source
- then source priority
- then most recent valid value

PHONE:
- prefer verified source
- then source priority
- then most recent valid value

NAME:
- source priority
- then recency

ADDRESS:
- verified flag
- then source priority
- then recency

Use QUALIFY ROW_NUMBER() or equivalent deterministic ranking.

Every ranking must have a stable tie-breaker such as source event ID or record hash.
Never allow nondeterministic ordering.


# 14. Golden Customer

Create:

MDM.CUSTOMER_GOLDEN

Columns:

GOLDEN_ID
ENTITY_ID
FIRST_NAME
LAST_NAME
FULL_NAME
EMAIL
PHONE
DATE_OF_BIRTH
ADDRESS_LINE_1
ADDRESS_LINE_2
CITY
STATE
POSTAL_CODE
COUNTRY
CUSTOMER_STATUS
EMAIL_SOURCE_SYSTEM
PHONE_SOURCE_SYSTEM
NAME_SOURCE_SYSTEM
ADDRESS_SOURCE_SYSTEM
GOLDEN_VERSION
RECORD_STATUS
CREATED_TS
UPDATED_TS

Golden record must be assembled from attribute-level survivorship, not by simply choosing one source row.

The Golden record must not overwrite source history.


# 15. Merge and Unmerge

Create:

MDM.CUSTOMER_MERGE_HISTORY

Columns:

MERGE_ID
FROM_GOLDEN_ID
TO_GOLDEN_ID
REASON
MATCH_ID
MATCH_RULE
MATCH_RULE_VERSION
PERFORMED_BY
PERFORMED_TS
STATUS

Create:

MDM.CUSTOMER_UNMERGE_HISTORY

Columns:

UNMERGE_ID
GOLDEN_ID
RESTORED_GOLDEN_ID
SOURCE_RECORD_ID
REASON
PERFORMED_BY
PERFORMED_TS
STATUS

Never physically destroy merge history.

When two Golden entities merge:

G10002 -> G10001

retain the historical mapping.

Unmerge must be possible by restoring the appropriate identity/crosswalk relationships.

## Implementation note: these are not dbt models

`CUSTOMER_MERGE_HISTORY` and `CUSTOMER_UNMERGE_HISTORY` must be created as plain persistent DDL tables (`CREATE TABLE IF NOT EXISTS ...`), never as dbt models materialized as `table` or `view`. A dbt `table` materialization is a `CREATE OR REPLACE TABLE ... AS SELECT`, which rebuilds — and silently wipes — the table on every `dbt run`. These tables are append-only audit logs written to exclusively by explicit merge/unmerge procedures/operations, not derived from a repeatable SELECT, so dbt must never own their lifecycle. Generate the DDL alongside the other control/audit tables (Section 4) rather than under `models/marts/mdm/`.


# 16. Suspect Queue

Create:

MDM.CUSTOMER_SUSPECT_QUEUE

Columns:

SUSPECT_ID
SOURCE_RECORD_ID_A
SOURCE_RECORD_ID_B
MATCH_SCORE
MATCH_RULE
MATCH_RULE_VERSION
MATCH_EXPLANATION
STATUS
ASSIGNED_TO
DECISION
DECISION_REASON
CREATED_TS
UPDATED_TS

Statuses:

OPEN
IN_REVIEW
APPROVED
REJECTED
MERGED

Human-approved decisions must be persisted and take precedence over automatic decisions when configured.


# 17. Snowflake Streams and Tasks

For near-real-time processing, create Streams over raw/source tables.

Example pattern:

CREATE STREAM MDM_RAW.CUSTOMER_RAW_STREAM
ON TABLE MDM_RAW.CUSTOMER_FILE_RAW;

Create Tasks that process only when:

SYSTEM$STREAM_HAS_DATA('<stream_name>')

Tasks should be small and idempotent.

Suggested chain:

RAW_STREAM
  -> NORMALIZE_TASK
  -> MATCH_TASK
  -> IDENTITY_TASK
  -> CROSSWALK_TASK
  -> SURVIVORSHIP_TASK
  -> GOLDEN_TASK

Where practical, combine tasks to reduce operational complexity.

Do not create an excessive number of one-minute tasks without considering warehouse cost and throughput.

Use a dedicated warehouse sized for the expected event rate.

Use Streams for change capture and Tasks for scheduled/conditional execution.

For atomic identity assignment or merge/unmerge, use a Snowflake stored procedure or transactional SQL where required.


# 18. File Ingestion from S3

The source files may arrive at different times.

Examples:

s3://bucket/crm/...
s3://bucket/erp/...
s3://bucket/stripe/...

Do NOT require all sources to arrive before processing.

Each file is independently:

1. discovered
2. registered in FILE_MANIFEST
3. loaded
4. validated
5. normalized
6. matched
7. mastered

A late ERP file can be processed independently.

The system must support replaying one file without rebuilding unrelated source data.

For bulk ingestion, generate Snowflake COPY INTO commands or Snowpipe/Snowpipe Streaming-compatible patterns as appropriate.

For simple file-driven near-real-time requirements, prefer Snowpipe/auto-ingest plus Streams.



# 19. File Idempotency

Never process a file twice simply because the task was retried.

Use FILE_MANIFEST and deterministic file identity.

Before loading:

if FILE_ID exists with LOAD_STATUS in:
  LOADED
  VALIDATED
  REPLAYED

then skip unless explicitly requested as a replay.

For replay:

increment REPLAY_COUNT and retain original lineage.

Never delete the original audit record.


# 20. dbt Incremental Strategy

Use incremental models for large event-driven datasets.

Prefer:

incremental_strategy='merge'

with a real unique_key.

Never use a timestamp alone as the unique key.

Examples:

RAW_EVENT unique key:
SOURCE_SYSTEM + SOURCE_EVENT_ID

Source record:
SOURCE_SYSTEM + SOURCE_RECORD_ID

Crosswalk:
SOURCE_SYSTEM + SOURCE_RECORD_ID + IS_CURRENT semantics

Match:
canonical ordered pair + match rule version

Attribute lineage:
GOLDEN_ID + ATTRIBUTE_NAME + SOURCE_RECORD_ID + SOURCE_UPDATED_TS or an event identity

All keys must be deterministic.


# 21. dbt Tests

Generate schema tests for:

- not_null SOURCE_SYSTEM
- not_null SOURCE_RECORD_ID
- unique source event IDs
- accepted match statuses
- accepted identity statuses
- accepted crosswalk statuses

Generate custom tests:

1. No duplicate active source identity

select source_system, source_record_id
from {{ ref('customer_identity') }}
where is_current
group by 1,2
having count(distinct golden_id) > 1

2. Crosswalk must resolve to identity

Every active crosswalk row must have a corresponding active identity row.

3. No duplicate Golden attribute survivors

golden_id + attribute_name should have max one IS_SURVIVOR=true.

4. No orphan golden records

Every active Golden ID should have at least one active crosswalk.

5. No active source record without a Golden ID.

6. No duplicate current Golden IDs.

7. Match rule version must be populated.

8. Golden IDs must not change for an existing source record unless a merge/unmerge operation explicitly caused the change.


# 22. Reconciliation

Create a daily or hourly reconciliation model, even though mastering is near-real-time.

It must compare:

RAW source records
vs
identity
vs
crosswalk
vs
golden

Report:

ORPHAN_SOURCE
ORPHAN_CROSSWALK
ORPHAN_GOLDEN
DUPLICATE_SOURCE_ID
MULTIPLE_GOLDEN_IDS
MISSING_ATTRIBUTE_LINEAGE
INVALID_MATCH
STALE_PROCESSING
FAILED_FILE
UNPROCESSED_EVENT

This is a safety net for the real-time path.

Airflow should execute reconciliation and alert on failures.


# 23. Airflow Integration

Airflow should NOT coordinate every customer event.

Use Airflow for:

- initial source setup
- scheduled reconciliation
- backfills
- replay
- failed file recovery
- dbt build/test
- documentation generation
- monitoring
- operational alerts

Example DAG:

start
 |
 +--> check S3/file manifest
 |
 +--> trigger or monitor ingestion
 |
 +--> dbt build --select staging/intermediate/marts
 |
 +--> dbt test
 |
 +--> reconciliation
 |
 +--> alert

For a replay:

airflow dag_run.conf:
{
  "file_id": "...",
  "source_system": "CRM",
  "replay": true
}

Do not use a global batch dependency for real-time mastering.


# 24. Security

Never put these into dbt source code:

AWS access key
AWS secret key
Snowflake password
private key
tokens

Use:

Snowflake storage integration
Snowflake secrets/integration where appropriate
IAM roles
AWS external ID
environment variables for non-secret configuration
Airflow connection/secret backends

Use least privilege.

Separate permissions for:

RAW ingestion
MDM processing
dbt transformation
read-only consumers
administration


# 25. Observability

Create:

MDM_CONTROL.PROCESSING_AUDIT

Columns:

RUN_ID
PROCESS_NAME
SOURCE_SYSTEM
FILE_ID
SOURCE_RECORD_ID
EVENT_ID
STATUS
START_TS
END_TS
ROWS_READ
ROWS_WRITTEN
ERROR_COUNT
ERROR_MESSAGE
RULE_VERSION
ENGINE_VERSION

Every real-time processing step should be traceable.

Recommended dashboards:

- files waiting
- files failed
- events waiting
- matches per minute
- auto-match rate
- suspect rate
- new Golden IDs
- merge count
- unmerge count
- duplicate identity violations
- processing latency
- reconciliation failures


# 26. Configuration

Generate dbt vars similar to:

vars:
  mdm_raw_database: MDM
  mdm_raw_schema: MDM_RAW
  mdm_stg_schema: MDM_STG
  mdm_int_schema: MDM_INT
  mdm_schema: MDM
  mdm_control_schema: MDM_CONTROL
  mdm_audit_schema: MDM_AUDIT

  default_match_auto_threshold: 95
  default_match_suspect_threshold: 75

  enable_fuzzy_matching: false

  source_systems:
    - CRM
    - ERP
    - STRIPE

All source-specific behavior must be configuration-driven where practical.


# 27. Source File Contract

Assume files can differ by source.

CRM may provide:
customer_id
first_name
last_name
email
phone
dob
address

ERP may provide:
customer_number
name
email_address
telephone
birth_date
billing_address

Stripe may provide:
customer_id
name
email
phone
metadata

Create source-specific staging models that map each source into the canonical customer model.

Do not force the raw source schema to be identical.

Raw preserves source fidelity.
Staging creates canonical semantics.


# 28. Required Generated Deliverables

When this skill is used, generate:

1. dbt_project.yml
2. models.yml/schema.yml files
3. source definitions
4. all staging SQL
5. all intermediate SQL
6. all MDM mart SQL
7. macros
8. seeds/configuration CSVs
9. custom dbt tests
10. Snowflake DDL for:
   - databases/schemas
   - storage integration template
   - external stage
   - file formats
   - raw tables
   - streams
   - tasks
   - control tables
11. Snowflake stored procedure templates where transactional identity assignment is needed
12. Airflow DAG template
13. README with deployment order
14. sample CRM/ERP/Stripe files
15. sample expected Golden/Crosswalk outputs
16. runbook for replay, merge, unmerge, and recovery

Never invent AWS bucket names, IAM role ARNs, Snowflake account identifiers, or credentials. Use clearly marked variables/placeholders.


# 29. Deployment Order

Always provide deployment steps in this order:

0. Discovery: confirm whether raw tables already exist (Section 0.2) or collect file structure/sample files per source system (Section 0.3), then confirm survivorship source priority (Section 0.4)
1. Create Snowflake databases/schemas
2. Create storage integration
3. Create file formats
4. Create external stages
5. Create control tables
6. Create raw tables
7. Configure file ingestion
8. Create dbt sources
9. Deploy staging models
10. Deploy matching models
11. Deploy identity/crosswalk
12. Deploy survivorship/golden
13. Deploy Streams
14. Deploy Tasks/procedures
15. Deploy dbt tests
16. Deploy reconciliation
17. Deploy Airflow
18. Run initial backfill
19. Validate Golden/Crosswalk counts
20. Enable near-real-time processing

Never enable automatic ingestion before the control and audit tables are deployed.
Never skip Step 0 — raw table existence, source file contracts, and survivorship source priority must all be confirmed before any DDL or survivorship model is generated.


# 30. Important Design Rules

1. No synchronized source batch is required for real-time processing.
2. Every source file/event has immutable lineage.
3. Raw data is append-oriented.
4. File/event processing is idempotent.
5. Golden IDs persist across source arrivals.
6. Crosswalk is independent of survivorship.
7. Attribute lineage explains every Golden value.
8. Match decisions are versioned.
9. Suspect matches never auto-merge unless explicitly configured.
10. Merge and unmerge are auditable.
11. Real-time processing uses Snowflake Streams/Tasks or an equivalent event mechanism.
12. dbt is used for transformations, tests, governance, and reconciliation—not as the per-event scheduler.
13. Airflow handles operational orchestration, replay, backfill, and reconciliation.
14. Do not compare every source record against every Golden record.
15. Candidate generation must use blocking keys.
16. Matching and identity assignment must be deterministic.
17. Concurrent identity creation must be protected against duplicate Golden IDs.
18. Never use CURRENT_TIMESTAMP as a business identity.
19. Never hardcode secrets.
20. Always provide a reconciliation path.
21. Never assume raw table schemas or source file formats — confirm existing raw tables (Section 0.2) or collect sample files/file structure (Section 0.3) before designing staging, matching, or DDL.
22. Never assume survivorship source priority — check for an existing `mdm_source_priority` seed/table first, and if none exists, collect priority order from the user via the checkbox-style question flow in Step 0.4 before generating survivorship models.
23. Never assume `COPY FILES`/`LIST` against the workspace live version will see a chat-uploaded attachment — read it directly and, for small files, load raw tables via generated `INSERT` statements instead (Step 0.3a).
24. Never assume the session's default/reported role has `CREATE DATABASE`/`CREATE SCHEMA` privilege — check, retry with an explicitly confirmed role on failure, and ask the user rather than guessing further (Section 3).
25. Never rely on single-hop blocking-key joins alone for entity assignment — compute connected components over AUTO_MATCH edges (bounded label propagation or equivalent) so transitive matches across more than 2 source systems are resolved correctly (Section 10).
26. Never materialize `customer_merge_history`/`customer_unmerge_history` as dbt models — they are append-only audit tables created via plain DDL and must never be rebuilt by `dbt run` (Section 15).
27. Always add a `generate_schema_name` macro override when using the Section 3 multi-schema layout with `+schema:` configs, or models will land in incorrectly prefixed schemas.


# 31. Expected End-to-End Example

Input files:

s3://bucket/crm/customer_001.json
s3://bucket/erp/customer_834.json
s3://bucket/stripe/customer_912.json

CRM:
1001, John Smith, john@gmail.com, 1111111111

ERP:
E5001, Jon Smith, john@gmail.com, 1111111111

Stripe:
cus_abc, John Smith, john@gmail.com

Processing:

CRM -> source identity CRM:1001
CRM -> no candidate -> create G10001

ERP -> candidate CRM:1001
ERP -> exact email + phone
ERP -> AUTO_MATCH
ERP -> G10001

Stripe -> candidate G10001
Stripe -> exact email
Stripe -> AUTO_MATCH
Stripe -> G10001

Crosswalk:

G10001 | CRM    | 1001
G10001 | ERP    | E5001
G10001 | STRIPE | cus_abc

Survivorship:

EMAIL:
  john@gmail.com
  source = CRM or configured highest-priority verified source

PHONE:
  1111111111
  source = CRM/ERP according to configured priority

NAME:
  John Smith
  source = CRM/Stripe according to configured priority

Golden:

G10001
John Smith
john@gmail.com
1111111111

The output must also preserve the lineage showing which source supplied each Golden attribute.

Note: in this example ERP directly matches both CRM (email+phone) and Stripe (email), so a single-hop join happens to find every pair. When a chain instead relies on transitive matching (e.g. A matches B on NAME_DOB only, B matches C on EMAIL_EXACT only, A and C share nothing directly), a single-hop join will NOT connect A and C — this is exactly the connected-components requirement described in Section 10.


# 32. Implementation Preference

Prefer simple, explicit SQL over opaque abstractions.

Use dbt refs and sources consistently.

Use macros only when they materially improve reuse.

Use incremental models for large tables.

Use Snowflake native features where they provide correctness or performance.

When a requirement cannot be safely implemented as a pure dbt model—especially concurrent identity assignment, transactional merge/unmerge, or event-driven execution—say so explicitly and generate the appropriate Snowflake procedure/task implementation instead of pretending a SELECT statement is sufficient.

The generated implementation must be production-oriented, idempotent, auditable, replayable, and configurable.

# MDM dbt Project (mdm_dbt_project)

Customer MDM pipeline built from 3 uploaded source files (CRM, ERP, Stripe) per the `snf-dbt-mdm` skill.

## Discovery (Step 0)

- **Step 0.1/0.3**: No existing raw tables — started from 3 sample CSV files (`crm_customers.csv`, `erp_customers.csv`, `stripe_customers.csv`). Field structures inspected directly (see `models/staging/*/stg_*.sql` for the exact source-to-canonical mapping).
- **Step 0.4**: Survivorship source priority confirmed interactively: **CRM = 1 (highest), ERP = 2, STRIPE = 3**. Stored in `seeds/mdm_source_priority.csv` and mirrored in `MDM_CONTROL.SOURCE_PRIORITY`.

## Adaptations from the full skill template

- Files were uploaded directly (no S3/external stage available), so raw tables were loaded via direct `INSERT` rather than `COPY INTO` from an S3 stage. `MDM_CONTROL.FILE_MANIFEST` still tracks each load for idempotency/audit.
- This is a one-time batch load, not a continuous event stream, so Snowflake Streams/Tasks for real-time processing and Airflow orchestration were not deployed (no continuous ingestion to react to). The architecture supports adding them later if files start arriving on a recurring basis.
- `customer_merge_history` / `customer_unmerge_history` are plain DDL tables (not dbt models) since they must persist across `dbt run` and are only written to by explicit merge/unmerge operations — a dbt table materialization would wipe them every run.

## Deployment order followed

0. Discovery — confirmed no raw tables existed, inspected sample files, confirmed survivorship priority
1. Created `MDM_DEMO` database with schemas: `MDM_RAW`, `MDM_STG`, `MDM_INT`, `MDM`, `MDM_CONTROL`, `MDM_AUDIT`
2. Created control tables: `FILE_MANIFEST`, `SOURCE_PRIORITY`, `MATCH_RULES`
3. Created source-specific raw tables: `CRM_CUSTOMER_RAW`, `ERP_CUSTOMER_RAW`, `STRIPE_CUSTOMER_RAW`
4. Loaded all 3 files and registered them in `FILE_MANIFEST`
5. Deployed dbt sources + staging models (per-source, then unified `stg_customer`)
6. Deployed intermediate layer: normalization → match candidates (blocking) → match scores → match decisions → entity assignment (connected-component label propagation over AUTO_MATCH edges)
7. Deployed MDM marts: `customer_identity`, `customer_crosswalk`, `customer_attribute_lineage`, `customer_golden`, `customer_suspect_queue`
8. Deployed merge/unmerge history tables (DDL only)
9. Deployed audit layer: `customer_reconciliation`, `customer_pipeline_audit`
10. Deployed dbt tests (schema + custom singular tests) — `dbt test`: 19/19 pass
11. Deployed golden customer snapshot for history tracking — `dbt snapshot`

## Result summary

- 33 source records (10 CRM + 11 ERP + 12 Stripe) resolved to **13 golden customers**
- 9 customers matched cleanly across all 3 systems (email or phone exact match)
- Emily Davis: ERP+Stripe auto-matched on email; CRM record correctly held in the **suspect queue** (NAME_DOB score below auto-match threshold) rather than being force-merged
- Stripe's decoy record (`cus_012`, same name as `cus_001` but different email/phone) correctly resolved as its own standalone golden entity
- `customer_reconciliation` returned 0 issues (no orphans, no duplicate golden IDs, no multi-golden source records)

## Running

```
dbt seed --project-dir mdm_dbt_project
dbt run --project-dir mdm_dbt_project
dbt test --project-dir mdm_dbt_project
dbt snapshot --project-dir mdm_dbt_project
```

## Scaling this to an enterprise-level MDM dataset with `/snf-dbt-mdm`

This project is a small, fully-worked example (3 files, ~33 records) demonstrating every layer the skill produces. The same `/snf-dbt-mdm` skill scales to enterprise datasets — many more source systems, millions of records, continuously arriving files — but a few things change in how you invoke it and what you should expect it to build. Re-invoke the skill (`/snf-dbt-mdm`) for any of the following rather than hand-extending this project ad hoc.

### 1. More source systems, more discovery rounds

Nothing about Step 0 changes conceptually, but with 5–10+ source systems (additional CRMs from acquisitions, regional ERPs, support/ticketing systems, marketing platforms, etc.) expect:

- **Step 0.2/0.3 run once per source system.** Provide real raw tables or sample files for each — the skill will not extrapolate a schema for a system it hasn't seen.
- **Step 0.4 priority ranking scales linearly** (N systems → N−1 questions), and per-attribute overrides become more important at this scale — e.g. a verified-email system should outrank a general-priority system specifically for `EMAIL`, even if it's lower priority overall. Capture these in `seeds/mdm_attribute_rules.csv` rather than trying to force one global ranking to work for every attribute.
- If new source systems are onboarded later, only Step 0.4 needs to re-run **for the new systems** — existing priorities are never silently reordered.

### 2. Real ingestion instead of one-time loads

This project loaded 3 uploaded files via direct `INSERT` because there was no S3/stage available (see "Adaptations" above). At enterprise scale, invoke the skill with an actual ingestion source and it will build the real pipeline instead of the simplified path:

- **S3 / external stage ingestion** (Section 1/3/18): storage integration, external stage, file formats, and `COPY INTO`/Snowpipe patterns — request this explicitly if files land in S3, and provide the bucket/prefix convention rather than letting the skill guess one.
- **`FILE_MANIFEST`-driven idempotency** (Section 4/19): at volume, files will be replayed, arrive late, or fail partway — the manifest is what prevents double-processing, not an optional nicety.
- **Snowflake Streams + Tasks** (Section 17): for near-real-time mastering as files/events land continuously, rather than a manual `dbt run` after every load. Ask the skill to deploy the `RAW_STREAM → NORMALIZE_TASK → MATCH_TASK → IDENTITY_TASK → CROSSWALK_TASK → SURVIVORSHIP_TASK → GOLDEN_TASK` chain once ingestion is continuous.
- **Airflow orchestration** (Section 23) for backfills, replay of specific failed files, and scheduled reconciliation/alerting — not for per-record processing.

### 3. Matching at volume: blocking is not optional

At enterprise row counts, a full cross-join between sources is not just slow — it's the difference between a query that finishes and one that times out a warehouse. The skill's match candidate generation (Section 7) already blocks on exact normalized keys (email, phone, name+DOB) rather than comparing every record to every record; at scale, insist on this and:

- Keep blocking keys **indexed/clustered** if row counts move into the tens of millions (ask the skill to add clustering key recommendations for `MDM_INT` tables).
- Enable fuzzy matching (`enable_fuzzy_matching` in `dbt_project.yml` vars) only *after* exact blocking is validated — fuzzy matching without blocking is a full cross-join with extra CPU cost, not a solution.
- Re-validate the connected-component entity assignment (Section 10) against transitive match scenarios at each new source system added — the number of label-propagation rounds needed scales with the longest realistic match chain, not just the source count.

### 4. Operational maturity: suspect queue, merge/unmerge, reconciliation

These matter more, not less, at enterprise scale, because a wrong auto-merge affects far more downstream consumers:

- Staff the **suspect queue** (`customer_suspect_queue`) as an actual review workflow — route it to a BI tool or ticketing system rather than letting it sit unreviewed in Snowflake.
- Exercise **merge/unmerge** (Section 15) deliberately — these are plain DDL tables written to by explicit operations, not dbt models, specifically so a bad merge can be reversed without losing history.
- Run **reconciliation** (Section 22, `customer_reconciliation`) on a schedule (via Airflow or a Snowflake Task) rather than only checking it manually — at enterprise scale, orphaned crosswalk rows or duplicate golden IDs are how you find out a task or stream silently stopped days ago.
- Wire the **observability** tables (Section 25, `MDM_CONTROL.PROCESSING_AUDIT`) into dashboards: auto-match rate, suspect rate, new-golden-ID rate, and reconciliation failures are the four numbers that tell you if the pipeline is healthy.

### 5. What to ask the skill for, concretely

When you're ready to scale beyond this demo, invoke `/snf-dbt-mdm` and describe the delta explicitly, e.g.:

> "We now have 6 source systems landing as JSON files in `s3://our-bucket/mdm/<source>/`, arriving continuously throughout the day. Build the full ingestion pipeline with Streams/Tasks, and help me re-run Step 0.4 to rank the 3 new systems against our existing CRM/ERP/Stripe priorities."

The skill will re-run Discovery for the new systems only, add the S3 stage/storage integration/file format layer, deploy the Streams/Tasks chain, and extend (not replace) the existing survivorship configuration.

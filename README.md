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

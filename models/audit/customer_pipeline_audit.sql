-- Pipeline run audit: row counts at each MDM processing layer for observability
-- Co-authored with CoCo

select 'STAGING' as process_name, source_system, count(*) as rows_written, current_timestamp() as run_ts
from {{ ref('stg_customer') }}
group by source_system

union all

select 'MATCH_CANDIDATES' as process_name, left_source_system as source_system, count(*) as rows_written, current_timestamp() as run_ts
from {{ ref('int_customer_match_candidates') }}
group by left_source_system

union all

select 'MATCH_DECISIONS_AUTO' as process_name, source_record_id_a_system as source_system, count(*) as rows_written, current_timestamp() as run_ts
from {{ ref('int_customer_match_decisions') }}
where match_status = 'AUTO_MATCH'
group by source_record_id_a_system

union all

select 'IDENTITY' as process_name, source_system, count(*) as rows_written, current_timestamp() as run_ts
from {{ ref('customer_identity') }}
group by source_system

union all

select 'GOLDEN' as process_name, 'ALL' as source_system, count(*) as rows_written, current_timestamp() as run_ts
from {{ ref('customer_golden') }}

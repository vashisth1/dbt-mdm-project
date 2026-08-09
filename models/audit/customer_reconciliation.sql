-- Reconciliation model comparing raw, identity, crosswalk, and golden layers for orphan/anomaly detection
-- Co-authored with CoCo

with source_records as (
    select source_system, source_record_id from {{ ref('int_customer_normalized') }}
),

identity_records as (
    select source_system, source_record_id, golden_id, is_current from {{ ref('customer_identity') }}
),

crosswalk_records as (
    select source_system, source_record_id, golden_id from {{ ref('customer_crosswalk') }}
),

golden_records as (
    select golden_id from {{ ref('customer_golden') }}
),

orphan_source as (
    select 'ORPHAN_SOURCE' as issue_type, s.source_system, s.source_record_id, null as golden_id
    from source_records s
    left join identity_records i
        on s.source_system = i.source_system and s.source_record_id = i.source_record_id
    where i.golden_id is null
),

orphan_crosswalk as (
    select 'ORPHAN_CROSSWALK' as issue_type, c.source_system, c.source_record_id, c.golden_id
    from crosswalk_records c
    left join golden_records g on c.golden_id = g.golden_id
    where g.golden_id is null
),

orphan_golden as (
    select 'ORPHAN_GOLDEN' as issue_type, null as source_system, null as source_record_id, g.golden_id
    from golden_records g
    left join crosswalk_records c on g.golden_id = c.golden_id
    where c.golden_id is null
),

duplicate_source_id as (
    select 'DUPLICATE_SOURCE_ID' as issue_type, source_system, source_record_id, null as golden_id
    from source_records
    group by source_system, source_record_id
    having count(*) > 1
),

multiple_golden_ids as (
    select 'MULTIPLE_GOLDEN_IDS' as issue_type, source_system, source_record_id, null as golden_id
    from identity_records
    where is_current = true or is_current is null
    group by source_system, source_record_id
    having count(distinct golden_id) > 1
)

select issue_type, source_system, source_record_id, golden_id, current_timestamp() as checked_ts
from orphan_source
union all
select issue_type, source_system, source_record_id, golden_id, current_timestamp() as checked_ts
from orphan_crosswalk
union all
select issue_type, source_system, source_record_id, golden_id, current_timestamp() as checked_ts
from orphan_golden
union all
select issue_type, source_system, source_record_id, golden_id, current_timestamp() as checked_ts
from duplicate_source_id
union all
select issue_type, source_system, source_record_id, golden_id, current_timestamp() as checked_ts
from multiple_golden_ids

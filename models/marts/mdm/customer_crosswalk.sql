-- Source-to-golden lineage: reverse and forward lookups between golden entities and source records
-- Co-authored with CoCo

select
    golden_id,
    source_system,
    source_record_id,
    true as is_current,
    current_timestamp() as first_seen_ts,
    current_timestamp() as last_seen_ts,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts
from {{ ref('customer_identity') }}

-- Persistent entity identity: one active golden_id per source record
-- Co-authored with CoCo

select
    a.golden_id,
    a.source_system,
    a.source_record_id,
    'ACTIVE' as identity_status,
    n.source_updated_ts as first_seen_ts,
    n.source_updated_ts as last_seen_ts,
    true as is_current,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts
from {{ ref('int_customer_entity_assignments') }} a
join {{ ref('int_customer_normalized') }} n
    on a.source_system = n.source_system and a.source_record_id = n.source_record_id

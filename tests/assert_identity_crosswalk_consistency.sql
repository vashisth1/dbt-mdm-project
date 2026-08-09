-- Fails if any active crosswalk row lacks a corresponding active identity row (or vice versa)
-- Co-authored with CoCo

select c.golden_id, c.source_system, c.source_record_id
from {{ ref('customer_crosswalk') }} c
where c.is_current
    and not exists (
        select 1 from {{ ref('customer_identity') }} i
        where i.is_current
            and i.golden_id = c.golden_id
            and i.source_system = c.source_system
            and i.source_record_id = c.source_record_id
    )

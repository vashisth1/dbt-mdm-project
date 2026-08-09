-- Fails if any source record maps to more than one active golden_id
-- Co-authored with CoCo

select source_system, source_record_id
from {{ ref('customer_identity') }}
where is_current
group by source_system, source_record_id
having count(distinct golden_id) > 1

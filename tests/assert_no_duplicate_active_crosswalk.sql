-- Fails if any (golden_id, source_system, source_record_id) crosswalk relationship is duplicated while active
-- Co-authored with CoCo

select golden_id, source_system, source_record_id
from {{ ref('customer_crosswalk') }}
where is_current
group by golden_id, source_system, source_record_id
having count(*) > 1

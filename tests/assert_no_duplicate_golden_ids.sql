-- Fails if the same golden_id appears more than once in the golden customer mart
-- Co-authored with CoCo

select golden_id
from {{ ref('customer_golden') }}
group by golden_id
having count(*) > 1

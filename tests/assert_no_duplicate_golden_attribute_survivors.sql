-- Fails if any golden_id has more than one surviving value for the same attribute
-- Co-authored with CoCo

select golden_id, attribute_name
from {{ ref('customer_attribute_lineage') }}
where is_survivor
group by golden_id, attribute_name
having count(*) > 1

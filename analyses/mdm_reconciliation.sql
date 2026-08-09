-- Ad-hoc reconciliation summary: issue counts by type for the latest run
-- Co-authored with CoCo

select issue_type, count(*) as issue_count
from {{ ref('customer_reconciliation') }}
group by issue_type
order by issue_count desc

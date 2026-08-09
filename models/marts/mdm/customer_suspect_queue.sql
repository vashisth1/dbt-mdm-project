-- Suspect matches requiring manual review (SUSPECT status only, never auto-merged)
-- Co-authored with CoCo

select
    match_id as suspect_id,
    source_record_id_a,
    source_record_id_b,
    match_score,
    match_rule,
    match_rule_version,
    match_explanation,
    'OPEN' as status,
    null as assigned_to,
    null as decision,
    null as decision_reason,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts
from {{ ref('int_customer_match_decisions') }}
where match_status = 'SUSPECT'

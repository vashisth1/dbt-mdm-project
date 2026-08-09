-- Auditable match decisions derived from scored candidate pairs
-- Co-authored with CoCo

select
    md5(
        left_source_system || '|' || left_source_record_id || '|' ||
        right_source_system || '|' || right_source_record_id || '|' || match_rule_version
    ) as match_id,
    left_source_system as source_record_id_a_system,
    left_source_record_id as source_record_id_a,
    right_source_system as source_record_id_b_system,
    right_source_record_id as source_record_id_b,
    match_score,
    match_status,
    match_rule,
    match_rule_version,
    match_explanation,
    current_timestamp() as decision_ts,
    'v1' as decision_engine_version
from {{ ref('int_customer_match_scores') }}

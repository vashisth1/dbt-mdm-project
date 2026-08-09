-- Attribute-level evidence and combined match score per candidate pair
-- Co-authored with CoCo

with pairs as (
    select distinct
        left_source_system, left_source_record_id,
        right_source_system, right_source_record_id
    from {{ ref('int_customer_match_candidates') }}
),

scored as (
    select
        p.left_source_system,
        p.left_source_record_id,
        p.right_source_system,
        p.right_source_record_id,
        (a.email_hash = b.email_hash and a.email is not null and b.email is not null) as email_match,
        (a.phone_hash = b.phone_hash and length(a.phone_norm) = 10 and length(b.phone_norm) = 10) as phone_match,
        (a.name_norm = b.name_norm and a.name_norm != '') as name_match,
        (a.date_of_birth = b.date_of_birth and a.date_of_birth is not null) as dob_match,
        (a.address_hash = b.address_hash and a.address_line_1 is not null and b.address_line_1 is not null) as address_match
    from pairs p
    join {{ ref('int_customer_normalized') }} a
        on p.left_source_system = a.source_system and p.left_source_record_id = a.source_record_id
    join {{ ref('int_customer_normalized') }} b
        on p.right_source_system = b.source_system and p.right_source_record_id = b.source_record_id
),

rule_applied as (
    select
        *,
        case
            when email_match then 'EMAIL_EXACT'
            when phone_match then 'PHONE_EXACT'
            when name_match and dob_match then 'NAME_DOB'
            else 'NO_RULE'
        end as match_rule
    from scored
)

select
    r.left_source_system,
    r.left_source_record_id,
    r.right_source_system,
    r.right_source_record_id,
    r.email_match,
    r.phone_match,
    r.name_match,
    r.dob_match,
    r.address_match,
    r.match_rule,
    coalesce(t.weight, 0) as match_score,
    'v1' as match_rule_version,
    case
        when r.match_rule = 'NO_RULE' then 'NO_MATCH'
        when coalesce(t.weight, 0) >= t.auto_match_min then 'AUTO_MATCH'
        when coalesce(t.weight, 0) >= t.suspect_min then 'SUSPECT'
        else 'NO_MATCH'
    end as match_status,
    'Matched on ' || r.match_rule || ' (email=' || r.email_match || ', phone=' || r.phone_match || ', name=' || r.name_match || ', dob=' || r.dob_match || ')' as match_explanation
from rule_applied r
left join {{ ref('mdm_match_thresholds') }} t
    on r.match_rule = t.rule_name

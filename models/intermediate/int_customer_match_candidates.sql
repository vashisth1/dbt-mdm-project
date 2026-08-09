-- Match candidates generated via blocking keys: exact email, exact phone, exact name+DOB
-- Co-authored with CoCo

with base as (
    select * from {{ ref('int_customer_normalized') }}
),

email_candidates as (
    select
        a.source_system as left_source_system,
        a.source_record_id as left_source_record_id,
        b.source_system as right_source_system,
        b.source_record_id as right_source_record_id,
        a.email_hash as candidate_key,
        'EMAIL_EXACT' as candidate_rule
    from base a
    join base b
        on a.email_hash = b.email_hash
        and a.email is not null and b.email is not null
        and a.source_system < b.source_system
),

phone_candidates as (
    select
        a.source_system as left_source_system,
        a.source_record_id as left_source_record_id,
        b.source_system as right_source_system,
        b.source_record_id as right_source_record_id,
        a.phone_hash as candidate_key,
        'PHONE_EXACT' as candidate_rule
    from base a
    join base b
        on a.phone_hash = b.phone_hash
        and a.phone_norm is not null and length(a.phone_norm) = 10
        and b.phone_norm is not null and length(b.phone_norm) = 10
        and a.source_system < b.source_system
),

name_dob_candidates as (
    select
        a.source_system as left_source_system,
        a.source_record_id as left_source_record_id,
        b.source_system as right_source_system,
        b.source_record_id as right_source_record_id,
        a.name_dob_hash as candidate_key,
        'NAME_DOB' as candidate_rule
    from base a
    join base b
        on a.name_dob_hash = b.name_dob_hash
        and a.date_of_birth is not null and b.date_of_birth is not null
        and a.source_system < b.source_system
)

select left_source_system, left_source_record_id, right_source_system, right_source_record_id, candidate_key, candidate_rule, current_timestamp() as created_ts
from email_candidates
union
select left_source_system, left_source_record_id, right_source_system, right_source_record_id, candidate_key, candidate_rule, current_timestamp() as created_ts
from phone_candidates
union
select left_source_system, left_source_record_id, right_source_system, right_source_record_id, candidate_key, candidate_rule, current_timestamp() as created_ts
from name_dob_candidates

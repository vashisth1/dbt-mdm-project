-- Attribute-level survivorship: ranks candidate values per attribute by confirmed source priority, then recency
-- Co-authored with CoCo

with entity as (
    select a.golden_id, n.*
    from {{ ref('int_customer_entity_assignments') }} a
    join {{ ref('int_customer_normalized') }} n
        on a.source_system = n.source_system and a.source_record_id = n.source_record_id
),

attributes as (
    select golden_id, source_system, source_record_id, source_updated_ts, 'FIRST_NAME' as attribute_name, first_name as attribute_value from entity where first_name is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'LAST_NAME', last_name from entity where last_name is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'FULL_NAME', full_name from entity where full_name is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'EMAIL', email from entity where email is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'PHONE', phone from entity where phone is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'DATE_OF_BIRTH', to_varchar(date_of_birth) from entity where date_of_birth is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'ADDRESS_LINE_1', address_line_1 from entity where address_line_1 is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'CITY', city from entity where city is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'STATE', state from entity where state is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'POSTAL_CODE', postal_code from entity where postal_code is not null
    union all
    select golden_id, source_system, source_record_id, source_updated_ts, 'COUNTRY', country from entity where country is not null
),

ranked as (
    select
        a.golden_id,
        a.attribute_name,
        a.attribute_value,
        a.source_system,
        a.source_record_id,
        a.source_updated_ts,
        p.priority as source_priority,
        row_number() over (
            partition by a.golden_id, a.attribute_name
            order by p.priority asc, a.source_updated_ts desc, a.source_record_id asc
        ) as survivor_rank
    from attributes a
    left join {{ ref('mdm_source_priority') }} p
        on a.source_system = p.source_system
)

select
    golden_id,
    attribute_name,
    attribute_value,
    md5(attribute_value) as attribute_hash,
    source_system,
    source_record_id,
    source_updated_ts,
    source_priority,
    'SOURCE_PRIORITY_THEN_RECENCY' as survivorship_rule,
    'v1' as survivorship_rule_version,
    (survivor_rank = 1) as is_survivor,
    true as is_current,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts
from ranked

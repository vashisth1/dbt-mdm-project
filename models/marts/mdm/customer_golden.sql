-- Golden customer record assembled from attribute-level survivors
-- Co-authored with CoCo

select
    golden_id,
    max(case when attribute_name = 'FIRST_NAME' then attribute_value end) as first_name,
    max(case when attribute_name = 'LAST_NAME' then attribute_value end) as last_name,
    max(case when attribute_name = 'FULL_NAME' then attribute_value end) as full_name,
    max(case when attribute_name = 'EMAIL' then attribute_value end) as email,
    max(case when attribute_name = 'EMAIL' then source_system end) as email_source_system,
    max(case when attribute_name = 'PHONE' then attribute_value end) as phone,
    max(case when attribute_name = 'PHONE' then source_system end) as phone_source_system,
    max(case when attribute_name = 'DATE_OF_BIRTH' then attribute_value end)::date as date_of_birth,
    max(case when attribute_name = 'ADDRESS_LINE_1' then attribute_value end) as address_line_1,
    max(case when attribute_name = 'CITY' then attribute_value end) as city,
    max(case when attribute_name = 'STATE' then attribute_value end) as state,
    max(case when attribute_name = 'POSTAL_CODE' then attribute_value end) as postal_code,
    max(case when attribute_name = 'COUNTRY' then attribute_value end) as country,
    max(case when attribute_name in ('FIRST_NAME','LAST_NAME','FULL_NAME') then source_system end) as name_source_system,
    max(case when attribute_name = 'ADDRESS_LINE_1' then source_system end) as address_source_system,
    'ACTIVE' as customer_status,
    1 as golden_version,
    'ACTIVE' as record_status,
    current_timestamp() as created_ts,
    current_timestamp() as updated_ts
from {{ ref('customer_attribute_lineage') }}
where is_survivor
group by golden_id

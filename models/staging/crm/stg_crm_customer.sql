-- Staging model mapping CRM raw fields into canonical customer semantics
-- Co-authored with CoCo

select
    'CRM' as source_system,
    customer_id as source_record_id,
    file_id,
    first_name,
    last_name,
    first_name || ' ' || last_name as full_name,
    email,
    phone,
    try_to_date(dob) as date_of_birth,
    address as address_line_1,
    null as address_line_2,
    city,
    state,
    postal_code,
    country,
    status as customer_status,
    try_to_timestamp_ntz(source_updated_ts) as source_updated_ts,
    ingestion_ts,
    row_hash
from {{ source('mdm_raw', 'crm_customer_raw') }}

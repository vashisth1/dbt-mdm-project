-- Staging model mapping ERP raw fields into canonical customer semantics
-- Co-authored with CoCo

select
    'ERP' as source_system,
    customer_number as source_record_id,
    file_id,
    name_first as first_name,
    name_last as last_name,
    name_first || ' ' || name_last as full_name,
    email_address as email,
    telephone as phone,
    try_to_date(birth_date) as date_of_birth,
    billing_address as address_line_1,
    null as address_line_2,
    city,
    state,
    postal_code,
    country,
    status as customer_status,
    try_to_timestamp_ntz(source_updated_ts) as source_updated_ts,
    ingestion_ts,
    row_hash
from {{ source('mdm_raw', 'erp_customer_raw') }}

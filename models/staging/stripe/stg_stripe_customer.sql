-- Staging model mapping Stripe raw fields into canonical customer semantics
-- Co-authored with CoCo

select
    'STRIPE' as source_system,
    customer_id as source_record_id,
    file_id,
    null as first_name,
    null as last_name,
    name as full_name,
    email,
    phone,
    null as date_of_birth,
    null as address_line_1,
    null as address_line_2,
    null as city,
    null as state,
    null as postal_code,
    country,
    status as customer_status,
    try_to_timestamp_ntz(source_updated_ts) as source_updated_ts,
    ingestion_ts,
    row_hash
from {{ source('mdm_raw', 'stripe_customer_raw') }}

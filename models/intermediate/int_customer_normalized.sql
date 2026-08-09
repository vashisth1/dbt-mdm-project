-- Canonical normalized customer records, entry point for the intermediate/matching layer
-- Co-authored with CoCo

select
    source_system,
    source_record_id,
    file_id,
    first_name,
    last_name,
    full_name,
    email,
    phone,
    date_of_birth,
    address_line_1,
    address_line_2,
    city,
    state,
    postal_code,
    country,
    customer_status,
    source_updated_ts,
    ingestion_ts,
    row_hash,
    email_norm,
    phone_norm,
    name_norm,
    email_hash,
    phone_hash,
    name_dob_hash,
    address_hash
from {{ ref('stg_customer') }}

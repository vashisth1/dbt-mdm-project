-- Unified canonical customer staging model, unioning all source systems
-- Co-authored with CoCo

with unioned as (
    select * from {{ ref('stg_crm_customer') }}
    union all
    select * from {{ ref('stg_erp_customer') }}
    union all
    select * from {{ ref('stg_stripe_customer') }}
)

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
    {{ normalize_email('email') }} as email_norm,
    {{ normalize_phone('phone') }} as phone_norm,
    {{ normalize_name('coalesce(full_name, \'\')') }} as name_norm,
    {{ safe_hash('email') }} as email_hash,
    {{ safe_hash('phone') }} as phone_hash,
    {{ safe_hash("coalesce(full_name,'') || '|' || coalesce(to_varchar(date_of_birth),'')") }} as name_dob_hash,
    {{ safe_hash("coalesce(address_line_1,'') || '|' || coalesce(postal_code,'')") }} as address_hash
from unioned

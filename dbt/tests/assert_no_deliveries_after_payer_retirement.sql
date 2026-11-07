-- A retired payer (dbt_project.yml payers: retired_month) should not deliver files after its
-- retirement month. A row here is a late file from a retired payer: it is loaded and kept,
-- but someone should confirm the retirement date (or un-retire the payer).
-- WARN (macros/test_scoping.sql): the marts stay correct; scoped to the batch.
{{ config(severity='warn') }}

select payer_id, retired_month, last_delivery_month
from {{ ref('dim_payer') }}
where retired_month is not null
    and last_delivery_month > retired_month
    and {{ in_batch('last_delivery_month') }}

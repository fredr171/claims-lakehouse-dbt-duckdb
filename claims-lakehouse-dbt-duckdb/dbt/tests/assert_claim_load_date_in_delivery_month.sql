-- A claim version is delivered in the month CMS loaded it (CLM_IDR_LD_DT), so the
-- load date must fall inside the delivery month. A row here means a file landed in
-- the wrong month's partition, or the delivery is mislabeled.
-- WARN (macros/test_scoping.sql): the intermediate layer stays correct; scoped to the batch.
{{ config(severity='warn') }}

with claim_versions as (

    select 'claims_header' as model, claim_id, claim_load_date, delivery_month
    from {{ ref('stg_cclf__claims_header') }}

    union all

    select distinct 'part_b_physician_lines', claim_id, claim_load_date, delivery_month
    from {{ ref('stg_cclf__part_b_physician_lines') }}

)

select *
from claim_versions
where date_trunc('month', claim_load_date) <> delivery_month
  and {{ in_batch() }}

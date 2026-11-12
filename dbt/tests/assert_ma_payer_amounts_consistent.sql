-- Every MA payer claim line follows the amount rules in macros/ma_payer_amount_rules.sql
-- (paid <= allowed <= billed, denied pays nothing, cost share = allowed - paid,
-- reversals never positive). Returns line keys and source location only.
-- WARN (macros/test_scoping.sql): the intermediate layer stays correct; scoped to the batch.
-- assert_ma_payer_amounts_mostly_consistent stops the build if a month has many of these.
{{ config(severity='warn') }}

select claim_id, line_number, claim_frequency_code, delivery_month,
       _source_file, _source_row_number, problem
from (
    select *, {{ ma_payer_amount_problem() }} as problem
    from {{ ref('stg_ma_payer__medical_claim_lines') }}
    where {{ in_batch() }}
)
where problem is not null

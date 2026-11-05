-- Every Humana claim line follows the amount rules in macros/humana_amount_rules.sql
-- (paid <= allowed <= billed, denied pays nothing, cost share = allowed - paid,
-- reversals never positive). Returns line keys and source location only.
-- WARN (macros/test_scoping.sql): the intermediate layer stays correct; scoped to the batch.
-- assert_humana_amounts_mostly_consistent stops the build if a month has many of these.
{{ config(severity='warn') }}

select claim_id, line_number, claim_frequency_code, delivery_month,
       _source_file, _source_row_number, problem
from (
    select *, {{ humana_amount_problem() }} as problem
    from {{ ref('stg_humana__medical_claim_lines') }}
    where {{ in_batch() }}
)
where problem is not null

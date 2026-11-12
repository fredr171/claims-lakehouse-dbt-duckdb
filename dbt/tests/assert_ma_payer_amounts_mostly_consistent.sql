-- STOP: a delivery month where more than 1% of MA payer claim lines (and at least 5)
-- break the amount rules (macros/ma_payer_amount_rules.sql) is a broken extract, not a
-- few odd lines. The 5-line floor keeps one odd line in a small month a warning.
-- Like every STOP it checks all months (macros/test_scoping.sql), so a bad month keeps
-- blocking the intermediate layer until the file is fixed or re-sent.

select delivery_month,
       count(*) filter (where problem is not null)     as problem_lines,
       count(*)                                         as total_lines
from (
    select delivery_month, {{ ma_payer_amount_problem() }} as problem
    from {{ ref('stg_ma_payer__medical_claim_lines') }}
)
group by delivery_month
having count(*) filter (where problem is not null) > 0.01 * count(*)
   and count(*) filter (where problem is not null) >= 5

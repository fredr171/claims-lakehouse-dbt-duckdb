{#
    Humana line amount rules, shared by two tests:
      tests/assert_humana_amounts_consistent.sql        WARN on every line that breaks one
      tests/assert_humana_amounts_mostly_consistent.sql STOP when >1% (and >= 5) of a month's lines do

    Originals and replacements (frequency 1/7): 0 <= paid <= allowed <= billed; denied
    lines allow and pay nothing; outside capitation, member cost share (copay +
    coinsurance + deductible) = allowed - paid. Reversals (frequency 8) negate their
    original, so no amount may be positive. Payers do send odd lines, which is why a
    few only warn; a broken extract breaks many lines at once.
#}

{% macro humana_amount_problem() -%}
    case
        when claim_frequency_code = '8' then
            case when billed_amount > 0 or allowed_amount > 0 or paid_amount > 0
                 then 'reversal with a positive amount' end
        when paid_amount < 0 or allowed_amount < 0 then 'negative amount'
        when paid_amount > allowed_amount then 'paid above allowed'
        when allowed_amount > billed_amount then 'allowed above billed'
        when claim_status_code = 'D' and (allowed_amount <> 0 or paid_amount <> 0)
            then 'denied line with allowed or paid'
        when claim_status_code <> 'D' and capitated_flag <> 'Y'
             and abs(coalesce(copay_amount, 0) + coalesce(coinsurance_amount, 0)
                     + coalesce(deductible_amount, 0) - (allowed_amount - paid_amount)) > 0.005
            then 'cost share is not allowed - paid'
    end
{%- endmacro %}

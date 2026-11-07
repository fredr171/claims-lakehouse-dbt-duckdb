-- One row per claim family across all payers: the claim as it stands today, after every
-- reversal, cancellation, adjustment and replacement (netting happened in intermediate).
-- claim_key is unique across payers; member_key joins dim_member.

with claims as (

    {{ union_conformed('claims') }}

)

select
    payer_id || ':' || payer_claim_id                               as claim_key,
    payer_id,
    payer_claim_id,
    beneficiary_mbi                                                 as member_key,
    payer_member_id,
    claim_category,
    claim_status,
    payer_claim_status,
    service_start_date,
    service_month,
    first_delivery_month,
    latest_delivery_month,
    net_paid_amount,
    is_capitated
from claims

-- Humana MA claims in the conformed contract (macros/payers.sql): one row per claim family
-- from int_humana__claim_families. The marts union this with every other payer's claims.
--
-- Mapping: claim form I -> INSTITUTIONAL, P -> PROFESSIONAL. Status ORIGINAL -> PAID,
-- REPLACED -> ADJUSTED, VOIDED -> VOIDED, DENIED -> DENIED. Capitated encounters keep
-- is_capitated (they pay nothing per claim).

{{ config(materialized='view', enabled=is_registered('humana')) }}

select
    cast({{ payer_id('humana') }} as varchar)                       as payer_id,
    cast(claim_family_key as varchar)                               as payer_claim_id,
    cast(member_id as varchar)                                      as payer_member_id,
    cast(beneficiary_mbi as varchar)                                as beneficiary_mbi,
    cast(
        case claim_form_type_code when 'I' then 'INSTITUTIONAL' when 'P' then 'PROFESSIONAL' end
        as varchar
    )                                                               as claim_category,
    cast(family_status as varchar)                                  as payer_claim_status,
    cast(
        case family_status
            when 'ORIGINAL' then 'PAID'
            when 'REPLACED' then 'ADJUSTED'
            when 'VOIDED' then 'VOIDED'
            when 'DENIED' then 'DENIED'
        end as varchar
    )                                                               as claim_status,
    cast(service_start_date as date)                                as service_start_date,
    cast(service_month as date)                                     as service_month,
    cast(first_delivery_month as date)                              as first_delivery_month,
    cast(latest_delivery_month as date)                             as latest_delivery_month,
    cast(net_paid_amount as decimal(18, 2))                         as net_paid_amount,
    cast(is_capitated as boolean)                                   as is_capitated
from {{ ref('int_humana__claim_families') }}

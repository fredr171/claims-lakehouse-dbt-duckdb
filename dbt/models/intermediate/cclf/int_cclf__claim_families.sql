-- Claim families: one row per (part, claim_control_number), across all deliveries.
--
-- A family is an original claim version plus any cancellation/adjustment that arrived
-- in a later delivery (each with its own claim_id, same control number). The latest
-- version (by load date) decides the net amount:
--   cancellation -> 0;  adjustment -> the adjustment's amount;  else -> the original's.
-- Part A amount = CCLF1 payment_amount; Part B amount = sum of the claim's CCLF5 line
-- paid amounts. Service month = month of the original's claim_start_date.
--
-- Built for partial history too: a cancellation whose original predates the loaded
-- data still forms a family (has_original = false) instead of being dropped.
--
-- Incremental by affected family (macros/incremental_by_affected_keys.sql): a
-- cancellation landing in a later month recomputes the family it belongs to.

{{
    config(
        unique_key='claim_family_key',
        incremental_strategy='delete+insert',
        post_hook="{{ delete_rows_from_vanished_files([ref('stg_cclf__claims_header'), ref('stg_cclf__part_b_physician_lines')]) }}",
    )
}}

with part_a_versions as (

    select
        'A'                         as part,
        claim_control_number,
        claim_id,
        beneficiary_mbi,
        adjustment_type_code,
        claim_start_date,
        claim_load_date,
        delivery_month,
        payment_amount              as paid_amount,
        _source_checksum
    from {{ ref('stg_cclf__claims_header') }}

),

-- CCLF5 is line-level; roll lines up to one row per claim version.
part_b_versions as (

    select
        'B'                         as part,
        claim_control_number,
        claim_id,
        any_value(beneficiary_mbi)          as beneficiary_mbi,
        any_value(adjustment_type_code)     as adjustment_type_code,
        min(claim_start_date)               as claim_start_date,
        max(claim_load_date)                as claim_load_date,
        delivery_month,
        sum(line_paid_amount)               as paid_amount,
        _source_checksum
    from {{ ref('stg_cclf__part_b_physician_lines') }}
    group by claim_control_number, claim_id, delivery_month, _source_checksum

),

staged as (

    select part || ':' || claim_control_number as claim_family_key, *
    from (
        select * from part_a_versions
        union all
        select * from part_b_versions
    )

),

{% if is_incremental() %}
affected as (

    {{ affected_keys('staged', 'claim_family_key') }}

),
{% endif %}

scoped as (

    select * from staged
    {% if is_incremental() %}
    where claim_family_key in (select affected_key from affected)
    {% endif %}

),

sequenced as (

    select
        *,
        row_number() over (
            partition by claim_family_key
            order by claim_load_date, delivery_month, claim_id
        ) as version_sequence
    from scoped

),

families as (

    select
        claim_family_key,
        any_value(part)                                                         as part,
        any_value(claim_control_number)                                         as claim_control_number,
        arg_min(beneficiary_mbi, version_sequence)                              as beneficiary_mbi,

        -- original
        arg_min(claim_id, version_sequence) filter (where adjustment_type_code = '0')         as original_claim_id,
        arg_min(delivery_month, version_sequence) filter (where adjustment_type_code = '0')   as original_delivery_month,
        arg_min(claim_start_date, version_sequence) filter (where adjustment_type_code = '0') as original_claim_start_date,
        arg_min(paid_amount, version_sequence) filter (where adjustment_type_code = '0')      as original_amount,
        count(*) filter (where adjustment_type_code = '0')                                    as original_count,

        -- latest cancellation/adjustment
        arg_max(adjustment_type_code, version_sequence) filter (where adjustment_type_code <> '0') as latest_action_type_code,
        arg_max(claim_id, version_sequence) filter (where adjustment_type_code <> '0')             as action_claim_id,
        arg_max(delivery_month, version_sequence) filter (where adjustment_type_code <> '0')       as action_delivery_month,
        arg_max(paid_amount, version_sequence) filter (where adjustment_type_code <> '0')          as action_amount,
        count(*) filter (where adjustment_type_code <> '0')                                        as action_count,

        -- the version that decides the outcome
        arg_max(adjustment_type_code, version_sequence)                         as final_adjustment_type_code,
        arg_max(paid_amount, version_sequence)                                  as final_version_amount,
        min(claim_start_date)                                                   as earliest_claim_start_date,

        count(*)                                                                as version_count,
        min(delivery_month)                                                     as first_delivery_month,
        max(delivery_month)                                                     as latest_delivery_month,
        list_sort(list(distinct _source_checksum))                              as _source_checksums
    from sequenced
    group by claim_family_key

)

select
    claim_family_key,
    part,
    claim_control_number,
    beneficiary_mbi,
    original_claim_id is not null                                       as has_original,
    original_claim_id,
    original_delivery_month,
    cast(original_amount as decimal(18, 2))                             as original_amount,
    case latest_action_type_code when '1' then 'CANCEL' when '2' then 'ADJUST' end as action_type,
    action_claim_id,
    action_delivery_month,
    cast(action_amount as decimal(18, 2))                               as action_amount,
    case final_adjustment_type_code
        when '0' then 'ORIGINAL'
        when '1' then 'CANCELLED'
        when '2' then 'ADJUSTED'
    end                                                                 as family_status,
    cast(
        case when final_adjustment_type_code = '1' then 0 else final_version_amount end
        as decimal(18, 2)
    )                                                                   as net_paid_amount,
    coalesce(original_claim_start_date, earliest_claim_start_date)      as service_start_date,
    cast(
        date_trunc('month', coalesce(original_claim_start_date, earliest_claim_start_date))
        as date
    )                                                                   as service_month,
    version_count,
    original_count,
    action_count,
    first_delivery_month,
    latest_delivery_month,
    _source_checksums
from families

-- Beneficiary history (SCD type 2) from CCLF8: one row per beneficiary per version.
--
-- A new version starts whenever the tracked attributes differ from the beneficiary's
-- previous delivered row (hash comparison). That works whether a CCLF8 delivery holds
-- only new/changed beneficiaries (the generator) or the full population every month:
-- unchanged rows just extend the current version. NEW/UPDATE is derived here, never
-- read from the synthetic record_change_type helper.
--
-- valid_from = delivery month the version arrived; valid_to = the next version's
-- valid_from (exclusive), NULL while current. Age is not tracked: it moves forward
-- every month without a real change and is derivable from birth_date.
--
-- Incremental by affected beneficiary (macros/incremental_by_affected_keys.sql):
-- unique_key is the REPLACEMENT key, so all versions of an affected beneficiary are
-- deleted and re-inserted together.

{{
    config(
        unique_key='beneficiary_mbi',
        incremental_strategy='delete+insert',
        post_hook="{{ delete_rows_from_vanished_files([ref('stg_cclf__beneficiary_demographics')]) }}",
    )
}}

{%- set tracked_columns = [
    'beneficiary_hicn',
    'birth_date',
    'death_date',
    'sex_code',
    'race_code',
    'first_name',
    'middle_name',
    'last_name',
    'address_line_1',
    'city',
    'usps_state_code',
    'zip5_code',
    'zip9_code',
    'fips_state_code',
    'fips_county_code',
    'medicare_status_code',
    'dual_status_code',
    'original_entitlement_reason_code',
    'entitlement_buyin_indicator',
    'part_a_enrollment_start_date',
    'part_b_enrollment_start_date',
    'coverage_start_date',
    'coverage_end_date',
] %}

with staged as (

    select
        *,
        {{ row_hash(tracked_columns) }} as attribute_hash
    from {{ ref('stg_cclf__beneficiary_demographics') }}

),

{% if is_incremental() %}
affected as (

    {{ affected_keys('staged', 'beneficiary_mbi') }}

),
{% endif %}

scoped as (

    select * from staged
    {% if is_incremental() %}
    where beneficiary_mbi in (select affected_key from affected)
    {% endif %}

),

flagged as (

    select
        *,
        (lag(attribute_hash) over by_delivery) is distinct from attribute_hash as starts_version
    from scoped
    window by_delivery as (partition by beneficiary_mbi order by delivery_month)

),

numbered as (

    select
        *,
        sum(case when starts_version then 1 else 0 end) over (
            partition by beneficiary_mbi
            order by delivery_month
            rows between unbounded preceding and current row
        ) as version_number
    from flagged

),

-- Delivered rows that collapse into one version (more than one only for snapshot-style
-- deliveries that repeat an unchanged beneficiary).
version_spans as (

    select
        beneficiary_mbi,
        version_number,
        max(delivery_month)                         as last_delivered_month,
        count(*)                                    as delivery_count,
        list_sort(list(distinct _source_checksum))  as _source_checksums
    from numbered
    group by beneficiary_mbi, version_number

),

versions as (

    select
        n.beneficiary_mbi,
        n.version_number,
        n.delivery_month as valid_from,
        lead(n.delivery_month) over by_version as valid_to,
        {%- for column in tracked_columns %}
        n.{{ column }},
        lag(n.{{ column }}) over by_version as previous_{{ column }},
        {%- endfor %}
        n.attribute_hash,
        s.last_delivered_month,
        s.delivery_count,
        s._source_checksums
    from numbered as n
    inner join version_spans as s
        on s.beneficiary_mbi = n.beneficiary_mbi
        and s.version_number = n.version_number
    where n.starts_version
    window by_version as (partition by n.beneficiary_mbi order by n.version_number)

)

select
    beneficiary_mbi || '|' || strftime(valid_from, '%Y-%m') as beneficiary_version_id,
    beneficiary_mbi,
    version_number,
    valid_from,
    valid_to,
    valid_to is null as is_current,
    case when version_number = 1 then 'NEW' else 'UPDATE' end as change_type,
    case
        when version_number = 1 then []::varchar[]
        else list_filter(
            [
                {%- for column in tracked_columns %}
                case when {{ column }} is distinct from previous_{{ column }} then '{{ column }}' end
                {%- if not loop.last %},{% endif %}
                {%- endfor %}
            ],
            lambda changed: changed is not null
        )
    end as changed_columns,
    {%- for column in tracked_columns %}
    {{ column }},
    {%- endfor %}
    attribute_hash,
    last_delivered_month,
    delivery_count,
    _source_checksums
from versions

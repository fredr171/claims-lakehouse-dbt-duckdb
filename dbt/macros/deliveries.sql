{#
    Immutable bronze: raw keeps EVERY delivered file, one key per delivery:
        <feed>/<file_type>/delivery_month=<yyyy-mm>/checksum=<sha256>/part-0.parquet
    A corrected re-send of a file (same file name, new content) lands next to the
    original instead of replacing it. Which delivery counts is decided here, in one place:

      deliveries_index()      -> stg_<feed>__deliveries: one row per delivery in raw, with
                                 is_current (the latest ingested delivery of each file
                                 name) and the checksum that superseded the others.
      current_delivery_rows() -> what each staging model reads: every row of the CURRENT
                                 delivery of each file, nothing from superseded ones.

    Intermediate only ever sees current deliveries. When a re-send supersedes a file, the
    old checksum disappears from staging, so incremental_by_affected_keys treats it like
    a vanished file and recomputes every key it touched: incremental == full refresh.
#}


{% macro deliveries_index(source_name, tables) -%}
with deliveries as (

    {%- for table in tables %}

    select
        '{{ table }}'               as file_type,
        _source_file                as file_name,
        delivery_month              as partition_delivery_month,
        _source_checksum            as source_checksum,
        checksum                    as partition_checksum,
        min(_ingested_at)           as ingested_at,
        count(*)                    as row_count
    from {{ source(source_name, table) }}
    group by all
    {%- if not loop.last %}

    union all
    {%- endif %}
    {%- endfor %}

)

select
    file_type,
    file_name,
    cast(partition_delivery_month || '-01' as date)     as delivery_month,
    source_checksum                                      as checksum,
    partition_checksum,
    ingested_at,
    row_count,
    row_number() over latest_first = 1                   as is_current,
    first_value(source_checksum) over latest_first       as current_checksum,
    count(*) over (partition by file_type, file_name)    as deliveries_of_file
from deliveries
window latest_first as (
    partition by file_type, file_name
    order by ingested_at desc, source_checksum
)
{%- endmacro %}


{% macro current_delivery_rows(source_name, table) -%}
    {#- The rows of the current delivery of each file of one file type (see above). -#}
    select * from {{ source(source_name, table) }}
    where _source_checksum in (
        select checksum
        from {{ ref('stg_' ~ source_name ~ '__deliveries') }}
        where is_current and file_type = '{{ table }}'
    )
{%- endmacro %}

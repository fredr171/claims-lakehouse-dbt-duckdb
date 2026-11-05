{#
    Test severity policy: STOP vs WARN.

    STOP (severity error, the dbt default): the data would make the intermediate layer
    wrong (keys, grain, identity, claim-family linkage, money, service dates, codes the
    intermediate logic branches on). `dbt build` then SKIPS every model downstream of
    the failing model, so the intermediate tables keep their last good state.

    WARN (severity warn): the intermediate layer is still correct, but the data looks
    suspicious (descriptive codes, orphans, dates vs the delivery month). Logged and
    reported; nothing is skipped.

    Scoping:
      - STOP tests always check ALL delivery months, never just the new ones. A file
        the intermediate layer skipped stays "unprocessed" (incremental_by_affected_keys
        compares checksums), so if a later run only tested its own months it would pass
        and pull the bad rows in. A stop keeps blocking until the file is fixed or
        re-sent, by design.
      - WARN tests check only the delivery months being processed when the runner
        passes them (--vars '{delivery_months: ["2025-03"]}'), so an accepted warning
        does not re-fire on every later delivery. Without the var (local builds) they
        check everything.

    In YAML, a warn test sets  config: {severity: warn, where: "__in_batch__"};
    singular warn tests call in_batch() themselves.
#}


{% macro batch_months() -%}
    {#- Validated list of 'YYYY-MM' strings from the delivery_months var, or none. -#}
    {%- set months = var('delivery_months', none) -%}
    {%- if months is none or months == [] -%}
        {{ return(none) }}
    {%- endif -%}
    {%- if months is string -%}
        {%- set months = months.split(',') -%}
    {%- endif -%}
    {%- set cleaned = [] -%}
    {%- for month in months -%}
        {%- set month = (month | string).strip() -%}
        {%- if not modules.re.fullmatch('\d{4}-(0[1-9]|1[0-2])', month) -%}
            {{ exceptions.raise_compiler_error("delivery_months: expected YYYY-MM, got '" ~ month ~ "'") }}
        {%- endif -%}
        {%- do cleaned.append(month) -%}
    {%- endfor -%}
    {{ return(cleaned) }}
{%- endmacro %}


{% macro in_batch(column='delivery_month') -%}
    {#- SQL predicate: the row's delivery month is one being processed (true if none given). -#}
    {%- set months = batch_months() -%}
    {%- if months is none -%}
        true
    {%- else -%}
        {{ column }} in ({% for m in months %}date '{{ m }}-01'{{ ", " if not loop.last }}{% endfor %})
    {%- endif -%}
{%- endmacro %}


{% macro get_where_subquery(relation) -%}
    {#- Overrides dbt's built-in: same behavior, plus the __in_batch__ placeholder. -#}
    {%- set where = config.get('where', '') -%}
    {%- if where -%}
        {%- set where = where | replace('__in_batch__', in_batch('delivery_month')) -%}
        {%- set filtered -%}
            (select * from {{ relation }} where {{ where }}) dbt_subquery
        {%- endset -%}
        {%- do return(filtered) -%}
    {%- else -%}
        {%- do return(relation) -%}
    {%- endif -%}
{%- endmacro %}

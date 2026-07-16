-- =============================================================================
-- classify_cancel_flow_screen — dbt Macro
-- =============================================================================
-- Classifies the application surface from URL host + device type.
-- Reusable across all dbt models that need screen classification.
--
-- Args:
--   url_host_col:    Column name for URL hostname
--   device_type_col: Column name for device type
--   page_path_col:   Column name for page path
--
-- Returns:
--   STRING: 'Account Portal' | 'SaaS Mobile App' | 'SaaS Web App' | 'Unknown'
--
-- Usage:
--   {{ classify_cancel_flow_screen('ci.properties_url_host_name', 'ci.ua_parser_device_type', 'ci.context_page_path') }}
-- =============================================================================

{% macro classify_cancel_flow_screen(url_host_col, device_type_col, page_path_col) %}
    case
        -- Account portal (OIAM equivalent)
        when lower(coalesce({{ url_host_col }}, '')) like '%accounts.%'
          or lower(coalesce({{ page_path_col }}, '')) like '%accountmanager%'
          or lower(coalesce({{ page_path_col }}, '')) like '%accounts.%'
            then 'Account Portal'

        -- Mobile app
        when lower(coalesce({{ device_type_col }}, '')) in ('mobile', 'tablet', 'smartphone', 'phone')
          or lower(coalesce({{ url_host_col }}, '')) like '%mobile%'
            then 'SaaS Mobile App'

        -- Web app
        when lower(coalesce({{ url_host_col }}, '')) like '%app.%'
          or lower(coalesce({{ url_host_col }}, '')) like '%saas.%'
          or lower(coalesce({{ url_host_col }}, '')) like '%dashboard%'
            then 'SaaS Web App'

        else 'Unknown'
    end
{% endmacro %}


-- =============================================================================
-- generate_initiation_date_trunc — Partition key helper
-- =============================================================================

{% macro partition_year(date_col) %}
    date_format({{ date_col }}, 'yyyy')
{% endmacro %}

{% macro partition_month(date_col) %}
    date_format({{ date_col }}, 'MM')
{% endmacro %}


-- =============================================================================
-- assert_row_count_match — dbt Test Macro
-- =============================================================================
-- Validates that Step 1 = Step 3 = Step 4 row counts match.

{% macro test_row_counts_match(model, column_name, compare_model) %}
    with model_count as (
        select count(*) as cnt from {{ model }}
    ),
    compare_count as (
        select count(*) as cnt from {{ ref(compare_model) }}
    )
    select 1
    from model_count m, compare_count c
    where m.cnt != c.cnt
{% endmacro %}


-- =============================================================================
-- safe_divide — Division with zero protection
-- =============================================================================

{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null then {{ default }}
        else {{ numerator }} * 1.0 / {{ denominator }}
    end
{% endmacro %}

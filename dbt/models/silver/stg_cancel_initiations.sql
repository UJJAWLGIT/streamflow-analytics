{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['company_id', 'initiation_rank', 'initiation_timestamp'],
    partition_by = {'field': 'initiation_date', 'data_type': 'date', 'granularity': 'month'},
    file_format = 'delta',
    tags = ['silver', 'daily'],
    meta = {'owner': 'ujjawl.kumar', 'tier': 'silver', 'step': 1}
  )
}}

/*
  silver.stg_cancel_initiations — Step 1
  =======================================
  Cancel initiation grain: one row per (company, sku, initiation_timestamp).
  "ALL products — no single-product or experiment filter"
  Captures all three confirmation taxonomies:
    1. cancel_success       (new, deployed 2024-05-07)
    2. yes_cancel           (legacy single-screen)
    3. cancelation flow     (legacy multi-screen)
*/

with raw as (
    select * from {{ ref('raw_clickstream_events') }}
    {% if is_incremental() %}
    where event_date between
        {{ var('start_date', "current_date - interval '7 days'") }}
        and {{ var('end_date', 'current_date') }}
    {% endif %}
),

cancel_initiations as (
    select
        company_id,
        to_timestamp(event_timestamp)      as initiation_timestamp,
        to_date(event_timestamp)           as initiation_date,
        max(product)                       as product,
        max(sku)                           as sku,
        max(billing_frequency)             as billing_frequency,
        max(subscription_type)             as subscription_type,
        max(properties_url_host_name)      as properties_url_host_name,
        max(ua_parser_device_type)         as ua_parser_device_type,
        max(context_page_path)             as context_page_path,
        max(accountant_realm_id)           as accountant_id_starting_cancellation
    from raw
    where event in ('workflow: started', 'workflow:started')
      and properties_object_detail in ('cancel', 'cancellation_workflow')
      and properties_ui_object_detail in ('cancel_subscription', 'cancel')
    group by 1, 2, 3
),

windowed as (
    select *,
        row_number() over (
            partition by company_id, sku order by initiation_timestamp
        ) as initiation_rank,
        lead(initiation_timestamp) over (
            partition by company_id, sku order by initiation_timestamp
        ) as next_initiation_timestamp
    from cancel_initiations
),

with_window_end as (
    select *,
        coalesce(next_initiation_timestamp,
                 initiation_timestamp + interval '1 hour') as window_end_timestamp
    from windowed
),

-- All three confirmation taxonomies
confirmations as (
    -- New: cancel_success
    select company_id, to_timestamp(event_timestamp) as confirm_ts
    from raw
    where event in ('workflow: completed', 'workflow:completed')
      and properties_object_detail = 'cancel'
      and properties_ui_object_detail = 'cancel_success'
    union all
    -- Legacy single-screen: yes_cancel
    select company_id, to_timestamp(event_timestamp)
    from raw
    where event in ('workflow: engaged', 'widget: engaged')
      and properties_ui_object_detail = 'yes_cancel'
    union all
    -- Legacy multi-screen: cancelation flow
    select company_id, to_timestamp(event_timestamp)
    from raw
    where event in ('cancelation flow: viewed', 'cancelation flow:viewed')
      and properties_ui_access_point = 'cancel success'
),

confirmed as (
    select
        ci.company_id,
        ci.initiation_timestamp,
        1 as cancel_confirmed,
        min(cfr.confirm_ts) as confirmation_timestamp
    from with_window_end ci
    join confirmations cfr
        on  cfr.company_id   = ci.company_id
        and cfr.confirm_ts  >= ci.initiation_timestamp
        and cfr.confirm_ts   < ci.window_end_timestamp
    group by 1, 2
),

companies as (
    select * from {{ ref('dim_company') }}
    where country = 'United States' and is_suspicious = 0
),

final as (
    select
        cast(co.company_id as bigint)                       as company_id,
        to_date(co.signup_date)                             as signup_date,
        datediff(ci.initiation_date, to_date(co.signup_date)) as tenure_at_cancel_initiation,
        ci.sku, ci.billing_frequency, ci.subscription_type,
        ci.properties_url_host_name, ci.ua_parser_device_type, ci.context_page_path,
        coalesce(ci.accountant_id_starting_cancellation is not null, false) as is_accountant_flag,
        ci.accountant_id_starting_cancellation,
        case when ci.accountant_id_starting_cancellation is not null then 'Y' else 'N' end
            as is_accountant_starting_cancellation,
        ci.initiation_date,
        ci.initiation_timestamp,
        ci.initiation_rank,
        ci.window_end_timestamp,
        coalesce(cc.cancel_confirmed, 0)                    as cancel_confirmed,
        cc.confirmation_timestamp,
        ci.product,
        date_format(ci.initiation_date, 'yyyy')             as initiation_year,
        date_format(ci.initiation_date, 'MM')               as initiation_month
    from with_window_end ci
    inner join companies co
        on cast(co.company_id as string) = ci.company_id
    left join confirmed cc
        on  cc.company_id            = ci.company_id
        and cc.initiation_timestamp  = ci.initiation_timestamp
)

select * from final

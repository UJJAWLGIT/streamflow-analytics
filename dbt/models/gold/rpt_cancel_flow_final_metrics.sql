{{
  config(
    materialized   = 'incremental',
    incremental_strategy = 'merge',
    unique_key     = ['company_id', 'initiation_rank', 'cancel_flow_start_timestamp'],
    partition_by   = {
      'field'       : 'initiation_date',
      'data_type'   : 'date',
      'granularity' : 'month',
    },
    cluster_by     = ['company_id', 'product'],
    file_format    = 'delta',
    tags           = ['gold', 'governed', '3star', 'daily'],
    meta           = {
      'owner'          : 'ujjawl.kumar',
      'tier'           : '3-star',
      'slo_freshness_h': 8,
      'slo_query_ms'   : 500,
      'iedm_entity'    : 'rpt_cancel_flow_final_metrics',
    },
  )
}}

/*
  gold.rpt_cancel_flow_final_metrics
  ===================================
  Final reporting table: one row per cancel initiation across all products.
  No IXP experiment filter — full population coverage.

  Grain:     (company_id, initiation_rank, cancel_flow_start_timestamp)
  Partition: (product, initiation_year, initiation_month)
  Format:    Delta Lake — ACID, time-travel enabled
  SLO:       Query P99 ≤ 500ms | Freshness ≤ 8h

  Joins:
    - silver.stg_cancel_initiations  (Step 1)
    - gold.rpt_ipd_detailed_engagement (Step 2) — IPD engagement aggregates
    - silver.stg_save_attribution    (Step 3)
    - silver.subscriber_status_daily              — 31d/92d retention

  Business logic:
    - cancel_confirmed converted INT → STRING (Y/N)
    - cancel_flow_screen classified from URL host + device type
    - baked_31d/92d computed from current_date vs initiation_date + N days
    - retained_31d/92d from subscriber_status_daily LEFT JOIN
*/

with cancel_initiations as (
    select * from {{ ref('stg_cancel_initiations') }}
    {% if is_incremental() %}
    where initiation_date >= {{ var('start_date', "current_date - interval '7 days'") }}
      and initiation_date <= {{ var('end_date', 'current_date') }}
    {% endif %}
),

save_attribution as (
    select * from {{ ref('stg_save_attribution') }}
),

-- Aggregate IPD engagement flags per initiation from offer-grain table
engagement_flags as (
    select
        company_id,
        initiation_rank,
        initiation_timestamp,
        max(case when ipd_type = 'CS IPD'          then coalesce(viewed_ipd, 0) else 0 end) as viewed_cs_ipd,
        max(case when ipd_type = 'CS IPD'          then coalesce(clicked_ipd, 0) else 0 end) as clicked_cs_ipd,
        max(case when ipd_type = 'Discount IPD'    then coalesce(viewed_ipd, 0) else 0 end) as viewed_discount_ipd,
        max(case when ipd_type = 'Discount IPD'    then coalesce(clicked_ipd, 0) else 0 end) as clicked_discount_ipd,
        max(case when ipd_type = 'Upgrade IPD'     then coalesce(viewed_ipd, 0) else 0 end) as viewed_upgrade_ipd,
        max(case when ipd_type = 'Upgrade IPD'     then coalesce(clicked_ipd, 0) else 0 end) as clicked_upgrade_ipd,
        max(case when ipd_type = 'Downgrade IPD'   then coalesce(viewed_ipd, 0) else 0 end) as viewed_downgrade_ipd,
        max(case when ipd_type = 'Downgrade IPD'   then coalesce(clicked_ipd, 0) else 0 end) as clicked_downgrade_ipd,
        max(case when ipd_type = 'Keep my Plan IPD' then coalesce(viewed_ipd, 0) else 0 end) as viewed_keep_plan_ipd,
        max(case when ipd_type = 'Keep my Plan IPD' then coalesce(clicked_ipd, 0) else 0 end) as clicked_keep_plan_ipd,
        max(coalesce(viewed_dic_component, 0))     as viewed_dic,
        max(coalesce(number_of_data_points_shown, 0)) as dic_max_data_points
    from {{ ref('rpt_ipd_detailed_engagement') }}
    group by 1, 2, 3
),

-- 31-day retention join
subscriber_31d as (
    select company_id, date_of
    from {{ ref('subscriber_status_daily') }}
    where open_subscriber = 1
),

-- 92-day retention join
subscriber_92d as (
    select company_id, date_of
    from {{ ref('subscriber_status_daily') }}
    where open_subscriber = 1
),

-- Cancel flow screen classification
final as (
    select
        -- Identity
        ci.company_id,
        cast(ci.company_id as varchar) as realm_id,

        -- Tenure
        ci.signup_date,
        ci.tenure_at_cancel_initiation,

        -- Product context
        ci.product,
        ci.sku,
        ci.billing_frequency,
        ci.subscription_type,

        -- Initiation
        ci.initiation_date,
        ci.initiation_timestamp            as cancel_flow_start_timestamp,
        ci.initiation_rank,

        -- Confirmation (INT → Y/N)
        case when ci.cancel_confirmed = 1 then 'Y' else 'N' end as cancel_confirmed,
        ci.confirmation_timestamp          as cancel_confirmation_timestamp,

        -- Save outcome flags (Y/N)
        case when coalesce(sa.saved_by_cs, 0) = 1 then 'Y' else 'N' end        as saved_by_customer_support,
        case when coalesce(sa.upgraded, 0) = 1 then 'Y' else 'N' end           as saved_by_upgrading,
        case when coalesce(sa.downgraded, 0) = 1 then 'Y' else 'N' end         as saved_by_downgrading,
        case when coalesce(sa.took_discount, 0) = 1 then 'Y' else 'N' end      as saved_by_taking_discount,
        sa.discount_take_timestamp,
        coalesce(sa.saved_by_abandoning, 'N')                                  as saved_by_abandoning_cancel_flow,

        -- Accountant context
        ci.is_accountant_starting_cancellation,
        ci.accountant_id_starting_cancellation,

        -- Screen classification
        {{ classify_cancel_flow_screen(
            url_host_col       = 'ci.properties_url_host_name',
            device_type_col    = 'ci.ua_parser_device_type',
            page_path_col      = 'ci.context_page_path'
        ) }} as cancel_flow_screen,

        ci.properties_url_host_name,
        ci.ua_parser_device_type,
        ci.context_page_path,

        -- Save attribution
        sa.save_attribution,
        coalesce(sa.contacted_by_cs, 0)  as contacted_by_cs,
        coalesce(sa.saved_by_cs, 0)      as saved_by_cs,
        coalesce(sa.upgraded, 0)         as upgraded,
        sa.upgrade_timestamp,
        coalesce(sa.downgraded, 0)       as downgraded,
        sa.downgrade_timestamp,
        coalesce(sa.took_discount, 0)    as took_discount,

        -- IPD engagement flags
        coalesce(ef.viewed_cs_ipd, 0)        as viewed_cs_ipd,
        coalesce(ef.clicked_cs_ipd, 0)       as clicked_cs_ipd,
        coalesce(ef.viewed_discount_ipd, 0)  as viewed_discount_ipd,
        coalesce(ef.clicked_discount_ipd, 0) as clicked_discount_ipd,
        coalesce(ef.viewed_upgrade_ipd, 0)   as viewed_upgrade_ipd,
        coalesce(ef.clicked_upgrade_ipd, 0)  as clicked_upgrade_ipd,
        coalesce(ef.viewed_downgrade_ipd, 0) as viewed_downgrade_ipd,
        coalesce(ef.clicked_downgrade_ipd, 0)as clicked_downgrade_ipd,
        coalesce(ef.viewed_keep_plan_ipd, 0) as viewed_keep_plan_ipd,
        coalesce(ef.clicked_keep_plan_ipd, 0)as clicked_keep_plan_ipd,
        coalesce(ef.viewed_dic, 0)           as viewed_dic,
        coalesce(ef.dic_max_data_points, 0)  as dic_max_data_points,

        -- 31-day bake + retention
        case when date_add('day', 31, ci.initiation_date) < current_date then 1 else 0 end as baked_31d,
        case when s31.company_id is not null then 1 else 0 end                              as retained_31d,

        -- 92-day bake + retention
        case when date_add('day', 92, ci.initiation_date) < current_date then 1 else 0 end as baked_92d,
        case when s92.company_id is not null then 1 else 0 end                              as retained_92d,

        -- Audit
        current_timestamp as dwh_create_date,
        current_timestamp as dwh_update_date,

        -- Partition keys
        date_format(ci.initiation_date, 'yyyy')  as initiation_year,
        date_format(ci.initiation_date, 'MM')    as initiation_month

    from cancel_initiations ci
    left join save_attribution sa
        on  sa.company_id           = ci.company_id
        and sa.initiation_rank      = ci.initiation_rank
        and sa.initiation_timestamp = ci.initiation_timestamp
    left join engagement_flags ef
        on  ef.company_id           = ci.company_id
        and ef.initiation_rank      = ci.initiation_rank
        and ef.initiation_timestamp = ci.initiation_timestamp
    left join subscriber_31d s31
        on  s31.company_id = ci.company_id
        and s31.date_of    = date_add('day', 31, ci.initiation_date)
    left join subscriber_92d s92
        on  s92.company_id = ci.company_id
        and s92.date_of    = date_add('day', 92, ci.initiation_date)
)

select * from final

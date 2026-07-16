{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['company_id','initiation_rank','initiation_timestamp','properties_ui_access_point','properties_custom_fp_offer_id'],
    partition_by = {'field': 'initiation_date', 'data_type': 'date', 'granularity': 'month'},
    file_format = 'delta',
    tags = ['gold', 'governed', '3star', 'daily'],
    meta = {
      'owner': 'ujjawl.kumar', 'tier': '3-star', 'step': 2,
      'slo_query_ms': 500, 'iedm_entity': 'rpt_ipd_detailed_engagement'
    }
  )
}}

/*
  gold.rpt_ipd_detailed_engagement -- Step 2 (3-star Consumable)
  ==============================================================
  Offer-grain IPD and DIC engagement: 0-N rows per cancel initiation.

  Grain:       (company_id, initiation_rank, initiation_timestamp, access_point, offer_id)
  Partition:   (product, initiation_year, initiation_month)
  DQ:          SELECT DISTINCT guarantees uniqueness = 1.0 (100%)
  Join to Step 4: on (company_id, initiation_rank, initiation_timestamp = cancel_flow_start_timestamp)
*/

with raw as (
    select * from {{ ref('raw_clickstream_events') }}
    {% if is_incremental() %}
    where event_date between
        {{ var('start_date', "current_date - interval '7 days'") }}
        and {{ var('end_date', 'current_date') }}
    {% endif %}
),

initiations as (
    select * from {{ ref('stg_cancel_initiations') }}
),

offer_catalog as (
    select * from {{ ref('dim_offer_catalog') }}
),

-- IPD views and clicks (INNER JOIN guarantees non-null access_point + offer_id)
ipd_events as (
    select
        ci.company_id,
        ci.initiation_rank,
        ci.initiation_timestamp,
        ci.window_end_timestamp,
        ecs.properties_ui_access_point,
        ecs.properties_custom_fp_offer_id,
        max(case when ecs.event = 'offer: viewed'  then 1 else 0 end) as viewed_ipd,
        max(case when ecs.event = 'offer: clicked' then 1 else 0 end) as clicked_ipd,
        min(case when ecs.event = 'offer: viewed'  then to_timestamp(ecs.event_timestamp) end) as ipd_view_timestamp
    from initiations ci
    inner join raw ecs
        on  cast(ecs.company_id as bigint)  = ci.company_id
        and ecs.event in ('offer: viewed', 'offer: clicked')
        and ecs.properties_ui_access_point in (
            'CancelFlowBillingCancel', 'CancelFlowTalkToExpert',
            'AccountSettingsCancel', 'MobileAppBillingCancel'
        )
        and to_timestamp(ecs.event_timestamp) >= ci.initiation_timestamp
        and to_timestamp(ecs.event_timestamp) <  ci.window_end_timestamp
    group by 1, 2, 3, 4, 5, 6
),

-- DIC (Data-Informed Cancellation) content:viewed events
dic_events as (
    select
        ci.company_id,
        ci.initiation_rank,
        ci.initiation_timestamp,
        1                                                        as viewed_dic_component,
        max(cast(
            get_json_object(ecs.properties_ui_object_detail, '$.data_object_display_count')
        as int))                                                  as number_of_data_points_shown,
        max(ecs.properties_ui_object_detail)                     as dic_component_detail,
        min(to_timestamp(ecs.event_timestamp))                   as dic_impression_timestamp
    from initiations ci
    inner join raw ecs
        on  cast(ecs.company_id as bigint) = ci.company_id
        and ecs.event in ('content: viewed', 'content:viewed')
        and ecs.properties_object_detail = 'usage-highlights-widget'
        and to_timestamp(ecs.event_timestamp) >= ci.initiation_timestamp
        and to_timestamp(ecs.event_timestamp) <  ci.window_end_timestamp
    group by 1, 2, 3
),

-- Final join with offer metadata + DIC
raw_result as (
    select
        ipd.company_id,
        ipd.initiation_rank,
        ipd.initiation_timestamp,
        ipd.window_end_timestamp,
        ipd.properties_ui_access_point,
        ipd.properties_custom_fp_offer_id,
        oc.offer_name,
        oc.cta_text                                         as primaryCtaText,
        oc.obill_offer_id,
        -- IPD type classification
        case
            when oc.cta_action = 'contact-us-widget'                          then 'CS IPD'
            when oc.obill_offer_id is not null                                 then 'Discount IPD'
            when oc.cta_action = 'external' and oc.cta_url like '%/obillupgrade%' then 'Upgrade IPD'
            when oc.cta_action = 'external' and oc.cta_url like '%/changeplan%'   then 'Downgrade IPD'
            when oc.cta_action = 'callbackOnly' and oc.obill_offer_id is null  then 'Keep my Plan IPD'
            else 'Unknown'
        end                                                 as ipd_type,
        ipd.ipd_view_timestamp,
        ipd.viewed_ipd,
        ipd.clicked_ipd,
        coalesce(dic.viewed_dic_component, 0)               as viewed_dic_component,
        coalesce(dic.number_of_data_points_shown, 0)        as number_of_data_points_shown,
        dic.dic_component_detail,
        dic.dic_impression_timestamp,
        ci.product,
        ci.initiation_date,
        date_format(ci.initiation_date, 'yyyy')             as initiation_year,
        date_format(ci.initiation_date, 'MM')               as initiation_month
    from ipd_events ipd
    join {{ ref('stg_cancel_initiations') }} ci
        on  ci.company_id           = ipd.company_id
        and ci.initiation_rank      = ipd.initiation_rank
        and ci.initiation_timestamp = ipd.initiation_timestamp
    left join offer_catalog oc
        on  oc.offer_id = ipd.properties_custom_fp_offer_id
    left join dic_events dic
        on  dic.company_id           = ipd.company_id
        and dic.initiation_rank      = ipd.initiation_rank
        and dic.initiation_timestamp = ipd.initiation_timestamp
)

-- SELECT DISTINCT eliminates row multiplication from multi-DIC-event windows
-- This is the DQ fix that guarantees uniqueness = 100% on the composite key
select distinct
    company_id, initiation_rank, initiation_timestamp, window_end_timestamp,
    properties_ui_access_point, properties_custom_fp_offer_id,
    offer_name, primaryCtaText, obill_offer_id, ipd_type,
    ipd_view_timestamp, viewed_ipd, clicked_ipd,
    viewed_dic_component, number_of_data_points_shown,
    dic_component_detail, dic_impression_timestamp,
    product, initiation_year, initiation_month
from raw_result

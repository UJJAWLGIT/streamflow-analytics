{{
  config(
    materialized = 'incremental',
    incremental_strategy = 'merge',
    unique_key = ['company_id', 'initiation_rank', 'initiation_timestamp'],
    file_format = 'delta',
    tags = ['silver', 'daily'],
    meta = {'owner': 'ujjawl.kumar', 'tier': 'silver', 'step': 3}
  )
}}

/*
  silver.stg_save_attribution — Step 3
  =====================================
  Save outcome per cancel initiation.
  Priority waterfall: CS Save > Cancelled > Upgrade Save > Downgrade Save > Discount Save > Abandoned
*/

with initiations as (
    select * from {{ ref('stg_cancel_initiations') }}
),

-- CS Reactive Saves (7-day window)
cs_saves as (
    select
        rs.company_id,
        1 as contacted_by_cs,
        1 as saved_by_cs
    from {{ ref('raw_cs_reactive_saves') }} rs
    group by 1
),

-- Discount taken (match offer history to IPD obill offer)
discount_taken as (
    select
        ipd.company_id,
        ipd.initiation_timestamp,
        1 as took_discount,
        min(oh.purchase_date) as discount_take_timestamp
    from {{ ref('rpt_ipd_detailed_engagement') }} ipd
    join {{ ref('raw_offer_history') }} oh
        on  oh.company_id = ipd.company_id
        and trim(oh.offer_id) = trim(regexp_replace(ipd.obill_offer_id, '["\"]', ''))
    where ipd.ipd_type = 'Discount IPD'
    group by 1, 2
),

final as (
    select
        ci.company_id,
        ci.initiation_rank,
        ci.initiation_timestamp,
        ci.initiation_date,
        ci.cancel_confirmed,
        ci.confirmation_timestamp,
        coalesce(cs.contacted_by_cs, 0) as contacted_by_cs,
        coalesce(cs.saved_by_cs, 0)     as saved_by_cs,
        coalesce(ci.upgraded, 0)        as upgraded,
        ci.upgrade_timestamp,
        coalesce(ci.downgraded, 0)      as downgraded,
        ci.downgrade_timestamp,
        coalesce(dt.took_discount, 0)   as took_discount,
        dt.discount_take_timestamp,
        -- Priority waterfall
        case
            when coalesce(cs.saved_by_cs, 0)     = 1 then 'CS Save'
            when ci.cancel_confirmed              = 1 then 'Cancelled'
            when coalesce(ci.upgraded, 0)         = 1 then 'Upgrade Save'
            when coalesce(ci.downgraded, 0)       = 1 then 'Downgrade Save'
            when coalesce(dt.took_discount, 0)    = 1 then 'Discount Save'
            else 'Abandoned'
        end as save_attribution,
        -- Saved by abandoning flag
        case
            when coalesce(cs.saved_by_cs, 0) = 0
             and ci.cancel_confirmed          = 0
             and coalesce(ci.upgraded, 0)     = 0
             and coalesce(ci.downgraded, 0)   = 0
             and coalesce(dt.took_discount,0) = 0
            then 'Y' else 'N'
        end as saved_by_abandoning,
        ci.is_accountant_starting_cancellation,
        ci.accountant_id_starting_cancellation
    from initiations ci
    left join cs_saves cs on cs.company_id = ci.company_id
    left join discount_taken dt
        on  dt.company_id           = ci.company_id
        and dt.initiation_timestamp = ci.initiation_timestamp
)

select * from final

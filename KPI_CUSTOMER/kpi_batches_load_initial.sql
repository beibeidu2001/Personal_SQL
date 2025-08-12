CREATE OR REPLACE TABLE `leo-goldrush-data-sandbox.temp_bi.kpi_customer_bathces_load_initial` AS
with
    customer as (
        select
            cust.customer_id,
            cust.customer_uid,
            cust.registration_date,
            cust.etl_created,
            cust.etl_modified,
            cust.acquisition_channel,
            coalesce(fcc.manually_classified, false) as manually_classified,
            ms.predicted_margin,
            ms.cross_product_historical_value_score_current,
            ms.cross_product_historical_value_segment_current,
            ms.sports_historical_value_score_current,
            ms.sports_historical_value_segment_current,
            ms.casino_historical_value_score_current,
            ms.casino_historical_value_segment_current
        from `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` cust
        left join `leo-prod-sports-bi.production_sports_customer_private.fact_customer_classification` fcc
            on cust.customer_id = fcc.customer_id
        left join `leo-prod-sports-bi.production_sports_bi_mart_public.marketing_segmentation_metrics` ms
            on cust.customer_uid = ms.customer_uid
    ),
    last_bet as (
        select distinct
            customer_uid,
            last_betslip_stake
        from `leo-prod-sports-bi.production_sports_bi_mart_private.dim_customer_sportsbook_statistics`
    ),
    settled_bet_counts as (
        select
            fs2.customer_uid,
            count(distinct fsb.bet_id) as settled_bet_count
        from `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook` fs2
        join
            `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook_bet` fsb
            on fsb.src_betslip_uuid = fs2.src_betslip_uuid
        join
            `leo-prod-sports-bi.production_sports_sportsbook_private.mapping_sportsbook_bet_combination_outcome` msbco
            on msbco.src_betslip_uuid = fs2.src_betslip_uuid
            and msbco.bet_id = fsb.bet_id
        join
            `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook_bet_combination` fsbc
            on fsbc.bet_combination_id = msbco.bet_combination_id
            and fsbc.bet_combination_status in ('WON', 'LOST')
        group by customer_uid
    ),
    avg_bet_sb_data as (
        select
            -- avg_bet CTE
            customer_uid,
            coalesce(sum(handle),0) as turnover,
            coalesce(count(distinct src_betslip_uuid),0) as bet_count_total,
            CASE WHEN coalesce(count(distinct src_betslip_uuid),0) = 0 THEN NULL ELSE coalesce(sum(handle),0) / coalesce(count(distinct src_betslip_uuid),0) END as average_bet_amount,
            
            -- sb_data CTE
            count(distinct case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET')
                then bet_id end) as cnt_real_lifetime,
            count(distinct case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') and date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 day
                then bet_id end) as cnt_real_7,
            count(distinct case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') and date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 day
                then bet_id end) as cnt_real_30,
            sum(case when date(datetime(kpi_date, "UTC")) = current_date("UTC") - interval 1 day 
                then ggr else 0 end) as previous_day_ggr,
            case when sum(handle) = 0 then null else sum(case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') then handle else 0 end) * 1.0 / sum(handle)
                end as real_money_pct_lifetime,
            case when sum(case when date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 day then handle else 0 end) = 0
                    then null
                else sum(case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') and date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 day then handle else 0 end)
                    * 1.0 / sum(case when date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 day then handle else 0 end)
                end as real_money_pct_7,
            case when sum(case when date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 day then handle else 0 end) = 0
                    then null
                else sum(case when coalesce(bonus_campaign_type, '') not in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') and date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 day then handle else 0 end)
                    * 1.0 / sum(case when date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 day then handle else 0 end)
                end as real_money_pct_30,
            cast(datetime(min(ticket_delivery_date)) as date) as first_betslip_date,
            cast(datetime(max(ticket_delivery_date)) as date) as last_betslip_date,
            string_agg(distinct sportsbook_source, ',' order by sportsbook_source) as sportsbook_sources
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360`
        where
            betslip_status in ('CLOSED', 'VOIDED', 'CASHED_OUT')
            and (bet_combination_status != 'REJECTED' or bet_combination_status is null)
            or (betslip_status is null and bet_combination_status is null)
        group by customer_uid
    ),
    current_cash_bonus_wallet as (
        select
            customer_uid,
            coalesce(sum(case when wallet_owner_type = 'BONUS' then amount end), 0) as bonus_wallet_balance  -- bonus
        from
            (  -- wallet balance below
                select
                    dw.owner_id as customer_uid,
                    fwb.end_balance as amount,
                    dw.wallet_owner_type,
                    row_number() over (
                        partition by fwb.wallet_id
                        order by fwb.balanceasofdate desc
                    ) as rn
                from
                    `leo-prod-sports-bi.production_sports_wallet_private.dim_wallet` dw
                join
                    `leo-prod-sports-bi.production_sports_wallet_private.fact_wallet_balance` fwb
                    on dw.wallet_id = fwb.wallet_id
                where dw.wallet_owner_type in ('USER', 'BONUS')
            )
        where rn = 1  -- find out the latest balanceofdate of the wallet belongs to customer
        group by customer_uid
    ),
    most_recent_active_campaign_id as (
        select
            fba.customer_uid,
            fba.src_campaign_id,
            ROW_NUMBER() OVER (PARTITION BY fba.customer_uid ORDER BY fba.message_timestamp DESC) AS rn
        from
            `leo-prod-sports-bi.production_sports_analytics_private.fact_bonus_activity` fba
        where
            fba.subtype = 'bonus-dispatch'
            and fba.value = 1
        qualify rn = 1
    ),
    enrolled_bonus_campaigns as (
        select
            customer_uid
            , SUBSTR(STRING_AGG(src_campaign_id, ',' ORDER BY message_timestamp DESC), 1, 10000) AS enrolled_bonus_campaigns
		from
		(
            select
                distinct fba.customer_uid as customer_uid,
                fba.src_campaign_id,
                fba.message_timestamp,
                row_number() over (PARTITION BY fba.customer_uid order by fba.message_timestamp DESC) as rn
            from `leo-prod-sports-bi.production_sports_analytics_private.fact_bonus_activity` fba
            where fba.subtype = 'bonus-dispatch'
                AND fba.value = 1
                AND fba.message_timestamp >= DATE_SUB(current_timestamp(), INTERVAL 180 DAY)
		) T1
		where rn <= 2000
		group by customer_uid
    ),
    pred_rev as (
        select
            player_key as customer_uid,
            pred_real_money_active_days_next30_bonus_0,
            pred_real_money_active_days_next30_bonus_1,
            CASE
                WHEN predicted_revenue_next_30 = 0
                    THEN (predicted_revenue_next_30_bonus -  0.01) / 0.01
                ELSE (predicted_revenue_next_30_bonus -  predicted_revenue_next_30) / predicted_revenue_next_30
            END AS pred_active_days_next30_bonus_lift,
            predicted_revenue_next_30,
            predicted_revenue_next_active_day,
            predicted_revenue_segment
        from `leo-prod-sports-bi.production_sports_bi_mart_private.predicted_revenue`
    ),
    -- returns the predictiom of revenue and bonus in certain timeframe
    missions as (
            select customer_uid
                , sum(payout_value_lc) as tiger_lifetime_cashback_lc
                , count(distinct case when finished_reason = 'COMPLETED' then assignment_id end) as tiger_lifetime_missions_completed
                , sum(
                    case when assignment_completed_timestamp >= DATE_SUB(current_timestamp(), INTERVAL 30 DAY)
                        then payout_value_lc
                    else 0 end
                    ) as tiger_l30_cashback_lc
                , count(distinct 
                    case when assignment_completed_timestamp >= DATE_SUB(current_timestamp(), INTERVAL 30 DAY) and finished_reason = 'COMPLETED'
                        then assignment_id 
                    end
                    ) as tiger_l30_missions_completed
            from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_mission_activity` m
            group by customer_uid
        ),
    available_bonuses as (
        select
            customer_uid
            ,case when sum(case when bonus_campaign_type = 'RISK_FREE_BET' THEN bonus_idle else 0 end) > 0 
                then 'true' else 'false' END as available_protected_bets
            ,case when sum(case when bonus_campaign_type in ('FREE_BET', 'RISK_FREE_BET_FREE_BET') THEN bonus_idle else 0 end) > 0 
                then 'true' else 'false' END as available_bet_credits
            ,cast(null as string)  as available_deposit_matches
            ,cast(null as string)  as available_bet_and_gets
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_bonus_360`
        group by customer_uid
    ),
---- ems table start here ----
    kpi_transaction_limited as (
        SELECT
            kt.customer_uid,
            kt.handle,
            kt.ggr, 
            kt.ngr,
            kb.bonus_cost,
            kb.bonus_claimed,
            kb.wallet_transaction_type,
            kb.wallet_transaction_type_action,
            kb.transaction_created_date,
            src_bonus_id,
		    kt.src_campaign_id, dc.name as campaign_name, 
            dc.type as campaign_type
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360` kt
        left join `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_bonus_360` kb on kb.customer_uid = kt.customer_uid
        left join `leo-prod-sports-bi.production_sports_wallet_private.dim_campaign` dc
            on kt.src_campaign_id = dc.src_campaign_id
        -- where kt.ngr is null
    ),
    list as (
        select
            customer_uid,
            campaign_name,
            src_bonus_id,
            campaign_type,
            case
                when
                    campaign_name like '%%_NDB_%%'
                    and campaign_type = 'MONEY'
                    then campaign_type || '_NDB'
                when
                    campaign_name not like '%%_NDB_%%'
                    and campaign_type = 'MONEY'
                    then campaign_type || '_DB'
                when campaign_type is null
                    then 'Non_Campaign'
                else campaign_type
            end as bonus_type,
            transaction_created_date
        from kpi_transaction_limited
        where
            wallet_transaction_type in ('BONUS_DEPOSIT')
    ),
    most_common_bonus_type as (
        select
            customer_uid,
            bonus_type,
            count(distinct src_bonus_id) cnt_promos_by_type,
            row_number() over (
                partition by customer_uid
                order by count(distinct src_bonus_id), bonus_type desc
            ) as rnbr
        from list
        group by 1, 2
        qualify rnbr = 1
    ),
    most_recent_bonus_type as (
        select
            customer_uid,
            bonus_type,
            campaign_name,
            row_number() over (
                partition by customer_uid order by transaction_created_date desc
            ) as recent_bonus_rnbr,
        from list
        qualify recent_bonus_rnbr = 1
    ),
    lt_l30 as (
        select
            customer_uid,
            sum(handle) as handle_lt,
            sum(ggr) as ggr_lt,
            sum(bonus_cost) as bonus_cost_lt,
            sum(ngr) as ngr_lt,
            sum(bonus_claimed) as bonus_claimed,
            count(distinct case when wallet_transaction_type_action like 'BET_SLIP_PLACEMENT' then date(transaction_created_date) end) as active_days_lt,
            sum(case when date_diff(current_date(), date(transaction_created_date), day) <= 30 then handle end) as handle_l30,
            sum(case when date_diff(current_date(), date(transaction_created_date), day) <= 30 then ggr end) as ggr_l30,
            sum(case when date_diff(current_date(), date(transaction_created_date), day) <= 30 then bonus_cost end) as bonus_cost_l30,
            sum(case when date_diff(current_date(), date(transaction_created_date), day) <= 30 then ngr end) as ngr_l30,
            count(distinct case when wallet_transaction_type_action like 'BET_SLIP_PLACEMENT' and date_diff(current_date(), date(transaction_created_date), day) <= 30
                    then date(transaction_created_date) end) as active_days_l30
        from kpi_transaction_limited as ktl
        group by customer_uid
    ),
    parlay_bet_flag as (
        select
            ks.customer_uid,
            logical_or(ks.wager_type = 'PARLAY') as placed_a_parlay_bet,
            true as bet_sports
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360` ks
        WHERE ks.handle > 0
                and (
                    (ks.betslip_status in ('CLOSED', 'VOIDED', 'CASHED_OUT') and 
                    (ks.bet_combination_status != 'REJECTED' or ks.bet_combination_status is null))
                    or (ks.betslip_status is null and ks.bet_combination_status is null)
                )
        group by ks.customer_uid
    ),
    customer_sports as (
        select
            customer_uid,
            case
                when
                    league in (
                        'NFL',
                        'NFL - Unreal Matchups',
                        'NFL Preseason',
                        'NFL Specials',
                        'NFL Weekly Specials',
                        'Season Specials',
                        'Specials',
                        'Weekly Specials'
                    )
                then 'NFL'
                when
                    league in (
                        'NCAAF',
                        'NCAAF Futures',
                        'NCAA Division I, FCS National Championship'
                    )
                then 'NCAAF'
                when
                    league in (
                        'MLB',
                        'MLB All Star Week',
                        'MLB Specials',
                        'MLB Spring Training',
                        'Triple-A East',
                        'Triple-A West'
                    )
                then 'MLB'
                when league in ('NHL', 'NHL All Star Game', 'NHL Preseason')
                then 'NHL'
                when
                    league in (
                        'NBA',
                        'NBA All Star Game',
                        'NBA Preseason',
                        'NBA Specials',
                        'NBA Summer League'
                    )
                then 'NBA'
                else sport
            end as league_group,
            count(distinct src_betslip_uuid) as num_bets
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360` ks
        where
            date(ticket_delivery_date) >= date_sub(current_date(), interval 12 month)
            and (league in ('NBA', 'MLB', 'NCAAF', 'NFL', 'NHL') or sport in ('Soccer', 'Tennis'))
            and betslip_status in ('CLOSED', 'VOIDED', 'CASHED_OUT')
        group by customer_uid, league_group
    ),
    combine_sports as (
        select
            customer_uid,
            upper(string_agg(league_group, '/' order by league_group desc)) as league_list
        from customer_sports
        group by customer_uid
    )

---- Final Select Statement ----
select distinct
    c.customer_id,
    c.customer_uid,
    c.registration_date as tiger_registration_date,
    kpi.created as lv_registration_date,
    c.etl_created,
    c.etl_modified,
    kpi.age_group,
    kpi.profile_country_name,
    kpi.brand_alias,
    absbd.first_betslip_date,
    absbd.last_betslip_date,
    absbd.sportsbook_sources,
    absbd.average_bet_amount,
    absbd.bet_count_total as lifetime_bets,
    lt_l30.bonus_claimed as bonus_amount_claimed, -- TODO: fix (bonus_amount_claimed_lc)
    c.manually_classified,
    c.cross_product_historical_value_score_current,
    c.cross_product_historical_value_segment_current,
    c.sports_historical_value_score_current,
    c.sports_historical_value_segment_current,
    c.casino_historical_value_score_current,
    c.casino_historical_value_segment_current,
    coalesce(sbc.settled_bet_count, 0) as settled_bet_count,
    coalesce(ccwb.bonus_wallet_balance, 0) as bonus_wallet_balance, -- TODO: fix
    mrac.customer_uid is not null as is_bonus_campaign_enrolled,
    mrac.src_campaign_id as most_recent_active_campign_id,
    ebc.enrolled_bonus_campaigns,
    kpi.ftd_date is not null as deposited,
    kpi.ftd_date,
    pr.pred_real_money_active_days_next30_bonus_0 as pred_real_money_active_days_next30_bonus_0,
    pr.pred_real_money_active_days_next30_bonus_1 as pred_real_money_active_days_next30_bonus_1,
    CAST(pr.pred_active_days_next30_bonus_lift AS FLOAT64) as pred_active_days_next30_bonus_lift,
    pr.predicted_revenue_next_30,
    pr.predicted_revenue_next_active_day,
    pr.predicted_revenue_segment,
    --there was previous Tipico logic but setting to false for now
    case when kpi.classification_name_current in ('D','F','KAMBI - Wiseguy','KAMBI - Arber', 'Wiseguy','Arber','Monitoring', 'Monitoring 1', 'Monitoring 2') then 'True' else 'False' end as do_not_bonus,
    c.predicted_margin,
    absbd.real_money_pct_lifetime,
    absbd.real_money_pct_7,
    absbd.real_money_pct_30,
    absbd.cnt_real_lifetime,
    absbd.cnt_real_7,
    absbd.cnt_real_30,
    ms.tiger_lifetime_cashback_lc,
    ms.tiger_lifetime_missions_completed,
    ms.tiger_l30_cashback_lc,
    ms.tiger_l30_missions_completed,
    abb.available_protected_bets,
    abb.available_bet_credits,
    abb.available_deposit_matches,
    abb.available_bet_and_gets,
    -- ems starting here
    mcbt.bonus_type as most_common_bonus_type,
    most_recent_bonus_type.bonus_type as most_recent_bonus_type,
    most_recent_bonus_type.campaign_name as most_recent_campaign_name,
    lt_l30.ggr_lt,
    lt_l30.ngr_lt,
    lt_l30.handle_lt,
    lt_l30.active_days_lt,
    lt_l30.active_days_l30,
    lt_l30.handle_l30,
    lt_l30.ggr_l30,
    lt_l30.ngr_l30,
    lt_l30.bonus_cost_lt,
    lt_l30.bonus_cost_l30,
    case
        when lt_l30.handle_l30 = 0 then null
        else lt_l30.ggr_l30 / lt_l30.handle_l30
    end as ggr_to_handle_l30,
    case
        when lt_l30.handle_lt = 0 then null 
        else lt_l30.ggr_lt / lt_l30.handle_lt
    end as ggr_to_handle_lt,
    case
        when lt_l30.active_days_lt = 0 then null
        else lt_l30.handle_lt / lt_l30.active_days_lt
    end as active_daily_handle_lt,
    case
        when lt_l30.ggr_l30 = 0 then null 
        else lt_l30.bonus_cost_l30 / lt_l30.ggr_l30
    end as bonus_to_ggr_l30,
    case
        when lt_l30.ggr_lt = 0 then null
         else lt_l30.bonus_cost_lt / lt_l30.ggr_lt
    end as bonus_to_ggr_lt,
    case when pbf.bet_sports then 'yes' else 'no' END AS bet_sports,
    case when pbf.placed_a_parlay_bet then 'yes' else 'no' END AS placed_a_parlay_bet,
    kpi.classification_name_current,
    cs.league_list,
    kpi.is_locked,
    kpi.flag_rg,
    kpi.locked_until,
    kpi.predicted_risk_category_latest,
    kpi.profile_welcome_offer, -- acqusition vertical
    c.acquisition_channel,
    kpi.self_exclusion_flag,
    kpi.is_bonus_abuser,
    kpi.is_suspended,
    kpi.person_id,
    kpi.license_uid,
    kpi.legal_entity,
    kpi.deposit_disabled,
    kpi.login_disabled,
    kpi.play_sports_disabled,
    kpi.receive_sports_promotions_disabled,
    kpi.withdraw_disabled,
    kpi.longest_restriction,
    kpi.longest_restriction_valid_to,
    kpi.longest_restriction_type,
    kpi.player_sb_ngr_last12months,
    CASE
        WHEN gta.customer_uid IS NOT NULL THEN TRUE
        ELSE FALSE
    END AS test_account_flag,
    current_timestamp() as data_update_time
from customer c  -- the big query being defined before and join everything below
left join last_bet lb on c.customer_uid = lb.customer_uid
left join avg_bet_sb_data absbd on absbd.customer_uid = c.customer_uid
left join settled_bet_counts sbc on sbc.customer_uid = c.customer_uid
left join current_cash_bonus_wallet ccwb on ccwb.customer_uid = c.customer_uid
left join most_recent_active_campaign_id mrac on mrac.customer_uid = c.customer_uid
left join enrolled_bonus_campaigns ebc on ebc.customer_uid = c.customer_uid
left join `leo-goldrush-data-sandbox.temp_bi.kpi_customer_initial` kpi on c.customer_uid = kpi.customer_uid -- this is the temp table
left join pred_rev pr on c.customer_uid = pr.customer_uid
left join missions ms on c.customer_uid = ms.customer_uid
left join available_bonuses abb on c.customer_uid = abb.customer_uid
left join most_common_bonus_type as mcbt on mcbt.customer_uid = c.customer_uid
-- -----------------------------------------------------
left join most_recent_bonus_type
    on most_recent_bonus_type.customer_uid = c.customer_uid
-- -----------------------------------------------------
left join lt_l30 as lt_l30 on kpi.customer_uid = lt_l30.customer_uid
-- -----------------------------------------------------
LEFT JOIN parlay_bet_flag pbf ON kpi.customer_uid = pbf.customer_uid
-- -----------------------------------------------------
left join combine_sports as cs on cs.customer_uid = kpi.customer_uid
-- -----------------------------------------------------
LEFT JOIN (
  SELECT DISTINCT dcs.customer_uid, dcs.customer_id
  FROM `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` AS dcs
  WHERE is_test_player = TRUE
) AS gta
  ON kpi.customer_uid = gta.customer_uid

CREATE OR REPLACE TABLE `leo-goldrush-data-sandbox.temp_bi.kpi_customer_combined` AS
WITH ks360 AS (
    SELECT 
        kt.betslip_status,
        kt.bet_combination_status,
        kt.src_betslip_uuid,
        kt.bonus_campaign_type,
        kt.bet_id,
        kt.kpi_date,
        kt.sportsbook_source,
        kt.wager_type,
        kt.league,
        kt.sport,
        kt.src_campaign_id,
        kt.customer_uid,
        kt.handle,
        kt.id,
        kt.ggr,
        kt.ngr,
        kt.bonus_total AS bonus_cost,
        kt.ticket_delivery_date,
        dc.name AS campaign_name,
        dc.type AS campaign_type
    FROM `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360` kt
    LEFT JOIN `leo-prod-sports-bi.production_sports_wallet_private.dim_campaign` dc
    ON kt.src_campaign_id = dc.src_campaign_id
    WHERE (kt.betslip_status IN ('CLOSED', 'VOIDED', 'CASHED_OUT') AND (kt.bet_combination_status != 'REJECTED' OR kt.bet_combination_status IS NULL))
        OR (kt.betslip_status IS NULL AND kt.bet_combination_status IS NULL)
)
, lt_l30 AS (
    SELECT customer_uid,
        sum(handle) AS handle_lt,
        sum(ggr) AS ggr_lt,
        sum(bonus_cost) AS bonus_cost_lt,
        sum(ngr) AS ngr_lt,
        count(DISTINCT date(ticket_delivery_date)) AS active_days_lt,
        sum(CASE WHEN date_diff(current_date(), date(ticket_delivery_date), day) <= 30 THEN handle END) AS handle_l30,
        sum(CASE WHEN date_diff(current_date(), date(ticket_delivery_date), day) <= 30 THEN ggr END) AS ggr_l30,
        sum(CASE WHEN date_diff(current_date(), date(ticket_delivery_date), day) <= 30 THEN bonus_cost END) AS bonus_cost_l30,
        sum(CASE WHEN date_diff(current_date(), date(ticket_delivery_date), day) <= 30 THEN ngr END) AS ngr_l30,
        count(DISTINCT CASE WHEN date_diff(current_date(), date(ticket_delivery_date), day) <= 30 THEN date(ticket_delivery_date) END) AS active_days_l30,
        min(date(ticket_delivery_date)) AS first_sports_bet_date,
        max(date(ticket_delivery_date)) AS last_sports_bet_date,
        count(DISTINCT src_betslip_uuid) AS bet_count_total,
        CASE WHEN coalesce(count(distinct src_betslip_uuid),0) = 0 THEN NULL ELSE coalesce(sum(handle),0) / coalesce(count(distinct src_betslip_uuid),0) END as average_bet_amount,
        count(DISTINCT CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET') THEN bet_id END) AS real_money_bet_cnt_lifetime,
        count(DISTINCT CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET')
                AND date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 DAY THEN bet_id END) AS real_money_bet_cnt_last7,
        count(DISTINCT CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET')
                AND date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 DAY THEN bet_id END) AS real_money_bet_cnt_last30,
        sum(CASE WHEN date(datetime(kpi_date, "UTC")) = current_date("UTC") - interval 1 DAY THEN ggr ELSE 0 END) AS previous_day_ggr,
        CASE WHEN sum(handle) = 0 THEN NULL
            ELSE sum(CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET') THEN handle ELSE 0 END) * 1.0 / sum(handle)
        END AS real_money_pct_lifetime,
        CASE WHEN sum(CASE WHEN date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 DAY THEN handle ELSE 0 END) = 0 THEN NULL
            ELSE sum(CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET')
                        AND date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 DAY THEN handle ELSE 0 END) * 1.0
                        / sum(CASE WHEN date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 7 DAY THEN handle ELSE 0 END)
        END AS real_money_pct_7,
        CASE WHEN sum(CASE WHEN date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 DAY THEN handle ELSE 0 END) = 0 THEN NULL
            ELSE sum(CASE WHEN coalesce(bonus_campaign_type, '') NOT IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET')
                        AND date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 DAY THEN handle ELSE 0 END) * 1.0
                        / sum(CASE WHEN date(datetime(kpi_date, "UTC")) >= current_date("UTC") - interval 30 DAY THEN handle ELSE 0 END)
        END AS real_money_pct_30,
        string_agg(DISTINCT sportsbook_source, ',' ORDER BY sportsbook_source) AS sportsbook_sources,
        logical_or(wager_type = 'PARLAY' and handle > 0) AS placed_a_parlay_bet,
        TRUE AS bet_sports
    FROM ks360
    GROUP BY customer_uid
)
, league_agg AS (
    SELECT customer_uid,
        CASE
            WHEN league IN ('NFL', 'NFL - Unreal Matchups', 'NFL Preseason', 'NFL Specials', 'NFL Weekly Specials', 'Season Specials', 'Specials', 'Weekly Specials')
                THEN 'NFL'
            WHEN league IN ('NCAAF','NCAAF Futures', 'NCAA Division I, FCS National Championship') 
                THEN 'NCAAF'
            WHEN league IN ('MLB', 'MLB All Star Week', 'MLB Specials', 'MLB Spring Training', 'Triple-A East', 'Triple-A West')
                THEN 'MLB'
            WHEN league IN ('NHL', 'NHL All Star Game', 'NHL Preseason')
                THEN 'NHL'
            WHEN league IN ('NBA', 'NBA All Star Game', 'NBA Preseason', 'NBA Specials', 'NBA Summer League')
                THEN 'NBA'
            ELSE sport
        END AS league_group,
        count(DISTINCT src_betslip_uuid) AS num_bets
    FROM ks360 ks
    WHERE date(ticket_delivery_date) >= date_sub(current_date(), interval 12 MONTH)
        AND (league IN ('NBA', 'MLB', 'NCAAF', 'NFL', 'NHL')
        OR sport IN ('Soccer', 'Tennis'))
    GROUP BY customer_uid, league_group
)
, theoretical_margin AS (
    SELECT DISTINCT UUID AS customer_uid,
        margin,
        margin_prematch,
        margin_live,
        betsize_prematch,
        betsize_live
    FROM `leo-prod-sports-bi.production_inwt_private.rating`
)
, customer_session AS (
    SELECT customer_uid,
        avg(session_length_seconds / 60) AS avg_session_min,
        avg(session_length_seconds) AS avg_session_sec,
        count(DISTINCT session_group_id) AS total_sessions
    FROM `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_session_view`
    GROUP BY customer_uid
)
, missions AS (
    SELECT customer_uid,
        count(DISTINCT CASE WHEN assignment_completed_timestamp IS NULL THEN assignment_id END) AS active_mission_count,
        count(DISTINCT CASE WHEN finished_reason = 'COMPLETED' THEN assignment_id END) AS completed_mission_count,
        sum(payout_value_lc) AS tiger_lifetime_cashback_lc,
        count(DISTINCT CASE WHEN finished_reason = 'COMPLETED' THEN assignment_id END) AS tiger_lifetime_missions_completed,
        sum(CASE WHEN assignment_completed_timestamp >= date_sub(current_timestamp(), interval 30 DAY) THEN payout_value_lc
            ELSE 0 END) AS tiger_l30_cashback_lc,
        count(DISTINCT CASE WHEN assignment_completed_timestamp >= date_sub(current_timestamp(), interval 30 DAY) AND finished_reason = 'COMPLETED' 
            THEN assignment_id END) AS tiger_l30_missions_completed
    FROM `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_mission_activity`
    GROUP BY customer_uid
),
last_bet AS (
    SELECT DISTINCT 
        customer_uid,
        last_betslip_stake
    FROM `leo-prod-sports-bi.production_sports_bi_mart_private.dim_customer_sportsbook_statistics`
)
, settled_bet_counts AS (
    SELECT
        fs2.customer_uid,
        count(DISTINCT fsb.bet_id) AS settled_bet_count
    FROM `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook` fs2
    JOIN `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook_bet` fsb 
        ON fsb.src_betslip_uuid = fs2.src_betslip_uuid
    JOIN `leo-prod-sports-bi.production_sports_sportsbook_private.mapping_sportsbook_bet_combination_outcome` msbco
        ON msbco.src_betslip_uuid = fs2.src_betslip_uuid
        AND msbco.bet_id = fsb.bet_id
    JOIN `leo-prod-sports-bi.production_sports_sportsbook_private.fact_sportsbook_bet_combination` fsbc
        ON fsbc.bet_combination_id = msbco.bet_combination_id
        AND fsbc.bet_combination_status IN ('WON', 'LOST')
    GROUP BY customer_uid
)
, current_cash_bonus_wallet AS (
    SELECT
        customer_uid,
        coalesce(sum(CASE WHEN wallet_owner_type = 'BONUS' THEN amount END), 0) AS bonus_wallet_balance
    FROM
        (SELECT 
            dw.owner_id AS customer_uid,
            fwb.end_balance AS amount,
            dw.wallet_owner_type,
            row_number() OVER (PARTITION BY fwb.wallet_id ORDER BY fwb.balanceasofdate DESC) AS rn
        FROM `leo-prod-sports-bi.production_sports_wallet_private.dim_wallet` dw
        JOIN `leo-prod-sports-bi.production_sports_wallet_private.fact_wallet_balance` fwb ON dw.wallet_id = fwb.wallet_id
        WHERE dw.wallet_owner_type IN ('USER', 'BONUS'))
    WHERE rn = 1
    GROUP BY customer_uid
)
, most_recent_active_campaign_id AS (
    SELECT 
        fba.customer_uid,
        fba.src_campaign_id,
        row_number() OVER (PARTITION BY fba.customer_uid ORDER BY fba.message_timestamp DESC) AS rn
    FROM `leo-prod-sports-bi.production_sports_analytics_private.fact_bonus_activity` fba
    WHERE fba.subtype = 'bonus-dispatch'
        AND fba.value = 1 QUALIFY rn = 1
)
, enrolled_bonus_campaigns AS (
    SELECT 
        customer_uid,
        substr(string_agg(src_campaign_id, ',' ORDER BY message_timestamp DESC), 1, 10000) AS enrolled_bonus_campaigns
    FROM
        (SELECT DISTINCT 
            fba.customer_uid AS customer_uid,
            fba.src_campaign_id,
            fba.message_timestamp,
            row_number() OVER (PARTITION BY fba.customer_uid ORDER BY fba.message_timestamp DESC) AS rn
        FROM `leo-prod-sports-bi.production_sports_analytics_private.fact_bonus_activity` fba
        WHERE fba.subtype = 'bonus-dispatch'
            AND fba.value = 1
            AND fba.message_timestamp >= date_sub(current_timestamp(), interval 180 DAY)) t1
    WHERE rn <= 2000
    GROUP BY customer_uid
)
, pred_rev AS (
    SELECT 
        player_key AS customer_uid,
        pred_real_money_active_days_next30_bonus_0,
        pred_real_money_active_days_next30_bonus_1,
        CASE
            WHEN predicted_revenue_next_30 = 0 THEN (predicted_revenue_next_30_bonus - 0.01) / 0.01
            ELSE (predicted_revenue_next_30_bonus - predicted_revenue_next_30) / predicted_revenue_next_30
        END AS pred_active_days_next30_bonus_lift,
        predicted_revenue_next_30,
        predicted_revenue_next_active_day,
        predicted_revenue_segment
    FROM `leo-prod-sports-bi.production_sports_bi_mart_private.predicted_revenue`
)
, available_bonuses AS (
    SELECT 
        customer_uid,
        CASE WHEN sum(CASE WHEN bonus_campaign_type = 'RISK_FREE_BET' THEN bonus_idle ELSE 0 END) > 0 THEN 'true'
            ELSE 'false'
        END AS available_protected_bets,
        CASE WHEN sum(CASE WHEN bonus_campaign_type IN ('FREE_BET', 'RISK_FREE_BET_FREE_BET') THEN bonus_idle ELSE 0 END) > 0 THEN 'true'
            ELSE 'false'
        END AS available_bet_credits,
        cast(NULL AS string) AS available_deposit_matches,
        cast(NULL AS string) AS available_bet_and_gets
    FROM `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_bonus_360`
    GROUP BY customer_uid
)
, list AS (
    SELECT 
        b.customer_uid,
        b.src_bonus_id,
        dc.name as campaign_name,
        dc.type as campaign_type,
        CASE
            WHEN dc.name like '%%_NDB_%%'
                AND dc.type = 'MONEY' THEN dc.type || '_NDB'
            WHEN dc.name not like '%%_NDB_%%'
                AND dc.type = 'MONEY' THEN dc.type || '_DB'
            WHEN dc.type IS NULL THEN 'Non_Campaign'
            ELSE dc.type
        END AS bonus_type,
        b.transaction_created_date
    FROM `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_bonus_360` b
    left join `leo-prod-sports-bi.production_sports_wallet_private.dim_campaign` dc
        on b.src_campaign_id = dc.src_campaign_id
    WHERE b.wallet_transaction_type IN ('BONUS_DEPOSIT')
)
, most_common_bonus_type AS (
    SELECT 
        customer_uid,
        bonus_type,
        count(DISTINCT src_bonus_id) cnt_promos_by_type,
        row_number() OVER (PARTITION BY customer_uid ORDER BY count(DISTINCT src_bonus_id), bonus_type DESC) AS rnbr
    FROM list
    GROUP BY 1,2 
    QUALIFY rnbr = 1
)
, most_recent_bonus_type AS (
    SELECT
        customer_uid,
        bonus_type,
        campaign_name,
        row_number() over (partition by customer_uid order by transaction_created_date desc) as recent_bonus_rnbr
    FROM list
    QUALIFY recent_bonus_rnbr = 1
)
, combine_sports AS (
    SELECT
        customer_uid,
        upper(string_agg(league_group, '/' ORDER BY league_group DESC)) AS league_list
    FROM league_agg
    GROUP BY customer_uid
)
SELECT DISTINCT
    tm.margin AS predicted_margin,
    tm.margin_prematch AS margin_prematch_predicted,
    tm.margin_live AS margin_live_predicted,
    tm.betsize_prematch AS avg_prematch_betslip_predicted,
    tm.betsize_live AS avg_live_betslip_predicted,
    lt_l30.first_sports_bet_date,
    lt_l30.last_sports_bet_date,
    lt_l30.average_bet_amount,
    lt_l30.bet_count_total AS lifetime_bets,
    lt_l30.real_money_pct_lifetime,
    lt_l30.real_money_pct_7,
    lt_l30.real_money_pct_30,
    lt_l30.real_money_bet_cnt_lifetime,
    lt_l30.real_money_bet_cnt_last7,
    lt_l30.real_money_bet_cnt_last30,
    lt_l30.sportsbook_sources,
    cs.avg_session_min,
    cs.avg_session_sec,
    cs.total_sessions,
    COALESCE(m.active_mission_count,0) as active_mission_count,
    m.completed_mission_count,
    m.tiger_lifetime_cashback_lc,
    m.tiger_l30_cashback_lc,
    m.tiger_l30_missions_completed,
    coalesce(ccwb.bonus_wallet_balance, 0) AS bonus_wallet_balance,
    coalesce(sbc.settled_bet_count, 0) as settled_bet_count,
    mrac.customer_uid IS NOT NULL AS is_bonus_campaign_enrolled,
    mrac.src_campaign_id AS most_recent_active_campaign_id,
    ebc.enrolled_bonus_campaigns,
    pr.pred_real_money_active_days_next30_bonus_0,
    pr.pred_real_money_active_days_next30_bonus_1,
    cast(pr.pred_active_days_next30_bonus_lift AS float64) AS pred_active_days_next30_bonus_lift,
    pr.predicted_revenue_next_30,
    pr.predicted_revenue_next_active_day,
    pr.predicted_revenue_segment,
    abb.available_protected_bets,
    abb.available_bet_credits,
    abb.available_deposit_matches,
    abb.available_bet_and_gets,
    mcbt.bonus_type AS most_common_bonus_type,
    mrt.bonus_type AS most_recent_bonus_type,
    mrt.campaign_name AS most_recent_campaign_name,
    lt_l30.ggr_lt as ggr,
    lt_l30.ngr_lt as ngr,
    lt_l30.handle_lt,
    lt_l30.active_days_lt,
    lt_l30.handle_l30,
    lt_l30.ggr_l30,
    lt_l30.previous_day_ggr,
    lt_l30.ngr_l30,
    lt_l30.bonus_cost_lt,
    lt_l30.bonus_cost_l30,
    lt_l30.active_days_l30,
    CASE
        WHEN lt_l30.handle_l30 = 0 THEN NULL
        ELSE lt_l30.ggr_l30 / lt_l30.handle_l30
    END AS ggr_to_handle_l30,
    CASE
        WHEN lt_l30.handle_lt = 0 THEN NULL
        ELSE lt_l30.ggr_lt / lt_l30.handle_lt
    END AS ggr_to_handle_lt,
    CASE
        WHEN lt_l30.active_days_lt = 0 THEN NULL
        ELSE lt_l30.handle_lt / lt_l30.active_days_lt
    END AS active_daily_handle_lt,
    CASE
        WHEN lt_l30.ggr_l30 = 0 THEN NULL
        ELSE lt_l30.bonus_cost_l30 / lt_l30.ggr_l30
    END AS bonus_to_ggr_l30,
    CASE
        WHEN lt_l30.ggr_lt = 0 THEN NULL
        ELSE lt_l30.bonus_cost_lt / lt_l30.ggr_lt
    END AS bonus_to_ggr_lt,
    CASE
        WHEN lt_l30.bet_sports THEN 'yes'
        ELSE 'no'
    END AS bet_sports,
    CASE
        WHEN lt_l30.placed_a_parlay_bet THEN 'yes'
        ELSE 'no'
    END AS placed_a_parlay_bet,
    csports.league_list,
    ss.totalaveragecombi AS average_combination_length,
    ss.first_betslip_stake AS first_sports_bet_amount,
    ss.last_betslip_stake AS last_sports_bet_amount,
    coalesce(rcc.classification_name, dc.punter_category_current) AS classification_name_current,
    msm.daily_active_handle,
    msm.lifetime_handle,
    dc.customer_id,
    dc.customer_uid,
    dc.registration_date AS tiger_registration_date,
    dc.created AS lv_registration_date,
    dc.etl_created,
    dc.etl_modified,
    dc.acquisition_channel,
    CASE WHEN gta.customer_uid IS NOT NULL THEN TRUE ELSE FALSE END AS test_account_flag,
    dc.sb_ngr_percentile_last12months,
    dc.profile_month_of_birth,
    dc.age_group,
    dc.profile_country_uid,
    dc.profile_country_name,
    dc.profile_gender,
    dc.is_locked,
    dc.flag_rg,
    dc.locked_until,
    dc.predicted_risk_category_latest,
    dc.profile_welcome_offer,
    dc.self_exclusion_flag,
    dc.is_bonus_abuser,
    dc.is_suspended,
    dc.person_id,
    dc.license_uid,
    dc.legal_entity,
    dc.deposit_disabled,
    dc.login_disabled,
    dc.play_sports_disabled,
    dc.receive_sports_promotions_disabled,
    dc.withdraw_disabled,
    dc.longest_restriction,
    dc.longest_restriction_valid_to,
    dc.longest_restriction_type,
    dc.player_sb_ngr_last12months,
    dc.player_cs_ngr_last12months,
    dc.alias,
    dc.brand_alias,
    msm.casino_historical_value_score_current,
    msm.casino_historical_value_segment_current,
    msm.cross_product_historical_value_score_current,
    msm.cross_product_historical_value_segment_current,
    fcc.manually_classified,
    dc.ftd_date,
    dc.ftd_date IS NOT NULL AS deposited,
    msm.sports_historical_value_score_current,
    msm.sports_historical_value_segment_current,
    cast(NULL AS string) AS welcome_offer,
    cast(NULL AS string) AS welcome_offer_short_name,
    m.tiger_lifetime_missions_completed,
    CASE WHEN coalesce(rcc.classification_name, dc.punter_category_current) IN ('D', 'F', 'KAMBI - Wiseguy', 'KAMBI - Arber', 'Wiseguy',
                                                                                'Arber', 'Monitoring', 'Monitoring 1', 'Monitoring 2') THEN 'True'
        ELSE 'False'
    END AS do_not_bonus,
    current_timestamp() AS data_update_time
FROM `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` AS dc
-- LEFT JOIN ks360 on ks360.customer_uid = dc.customer_uid
LEFT JOIN `leo-prod-sports-bi.production_sports_customer_private.fact_customer_classification` AS fcc ON fcc.customer_id = dc.customer_id
LEFT JOIN `leo-prod-sports-bi.production_sports_customer_private.ref_customer_classification` AS rcc ON rcc.classification_id = fcc.classification_id
LEFT JOIN `leo-prod-sports-bi.production_sports_bi_mart_private.dim_customer_sportsbook_statistics` AS ss ON dc.customer_uid = ss.customer_uid
LEFT JOIN theoretical_margin AS tm ON tm.customer_uid = dc.customer_uid
LEFT JOIN `leo-prod-sports-bi.production_sports_bi_mart_public.marketing_segmentation_metrics` AS msm ON dc.customer_uid = msm.customer_uid
LEFT JOIN customer_session AS cs ON dc.customer_uid = cs.customer_uid
LEFT JOIN missions AS m ON dc.customer_uid = m.customer_uid
LEFT JOIN last_bet lb ON msm.customer_uid = lb.customer_uid
LEFT JOIN settled_bet_counts sbc ON sbc.customer_uid = msm.customer_uid
LEFT JOIN current_cash_bonus_wallet ccwb ON ccwb.customer_uid = msm.customer_uid
LEFT JOIN most_recent_active_campaign_id mrac ON mrac.customer_uid = msm.customer_uid
LEFT JOIN enrolled_bonus_campaigns ebc ON ebc.customer_uid = msm.customer_uid
LEFT JOIN pred_rev pr ON msm.customer_uid = pr.customer_uid
LEFT JOIN available_bonuses abb ON msm.customer_uid = abb.customer_uid
LEFT JOIN most_common_bonus_type AS mcbt ON mcbt.customer_uid = msm.customer_uid
LEFT JOIN most_recent_bonus_type AS mrt ON mrt.customer_uid = msm.customer_uid
LEFT JOIN lt_l30 AS lt_l30 ON msm.customer_uid = lt_l30.customer_uid
LEFT JOIN combine_sports AS csports ON csports.customer_uid = msm.customer_uid
LEFT JOIN (
    SELECT DISTINCT dcs.customer_uid, dcs.customer_id
    FROM `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` AS dcs
    WHERE is_test_player = TRUE
) AS gta
ON msm.customer_uid = gta.customer_uid

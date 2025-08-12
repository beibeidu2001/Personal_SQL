CREATE OR REPLACE TABLE `leo-goldrush-data-sandbox.temp_bi.kpi_customer_initial` AS
with theoretical_margin as (
        select distinct
            uuid as customer_uid,
            margin,
            margin_prematch,
            margin_live,
            betsize_prematch,
            betsize_live
        FROM `leo-prod-sports-bi.production_inwt_private.rating`
    ),
    ngr as (
        select 
              dc.customer_id
            , sum(ngr) as ngr
            , sum(ggr) as ggr
            , min(date(ticket_delivery_date)) as first_sports_bet_date
            , max(date(ticket_delivery_date)) as last_sports_bet_date
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_sportsbook_360` kpi
        left join `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` as dc
        on kpi.customer_uid = dc.customer_uid
        where (
                (
                    betslip_status in ('CLOSED', 'VOIDED', 'CASHED_OUT')
                    and (bet_combination_status != 'REJECTED' or bet_combination_status is null )
                )
                or (betslip_status is null and bet_combination_status is null)
              ) 
        group by 1
    ),
    customer_session as (
        select
            customer_uid
            , avg(session_length_seconds/60) as avg_session_min
            , avg(session_length_seconds) as avg_session_sec
            , count(distinct session_group_id) as total_sessions
        from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_session_view` s
        group by customer_uid
    ),
    missions as (
            select customer_uid
                , count(distinct case when assignment_completed_timestamp is null then assignment_id end) as active_mission_count
                , count(distinct case when finished_reason = 'COMPLETED' then assignment_id end) as completed_mission_count
            from `leo-prod-sports-bi.production_sports_bi_mart_public.kpi_mission_activity` m
            group by customer_uid
        )
        select
            dc.customer_id,
            dc.customer_uid,
            dc.alias,
            coalesce(rcc.classification_name,dc.punter_category_current) as classification_name_current,
            margin as predicted_margin,
            margin_prematch as margin_prematch_predicted,
            margin_live as margin_live_predicted,
            betsize_prematch as avg_prematch_betslip_predicted,
            betsize_live as avg_live_betslip_predicted,
            ss.first_betslip_stake as first_sports_bet_amount,
            ss.last_betslip_stake as last_sports_bet_amount,
            ss.totalaveragecombi as average_combination_length,
            ngr.ggr,
            ngr.ngr,
            msm.cross_product_historical_value_score_current,
            msm.cross_product_historical_value_segment_current,
            msm.sports_historical_value_score_current,
            msm.sports_historical_value_segment_current,
            msm.casino_historical_value_score_current,
            msm.casino_historical_value_segment_current,
            ngr.first_sports_bet_date,
            ngr.last_sports_bet_date,
            msm.lifetime_handle,
            msm.daily_active_handle,
            msm.lifetime_bets,
            cs.avg_session_min,
            cs.avg_session_sec,
            cs.total_sessions,
            date(dc.registration_date) as registration_date,
            dc.ftd_date,
            cast(null as string) as welcome_offer,
            cast(null as string) as welcome_offer_short_name,
            fcc.manually_classified,
            dc.is_test_player AS test_account_flag,
            dc.sb_ngr_percentile_last12months,
            dc.created,
            dc.profile_month_of_birth,
            dc.age_group,
            dc.profile_country_uid,
            dc.profile_country_name,
            dc.profile_gender,
            dc.brand_alias,
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
            COALESCE(ms.active_mission_count,0) as active_mission_count,
            COALESCE(ms.completed_mission_count,0) as completed_mission_count
        from `leo-prod-sports-bi.production_sports_customer_private.dim_customer_unified` as dc
        left join
            `leo-prod-sports-bi.production_sports_customer_private.fact_customer_classification` as fcc
            on fcc.customer_id = dc.customer_id
        left join `leo-prod-sports-bi.production_sports_customer_private.ref_customer_classification` as rcc
            on rcc.classification_id = fcc.classification_id
        left join `leo-prod-sports-bi.production_sports_bi_mart_private.dim_customer_sportsbook_statistics` as ss
            on dc.customer_uid = ss.customer_uid
        left join theoretical_margin as tm 
            on tm.customer_uid = dc.customer_uid
        left join ngr 
            on ngr.customer_id = dc.customer_id
        left join `leo-prod-sports-bi.production_sports_bi_mart_public.marketing_segmentation_metrics` as msm
            on dc.customer_uid = msm.customer_uid
        left join customer_session as cs 
            on dc.customer_uid = cs.customer_uid
        left join missions as ms 
            on dc.customer_uid = ms.customer_uid


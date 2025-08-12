-- Config
DECLARE tol FLOAT64 DEFAULT 1;

CREATE TEMP FUNCTION num_diff(a FLOAT64, b FLOAT64, tol FLOAT64)
RETURNS BOOL
AS ( (a IS NULL) <> (b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND ABS(a - b) > tol) );

-- 1) Build the diff flags and persist as a temp table
CREATE TEMP TABLE diffs AS
WITH
-- batches_load
kc AS (
  SELECT *
  FROM `leo-goldrush-data-sandbox.temp_bi.kpi_customer_bathces_load_initial`
  -- `leo-goldrush-data-sandbox._5677d8e4ef53e303a8067326b12b9e8fff30d22c.anon8abd4850_721e_4845_a443_0e0e1fc72652`
),
-- combined kpi_customer
old_kc AS (
  SELECT *
  FROM `leo-goldrush-data-sandbox.temp_bi.kpi_customer_combined`
  -- `leo-goldrush-data-sandbox._5677d8e4ef53e303a8067326b12b9e8fff30d22c.anon74627b9f_c77c_45ca_b7bc_ebcd408376ad`
)
SELECT
  COALESCE(kc.customer_uid, old_kc.customer_uid) AS customer_uid,

  -- Text/Date/Bool (null-safe via STRING)
  CAST(kc.customer_id AS STRING)                          IS DISTINCT FROM CAST(old_kc.customer_id AS STRING)                          AS diff_customer_id,
  CAST(kc.customer_uid AS STRING)                         IS DISTINCT FROM CAST(old_kc.customer_uid AS STRING)                        AS diff_customer_uid,
  CAST(kc.tiger_registration_date AS STRING)              IS DISTINCT FROM CAST(old_kc.tiger_registration_date AS STRING)                   AS diff_registration_date,
  CAST(kc.predicted_margin AS STRING)                     IS DISTINCT FROM CAST(old_kc.predicted_margin AS STRING)                    AS diff_predicted_margin,
  CAST(kc.ftd_date AS STRING)                             IS DISTINCT FROM CAST(old_kc.ftd_date AS STRING)                            AS diff_ftd_date,
  CAST(kc.first_betslip_date AS STRING)                   IS DISTINCT FROM CAST(old_kc.first_sports_bet_date AS STRING)               AS diff_first_betslip_date,
  CAST(kc.last_betslip_date AS STRING)                    IS DISTINCT FROM CAST(old_kc.last_sports_bet_date AS STRING)                AS diff_last_betslip_date,
  -- CAST(kc.acquisition_channel AS STRING)                  IS DISTINCT FROM CAST(old_kc.acquisition_channel AS STRING)                 AS diff_acquisition_channel,
  CAST(kc.classification_name_current AS STRING)          IS DISTINCT FROM CAST(old_kc.classification_name_current AS STRING)         AS diff_classification_name_current,
  CAST(kc.cross_product_historical_value_score_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.cross_product_historical_value_score_current AS STRING)                                              AS diff_xprod_value_score_curr,
  CAST(kc.cross_product_historical_value_segment_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.cross_product_historical_value_segment_current AS STRING)                                            AS diff_xprod_value_segment_curr,
  CAST(kc.sports_historical_value_score_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.sports_historical_value_score_current AS STRING)                                                     AS diff_sports_value_score_curr,
  CAST(kc.sports_historical_value_segment_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.sports_historical_value_segment_current AS STRING)                                                   AS diff_sports_value_segment_curr,
  CAST(kc.casino_historical_value_score_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.casino_historical_value_score_current AS STRING)                                                     AS diff_casino_value_score_curr,
  CAST(kc.casino_historical_value_segment_current AS STRING)
    IS DISTINCT FROM CAST(old_kc.casino_historical_value_segment_current AS STRING)                                                   AS diff_casino_value_segment_curr,
  CAST(kc.manually_classified AS STRING)                  IS DISTINCT FROM CAST(old_kc.manually_classified AS STRING)                 AS diff_manually_classified,
  CAST(kc.lifetime_bets AS STRING)                        IS DISTINCT FROM CAST(old_kc.lifetime_bets AS STRING)                       AS diff_lifetime_bets,
  CAST(kc.test_account_flag AS STRING)                    IS DISTINCT FROM CAST(old_kc.test_account_flag AS STRING)                   AS diff_test_account_flag,
  CAST(kc.age_group AS STRING)                            IS DISTINCT FROM CAST(old_kc.age_group AS STRING)                           AS diff_age_group,
  CAST(kc.profile_country_name AS STRING)                 IS DISTINCT FROM CAST(old_kc.profile_country_name AS STRING)                AS diff_profile_country_name,
  CAST(kc.brand_alias AS STRING)                          IS DISTINCT FROM CAST(old_kc.brand_alias AS STRING)                         AS diff_brand_alias,
  CAST(kc.is_locked AS STRING)                            IS DISTINCT FROM CAST(old_kc.is_locked AS STRING)                           AS diff_is_locked,
  CAST(kc.flag_rg AS STRING)                              IS DISTINCT FROM CAST(old_kc.flag_rg AS STRING)                             AS diff_flag_rg,
  CAST(kc.locked_until AS STRING)                         IS DISTINCT FROM CAST(old_kc.locked_until AS STRING)                        AS diff_locked_until,
  CAST(kc.predicted_risk_category_latest AS STRING)       IS DISTINCT FROM CAST(old_kc.predicted_risk_category_latest AS STRING)      AS diff_predicted_risk_category_latest,
  CAST(kc.profile_welcome_offer AS STRING)                IS DISTINCT FROM CAST(old_kc.profile_welcome_offer AS STRING)               AS diff_profile_welcome_offer,
  CAST(kc.self_exclusion_flag AS STRING)                  IS DISTINCT FROM CAST(old_kc.self_exclusion_flag AS STRING)                 AS diff_self_exclusion_flag,
  CAST(kc.is_bonus_abuser AS STRING)                      IS DISTINCT FROM CAST(old_kc.is_bonus_abuser AS STRING)                     AS diff_is_bonus_abuser,
  CAST(kc.is_suspended AS STRING)                         IS DISTINCT FROM CAST(old_kc.is_suspended AS STRING)                        AS diff_is_suspended,
  CAST(kc.person_id AS STRING)                            IS DISTINCT FROM CAST(old_kc.person_id AS STRING)                           AS diff_person_id,
  CAST(kc.license_uid AS STRING)                          IS DISTINCT FROM CAST(old_kc.license_uid AS STRING)                         AS diff_license_uid,
  CAST(kc.legal_entity AS STRING)                         IS DISTINCT FROM CAST(old_kc.legal_entity AS STRING)                        AS diff_legal_entity,
  CAST(kc.deposit_disabled AS STRING)                     IS DISTINCT FROM CAST(old_kc.deposit_disabled AS STRING)                    AS diff_deposit_disabled,
  CAST(kc.login_disabled AS STRING)                       IS DISTINCT FROM CAST(old_kc.login_disabled AS STRING)                      AS diff_login_disabled,
  CAST(kc.play_sports_disabled AS STRING)                 IS DISTINCT FROM CAST(old_kc.play_sports_disabled AS STRING)                AS diff_play_sports_disabled,
  CAST(kc.receive_sports_promotions_disabled AS STRING)   IS DISTINCT FROM CAST(old_kc.receive_sports_promotions_disabled AS STRING)  AS diff_receive_sports_promotions_disabled,
  CAST(kc.withdraw_disabled AS STRING)                    IS DISTINCT FROM CAST(old_kc.withdraw_disabled AS STRING)                   AS diff_withdraw_disabled,
  CAST(kc.longest_restriction AS STRING)                  IS DISTINCT FROM CAST(old_kc.longest_restriction AS STRING)                 AS diff_longest_restriction,
  CAST(kc.longest_restriction_valid_to AS STRING)         IS DISTINCT FROM CAST(old_kc.longest_restriction_valid_to AS STRING)        AS diff_longest_restriction_valid_to,
  CAST(kc.longest_restriction_type AS STRING)             IS DISTINCT FROM CAST(old_kc.longest_restriction_type AS STRING)            AS diff_longest_restriction_type,
  CAST(kc.player_sb_ngr_last12months AS STRING)           IS DISTINCT FROM CAST(old_kc.player_sb_ngr_last12months AS STRING)          AS diff_player_sb_ngr_last12months,

  CAST(kc.enrolled_bonus_campaigns AS STRING)             IS DISTINCT FROM CAST(old_kc.enrolled_bonus_campaigns AS STRING)            AS diff_enrolled_bonus_campaigns,
  CAST(kc.is_bonus_campaign_enrolled AS STRING)           IS DISTINCT FROM CAST(old_kc.is_bonus_campaign_enrolled AS STRING)          AS diff_is_bonus_campaign_enrolled,
  CAST(kc.most_recent_active_campign_id AS STRING)        IS DISTINCT FROM CAST(old_kc.most_recent_active_campaign_id AS STRING)      AS diff_most_recent_active_campaign_id,
  CAST(kc.most_recent_campaign_name AS STRING)            IS DISTINCT FROM CAST(old_kc.most_recent_campaign_name AS STRING)           AS diff_most_recent_campaign_name,
  CAST(kc.most_common_bonus_type AS STRING)               IS DISTINCT FROM CAST(old_kc.most_common_bonus_type AS STRING)              AS diff_most_common_bonus_type,
  CAST(kc.most_recent_bonus_type AS STRING)               IS DISTINCT FROM CAST(old_kc.most_recent_bonus_type AS STRING)              AS diff_most_recent_bonus_type,

  CAST(kc.sportsbook_sources AS STRING)                   IS DISTINCT FROM CAST(old_kc.sportsbook_sources AS STRING)                  AS diff_sportsbook_sources,
  CAST(kc.bet_sports AS STRING)                           IS DISTINCT FROM CAST(old_kc.bet_sports AS STRING)                          AS diff_bet_sports,
  CAST(kc.league_list AS STRING)                          IS DISTINCT FROM CAST(old_kc.league_list AS STRING)                         AS diff_league_list,
  CAST(kc.etl_created AS STRING)                          IS DISTINCT FROM CAST(old_kc.etl_created AS STRING)                         AS diff_etl_created,
  CAST(kc.etl_modified AS STRING)                         IS DISTINCT FROM CAST(old_kc.etl_modified AS STRING)                        AS diff_etl_modified,
  CAST(kc.data_update_time AS STRING)                     IS DISTINCT FROM CAST(old_kc.data_update_time AS STRING)                    AS diff_data_update_time,
  CAST(kc.predicted_revenue_segment AS STRING)            IS DISTINCT FROM CAST(old_kc.predicted_revenue_segment AS STRING)           AS diff_predicted_revenue_segment,
  CAST(kc.placed_a_parlay_bet AS STRING)                  IS DISTINCT FROM CAST(old_kc.placed_a_parlay_bet AS STRING)                 AS diff_placed_a_parlay_bet,

  -- Numeric with tolerance
  num_diff(SAFE_CAST(kc.active_daily_handle_lt AS FLOAT64), SAFE_CAST(old_kc.active_daily_handle_lt AS FLOAT64), tol) AS diff_active_daily_handle_lt,
  num_diff(SAFE_CAST(kc.active_days_lt AS FLOAT64),        SAFE_CAST(old_kc.active_days_lt AS FLOAT64),        tol) AS diff_active_days_lt,
  num_diff(SAFE_CAST(kc.active_days_l30 AS FLOAT64),       SAFE_CAST(old_kc.active_days_l30 AS FLOAT64),       tol) AS diff_active_days_l30,
  num_diff(SAFE_CAST(kc.bonus_cost_l30 AS FLOAT64),        SAFE_CAST(old_kc.bonus_cost_l30 AS FLOAT64),        tol) AS diff_bonus_cost_l30,
  num_diff(SAFE_CAST(kc.bonus_cost_lt AS FLOAT64),         SAFE_CAST(old_kc.bonus_cost_lt AS FLOAT64),         tol) AS diff_bonus_cost_lt,
  num_diff(SAFE_CAST(kc.bonus_to_ggr_l30 AS FLOAT64),      SAFE_CAST(old_kc.bonus_to_ggr_l30 AS FLOAT64),      tol) AS diff_bonus_to_ggr_l30,
  num_diff(SAFE_CAST(kc.bonus_to_ggr_lt AS FLOAT64),       SAFE_CAST(old_kc.bonus_to_ggr_lt AS FLOAT64),       tol) AS diff_bonus_to_ggr_lt,
  num_diff(SAFE_CAST(kc.handle_lt AS FLOAT64),             SAFE_CAST(old_kc.handle_lt AS FLOAT64),             tol) AS diff_handle_lt,
  num_diff(SAFE_CAST(kc.handle_l30 AS FLOAT64),            SAFE_CAST(old_kc.handle_l30 AS FLOAT64),            tol) AS diff_handle_l30,
  num_diff(SAFE_CAST(kc.ggr_l30 AS FLOAT64),               SAFE_CAST(old_kc.ggr_l30 AS FLOAT64),               tol) AS diff_ggr_l30,
  num_diff(SAFE_CAST(kc.ggr_lt AS FLOAT64),                SAFE_CAST(old_kc.ggr AS FLOAT64),                tol) AS diff_ggr_lt,
  num_diff(SAFE_CAST(kc.ggr_to_handle_l30 AS FLOAT64),     SAFE_CAST(old_kc.ggr_to_handle_l30 AS FLOAT64),     tol) AS diff_ggr_to_handle_l30,
  num_diff(SAFE_CAST(kc.ggr_to_handle_lt AS FLOAT64),      SAFE_CAST(old_kc.ggr_to_handle_lt AS FLOAT64),      tol) AS diff_ggr_to_handle_lt,
  num_diff(SAFE_CAST(kc.ngr_l30 AS FLOAT64),               SAFE_CAST(old_kc.ngr_l30 AS FLOAT64),               tol) AS diff_ngr_l30,
  num_diff(SAFE_CAST(kc.ngr_lt AS FLOAT64),                SAFE_CAST(old_kc.ngr AS FLOAT64),                tol) AS diff_ngr_lt,
  num_diff(SAFE_CAST(kc.bonus_wallet_balance AS FLOAT64),  SAFE_CAST(old_kc.bonus_wallet_balance AS FLOAT64),  tol) AS diff_bonus_wallet_balance,
  num_diff(SAFE_CAST(kc.real_money_pct_lifetime AS FLOAT64),SAFE_CAST(old_kc.real_money_pct_lifetime AS FLOAT64),tol) AS diff_real_money_pct_lifetime,
  num_diff(SAFE_CAST(kc.real_money_pct_7 AS FLOAT64),      SAFE_CAST(old_kc.real_money_pct_7 AS FLOAT64),      tol) AS diff_real_money_pct_7,
  num_diff(SAFE_CAST(kc.real_money_pct_30 AS FLOAT64),     SAFE_CAST(old_kc.real_money_pct_30 AS FLOAT64),     tol) AS diff_real_money_pct_30,
  num_diff(SAFE_CAST(kc.cnt_real_lifetime AS FLOAT64),     SAFE_CAST(old_kc.real_money_bet_cnt_lifetime AS FLOAT64),     tol) AS diff_cnt_real_lifetime,
  num_diff(SAFE_CAST(kc.cnt_real_7 AS FLOAT64),            SAFE_CAST(old_kc.real_money_bet_cnt_lifetime AS FLOAT64),            tol) AS diff_cnt_real_7,
  num_diff(SAFE_CAST(kc.cnt_real_30 AS FLOAT64),           SAFE_CAST(old_kc.real_money_bet_cnt_last30 AS FLOAT64),           tol) AS diff_cnt_real_30,
  num_diff(SAFE_CAST(kc.available_bet_and_gets AS FLOAT64),SAFE_CAST(old_kc.available_bet_and_gets AS FLOAT64),tol) AS diff_available_bet_and_gets,
  num_diff(SAFE_CAST(kc.available_protected_bets AS FLOAT64),SAFE_CAST(old_kc.available_protected_bets AS FLOAT64),tol) AS diff_available_protected_bets,
  num_diff(SAFE_CAST(kc.available_bet_credits AS FLOAT64), SAFE_CAST(old_kc.available_bet_credits AS FLOAT64), tol) AS diff_available_bet_credits,
  num_diff(SAFE_CAST(kc.available_deposit_matches AS FLOAT64),SAFE_CAST(old_kc.available_deposit_matches AS FLOAT64),tol) AS diff_available_deposit_matches,
  num_diff(SAFE_CAST(kc.average_bet_amount AS FLOAT64),    SAFE_CAST(old_kc.average_bet_amount AS FLOAT64),    tol) AS diff_average_bet_amount,
  num_diff(SAFE_CAST(kc.settled_bet_count AS FLOAT64),     SAFE_CAST(old_kc.settled_bet_count AS FLOAT64),     tol) AS diff_settled_bet_count_num,
  num_diff(SAFE_CAST(kc.predicted_revenue_next_30 AS FLOAT64), SAFE_CAST(old_kc.predicted_revenue_next_30 AS FLOAT64), tol) AS diff_predicted_revenue_next_30_num,
  num_diff(SAFE_CAST(kc.predicted_revenue_next_active_day AS FLOAT64), SAFE_CAST(old_kc.predicted_revenue_next_active_day AS FLOAT64), tol) AS diff_predicted_revenue_next_active_day_num,
  num_diff(SAFE_CAST(kc.pred_real_money_active_days_next30_bonus_0 AS FLOAT64), SAFE_CAST(old_kc.pred_real_money_active_days_next30_bonus_0 AS FLOAT64), tol) AS diff_pred_rm_days_next30_bonus_0,
  num_diff(SAFE_CAST(kc.pred_real_money_active_days_next30_bonus_1 AS FLOAT64), SAFE_CAST(old_kc.pred_real_money_active_days_next30_bonus_1 AS FLOAT64), tol) AS diff_pred_rm_days_next30_bonus_1,
  num_diff(SAFE_CAST(kc.pred_active_days_next30_bonus_lift AS FLOAT64), SAFE_CAST(old_kc.pred_active_days_next30_bonus_lift AS FLOAT64), tol) AS diff_pred_active_days_next30_bonus_lift,
  num_diff(SAFE_CAST(kc.tiger_lifetime_cashback_lc AS FLOAT64), SAFE_CAST(old_kc.tiger_lifetime_cashback_lc AS FLOAT64), tol) AS diff_tiger_lifetime_cashback_lc,
  num_diff(SAFE_CAST(kc.tiger_lifetime_missions_completed AS FLOAT64), SAFE_CAST(old_kc.tiger_lifetime_missions_completed AS FLOAT64), tol) AS diff_tiger_lifetime_missions_completed,
  num_diff(SAFE_CAST(kc.tiger_l30_cashback_lc AS FLOAT64), SAFE_CAST(old_kc.tiger_l30_cashback_lc AS FLOAT64), tol) AS diff_tiger_l30_cashback_lc,
  num_diff(SAFE_CAST(kc.tiger_l30_missions_completed AS FLOAT64), SAFE_CAST(old_kc.tiger_l30_missions_completed AS FLOAT64), tol) AS diff_tiger_l30_missions_completed,

  -- Booleans as text diffs
  CAST(kc.do_not_bonus AS STRING) IS DISTINCT FROM CAST(old_kc.do_not_bonus AS STRING) AS diff_do_not_bonus,
  CAST(kc.deposited   AS STRING) IS DISTINCT FROM CAST(old_kc.deposited   AS STRING) AS diff_deposited
FROM old_kc
FULL OUTER JOIN kc
  ON kc.customer_uid = old_kc.customer_uid
;

-- 2) Per-customer mismatches
CREATE TEMP TABLE per_customer AS
SELECT
  customer_uid,
  (
    SELECT ARRAY_AGG(col)
    FROM UNNEST([
      IF(diff_customer_id,'customer_id',NULL),
      IF(diff_customer_uid,'customer_uid',NULL),
      IF(diff_registration_date,'registration_date',NULL),
      IF(diff_predicted_margin,'predicted_margin',NULL),
      IF(diff_ftd_date,'ftd_date',NULL),
      IF(diff_first_betslip_date,'first_betslip_date',NULL),
      IF(diff_last_betslip_date,'last_sports_bet_date',NULL),
      -- IF(diff_acquisition_channel,'acquisition_channel',NULL),
      IF(diff_classification_name_current,'classification_name_current',NULL),
      IF(diff_xprod_value_score_curr,'cross_product_historical_value_score_current',NULL),
      IF(diff_xprod_value_segment_curr,'cross_product_historical_value_segment_current',NULL),
      IF(diff_sports_value_score_curr,'sports_historical_value_score_current',NULL),
      IF(diff_sports_value_segment_curr,'sports_historical_value_segment_current',NULL),
      IF(diff_casino_value_score_curr,'casino_historical_value_score_current',NULL),
      IF(diff_casino_value_segment_curr,'casino_historical_value_segment_current',NULL),
      IF(diff_manually_classified,'manually_classified',NULL),
      IF(diff_lifetime_bets,'lifetime_bets',NULL),
      IF(diff_test_account_flag,'test_account_flag',NULL),
      IF(diff_age_group,'age_group',NULL),
      IF(diff_profile_country_name,'profile_country_name',NULL),
      IF(diff_brand_alias,'brand_alias',NULL),
      IF(diff_is_locked,'is_locked',NULL),
      IF(diff_flag_rg,'flag_rg',NULL),
      IF(diff_locked_until,'locked_until',NULL),
      IF(diff_predicted_risk_category_latest,'predicted_risk_category_latest',NULL),
      IF(diff_profile_welcome_offer,'profile_welcome_offer',NULL),
      IF(diff_self_exclusion_flag,'self_exclusion_flag',NULL),
      IF(diff_is_bonus_abuser,'is_bonus_abuser',NULL),
      IF(diff_is_suspended,'is_suspended',NULL),
      IF(diff_person_id,'person_id',NULL),
      IF(diff_license_uid,'license_uid',NULL),
      IF(diff_legal_entity,'legal_entity',NULL),
      IF(diff_deposit_disabled,'deposit_disabled',NULL),
      IF(diff_login_disabled,'login_disabled',NULL),
      IF(diff_play_sports_disabled,'play_sports_disabled',NULL),
      IF(diff_receive_sports_promotions_disabled,'receive_sports_promotions_disabled',NULL),
      IF(diff_withdraw_disabled,'withdraw_disabled',NULL),
      IF(diff_longest_restriction,'longest_restriction',NULL),
      IF(diff_longest_restriction_valid_to,'longest_restriction_valid_to',NULL),
      IF(diff_longest_restriction_type,'longest_restriction_type',NULL),
      IF(diff_player_sb_ngr_last12months,'player_sb_ngr_last12months',NULL),

      IF(diff_enrolled_bonus_campaigns,'enrolled_bonus_campaigns',NULL),
      IF(diff_is_bonus_campaign_enrolled,'is_bonus_campaign_enrolled',NULL),
      IF(diff_most_recent_active_campaign_id,'most_recent_active_campaign_id',NULL),
      IF(diff_most_recent_campaign_name,'most_recent_campaign_name',NULL),
      IF(diff_most_common_bonus_type,'most_common_bonus_type',NULL),
      IF(diff_most_recent_bonus_type,'most_recent_bonus_type',NULL),

      IF(diff_sportsbook_sources,'sportsbook_sources',NULL),
      IF(diff_bet_sports,'bet_sports',NULL),
      IF(diff_league_list,'league_list',NULL),
      IF(diff_etl_created,'etl_created',NULL),
      IF(diff_etl_modified,'etl_modified',NULL),
      IF(diff_data_update_time,'data_update_time',NULL),
      IF(diff_predicted_revenue_segment,'predicted_revenue_segment',NULL),
      IF(diff_placed_a_parlay_bet,'placed_a_parlay_bet',NULL),

      IF(diff_active_daily_handle_lt,'active_daily_handle_lt',NULL),
      IF(diff_active_days_lt,'active_days_lt',NULL),
      IF(diff_active_days_l30,'active_days_l30',NULL),
      IF(diff_bonus_cost_l30,'bonus_cost_l30',NULL),
      IF(diff_bonus_cost_lt,'bonus_cost_lt',NULL),
      IF(diff_bonus_to_ggr_l30,'bonus_to_ggr_l30',NULL),
      IF(diff_bonus_to_ggr_lt,'bonus_to_ggr_lt',NULL),
      IF(diff_handle_lt,'handle_lt',NULL),
      IF(diff_handle_l30,'handle_l30',NULL),
      IF(diff_ggr_l30,'ggr_l30',NULL),
      IF(diff_ggr_lt,'ggr_lt',NULL),
      IF(diff_ggr_to_handle_l30,'ggr_to_handle_l30',NULL),
      IF(diff_ggr_to_handle_lt,'ggr_to_handle_lt',NULL),
      IF(diff_ngr_l30,'ngr_l30',NULL),
      IF(diff_ngr_lt,'ngr_lt',NULL),
      IF(diff_bonus_wallet_balance,'bonus_wallet_balance',NULL),
      IF(diff_real_money_pct_lifetime,'real_money_pct_lifetime',NULL),
      IF(diff_real_money_pct_7,'real_money_pct_7',NULL),
      IF(diff_real_money_pct_30,'real_money_pct_30',NULL),
      IF(diff_cnt_real_lifetime,'cnt_real_lifetime',NULL),
      IF(diff_cnt_real_7,'cnt_real_7',NULL),
      IF(diff_cnt_real_30,'cnt_real_30',NULL),
      IF(diff_available_bet_and_gets,'available_bet_and_gets',NULL),
      IF(diff_available_protected_bets,'available_protected_bets',NULL),
      IF(diff_available_bet_credits,'available_bet_credits',NULL),
      IF(diff_available_deposit_matches,'available_deposit_matches',NULL),
      IF(diff_average_bet_amount,'average_bet_amount',NULL),
      IF(diff_settled_bet_count_num,'settled_bet_count',NULL),
      IF(diff_predicted_revenue_next_30_num,'predicted_revenue_next_30',NULL),
      IF(diff_predicted_revenue_next_active_day_num,'predicted_revenue_next_active_day',NULL),
      IF(diff_pred_rm_days_next30_bonus_0,'pred_real_money_active_days_next30_bonus_0',NULL),
      IF(diff_pred_rm_days_next30_bonus_1,'pred_real_money_active_days_next30_bonus_1',NULL),
      IF(diff_pred_active_days_next30_bonus_lift,'pred_active_days_next30_bonus_lift',NULL),
      IF(diff_tiger_lifetime_cashback_lc,'tiger_lifetime_cashback_lc',NULL),
      IF(diff_tiger_lifetime_missions_completed,'tiger_lifetime_missions_completed',NULL),
      IF(diff_tiger_l30_cashback_lc,'tiger_l30_cashback_lc',NULL),
      IF(diff_tiger_l30_missions_completed,'tiger_l30_missions_completed',NULL),
      IF(diff_do_not_bonus,'do_not_bonus',NULL),
      IF(diff_deposited,'deposited',NULL)
    ]) col
    WHERE col IS NOT NULL
  ) AS mismatched_cols_array
FROM diffs
;

-- RESULT SET #1: per-customer mismatches
SELECT
  customer_uid,
  ARRAY_LENGTH(mismatched_cols_array) AS mismatch_count,
  ARRAY_TO_STRING(mismatched_cols_array, ',') AS mismatched_fields
FROM per_customer
WHERE ARRAY_LENGTH(mismatched_cols_array) > 0
ORDER BY mismatch_count DESC, customer_uid;

-- RESULT SET #2: per-column counts (how many customers mismatched each col)
SELECT
  col_name,
  COUNTIF(flag) AS mismatched_customer_count
FROM diffs,
UNNEST([
  STRUCT('customer_id' AS col_name, diff_customer_id AS flag),
  STRUCT('customer_uid', diff_customer_uid),
  STRUCT('registration_date', diff_registration_date),
  STRUCT('predicted_margin', diff_predicted_margin),
  STRUCT('ftd_date', diff_ftd_date),
  STRUCT('first_betslip_date', diff_first_betslip_date),
  STRUCT('last_sports_bet_date', diff_last_betslip_date),
  -- STRUCT('acquisition_channel', diff_acquisition_channel),
  STRUCT('classification_name_current', diff_classification_name_current),
  STRUCT('cross_product_historical_value_score_current', diff_xprod_value_score_curr),
  STRUCT('cross_product_historical_value_segment_current', diff_xprod_value_segment_curr),
  STRUCT('sports_historical_value_score_current', diff_sports_value_score_curr),
  STRUCT('sports_historical_value_segment_current', diff_sports_value_segment_curr),
  STRUCT('casino_historical_value_score_current', diff_casino_value_score_curr),
  STRUCT('casino_historical_value_segment_current', diff_casino_value_segment_curr),
  STRUCT('manually_classified', diff_manually_classified),
  STRUCT('lifetime_bets', diff_lifetime_bets),
  STRUCT('test_account_flag', diff_test_account_flag),
  STRUCT('age_group', diff_age_group),
  STRUCT('profile_country_name', diff_profile_country_name),
  STRUCT('brand_alias', diff_brand_alias),
  STRUCT('is_locked', diff_is_locked),
  STRUCT('flag_rg', diff_flag_rg),
  STRUCT('locked_until', diff_locked_until),
  STRUCT('predicted_risk_category_latest', diff_predicted_risk_category_latest),
  STRUCT('profile_welcome_offer', diff_profile_welcome_offer),
  STRUCT('self_exclusion_flag', diff_self_exclusion_flag),
  STRUCT('is_bonus_abuser', diff_is_bonus_abuser),
  STRUCT('is_suspended', diff_is_suspended),
  STRUCT('person_id', diff_person_id),
  STRUCT('license_uid', diff_license_uid),
  STRUCT('legal_entity', diff_legal_entity),
  STRUCT('deposit_disabled', diff_deposit_disabled),
  STRUCT('login_disabled', diff_login_disabled),
  STRUCT('play_sports_disabled', diff_play_sports_disabled),
  STRUCT('receive_sports_promotions_disabled', diff_receive_sports_promotions_disabled),
  STRUCT('withdraw_disabled', diff_withdraw_disabled),
  STRUCT('longest_restriction', diff_longest_restriction),
  STRUCT('longest_restriction_valid_to', diff_longest_restriction_valid_to),
  STRUCT('longest_restriction_type', diff_longest_restriction_type),
  STRUCT('player_sb_ngr_last12months', diff_player_sb_ngr_last12months),

  STRUCT('enrolled_bonus_campaigns', diff_enrolled_bonus_campaigns),
  STRUCT('is_bonus_campaign_enrolled', diff_is_bonus_campaign_enrolled),
  STRUCT('most_recent_active_campaign_id', diff_most_recent_active_campaign_id),
  STRUCT('most_recent_campaign_name', diff_most_recent_campaign_name),
  STRUCT('most_common_bonus_type', diff_most_common_bonus_type),
  STRUCT('most_recent_bonus_type', diff_most_recent_bonus_type),

  STRUCT('sportsbook_sources', diff_sportsbook_sources),
  STRUCT('bet_sports', diff_bet_sports),
  STRUCT('league_list', diff_league_list),
  STRUCT('etl_created', diff_etl_created),
  STRUCT('etl_modified', diff_etl_modified),
  STRUCT('data_update_time', diff_data_update_time),
  STRUCT('predicted_revenue_segment', diff_predicted_revenue_segment),
  STRUCT('placed_a_parlay_bet', diff_placed_a_parlay_bet),

  STRUCT('active_daily_handle_lt', diff_active_daily_handle_lt),
  STRUCT('active_days_lt', diff_active_days_lt),
  STRUCT('active_days_l30', diff_active_days_l30),
  STRUCT('bonus_cost_l30', diff_bonus_cost_l30),
  STRUCT('bonus_cost_lt', diff_bonus_cost_lt),
  STRUCT('bonus_to_ggr_l30', diff_bonus_to_ggr_l30),
  STRUCT('bonus_to_ggr_lt', diff_bonus_to_ggr_lt),
  STRUCT('handle_lt', diff_handle_lt),
  STRUCT('handle_l30', diff_handle_l30),
  STRUCT('ggr_l30', diff_ggr_l30),
  STRUCT('ggr_lt', diff_ggr_lt),
  STRUCT('ggr_to_handle_l30', diff_ggr_to_handle_l30),
  STRUCT('ggr_to_handle_lt', diff_ggr_to_handle_lt),
  STRUCT('ngr_l30', diff_ngr_l30),
  STRUCT('ngr_lt', diff_ngr_lt),
  STRUCT('bonus_wallet_balance', diff_bonus_wallet_balance),
  STRUCT('real_money_pct_lifetime', diff_real_money_pct_lifetime),
  STRUCT('real_money_pct_7', diff_real_money_pct_7),
  STRUCT('real_money_pct_30', diff_real_money_pct_30),
  STRUCT('cnt_real_lifetime', diff_cnt_real_lifetime),
  STRUCT('cnt_real_7', diff_cnt_real_7),
  STRUCT('cnt_real_30', diff_cnt_real_30),
  STRUCT('available_bet_and_gets', diff_available_bet_and_gets),
  STRUCT('available_protected_bets', diff_available_protected_bets),
  STRUCT('available_bet_credits', diff_available_bet_credits),
  STRUCT('available_deposit_matches', diff_available_deposit_matches),
  STRUCT('average_bet_amount', diff_average_bet_amount),
  STRUCT('settled_bet_count', diff_settled_bet_count_num),
  STRUCT('predicted_revenue_next_30', diff_predicted_revenue_next_30_num),
  STRUCT('predicted_revenue_next_active_day', diff_predicted_revenue_next_active_day_num),
  STRUCT('pred_real_money_active_days_next30_bonus_0', diff_pred_rm_days_next30_bonus_0),
  STRUCT('pred_real_money_active_days_next30_bonus_1', diff_pred_rm_days_next30_bonus_1),
  STRUCT('pred_active_days_next30_bonus_lift', diff_pred_active_days_next30_bonus_lift),
  STRUCT('tiger_lifetime_cashback_lc', diff_tiger_lifetime_cashback_lc),
  STRUCT('tiger_lifetime_missions_completed', diff_tiger_lifetime_missions_completed),
  STRUCT('tiger_l30_cashback_lc', diff_tiger_l30_cashback_lc),
  STRUCT('tiger_l30_missions_completed', diff_tiger_l30_missions_completed),
  STRUCT('do_not_bonus', diff_do_not_bonus),
  STRUCT('deposited', diff_deposited)
]) AS diff_map
GROUP BY col_name
HAVING mismatched_customer_count > 0
ORDER BY mismatched_customer_count DESC, col_name;

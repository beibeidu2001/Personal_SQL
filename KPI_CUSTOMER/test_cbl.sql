-- Config
DECLARE tol FLOAT64 DEFAULT 0.01;
CREATE TEMP FUNCTION num_diff(a FLOAT64, b FLOAT64, tol FLOAT64) RETURNS BOOL AS (
(a IS NULL) <> (b IS NULL) OR (a IS NOT NULL AND b IS NOT NULL AND ABS(a - b) > tol)
);
-- 1) Build the diff flags and persist as a temp table
CREATE TEMP TABLE diffs AS
WITH
-- kpi_customer
kc AS (
SELECT * FROM `leo-goldrush-data-sandbox.temp_bi.kpi_customer_initial`
-- `leo-goldrush-data-sandbox._5677d8e4ef53e303a8067326b12b9e8fff30d22c.anon9c41f5c2a375e37e30f27927d99c14c862c92c37cad923532944565d7cec6172`
),
-- kpi_customer_combined
old_kc AS (
SELECT * FROM `leo-goldrush-data-sandbox.temp_bi.kpi_customer_combined`
-- leo-goldrush-data-sandbox._5677d8e4ef53e303a8067326b12b9e8fff30d22c.anon74627b9f_c77c_45ca_b7bc_ebcd408376ad
)
SELECT
COALESCE(kc.customer_uid, old_kc.customer_uid) AS customer_uid,
-- Text/Date/Bool (null-safe via STRING)
CAST(kc.customer_id AS STRING) IS DISTINCT FROM CAST(old_kc.customer_id AS STRING) AS diff_customer_id,
CAST(kc.customer_uid AS STRING) IS DISTINCT FROM CAST(old_kc.customer_uid AS STRING) AS diff_customer_uid,
CAST(kc.registration_date AS STRING) IS DISTINCT FROM CAST(old_kc.tiger_registration_date AS STRING) AS diff_registration_date,
CAST(kc.predicted_margin AS STRING) IS DISTINCT FROM CAST(old_kc.predicted_margin AS STRING) AS diff_predicted_margin,
CAST(kc.ftd_date AS STRING) IS DISTINCT FROM CAST(old_kc.ftd_date AS STRING) AS diff_ftd_date,
CAST(kc.first_sports_bet_date AS STRING) IS DISTINCT FROM CAST(old_kc.first_sports_bet_date AS STRING) AS diff_first_sports_bet_date,
CAST(kc.last_sports_bet_date AS STRING) IS DISTINCT FROM CAST(old_kc.last_sports_bet_date AS STRING) AS diff_last_sports_bet_date,
CAST(kc.classification_name_current AS STRING) IS DISTINCT FROM CAST(old_kc.classification_name_current AS STRING) AS diff_classification_name_current,
CAST(kc.cross_product_historical_value_score_current AS STRING) IS DISTINCT FROM CAST(old_kc.cross_product_historical_value_score_current AS STRING) AS diff_cross_product_historical_value_score_current,
CAST(kc.cross_product_historical_value_segment_current AS STRING) IS DISTINCT FROM CAST(old_kc.cross_product_historical_value_segment_current AS STRING) AS diff_cross_product_historical_value_segment_current,
CAST(kc.sports_historical_value_score_current AS STRING) IS DISTINCT FROM CAST(old_kc.sports_historical_value_score_current AS STRING) AS diff_sports_historical_value_score_current,
CAST(kc.sports_historical_value_segment_current AS STRING) IS DISTINCT FROM CAST(old_kc.sports_historical_value_segment_current AS STRING) AS diff_sports_historical_value_segment_current,
CAST(kc.casino_historical_value_score_current AS STRING) IS DISTINCT FROM CAST(old_kc.casino_historical_value_score_current AS STRING) AS diff_casino_historical_value_score_current,
CAST(kc.casino_historical_value_segment_current AS STRING) IS DISTINCT FROM CAST(old_kc.casino_historical_value_segment_current AS STRING) AS diff_casino_historical_value_segment_current,
CAST(kc.manually_classified AS STRING) IS DISTINCT FROM CAST(old_kc.manually_classified AS STRING) AS diff_manually_classified,
CAST(kc.lifetime_bets AS STRING) IS DISTINCT FROM CAST(old_kc.lifetime_bets AS STRING) AS diff_lifetime_bets,
CAST(kc.test_account_flag AS STRING) IS DISTINCT FROM CAST(old_kc.test_account_flag AS STRING) AS diff_test_account_flag,
CAST(kc.age_group AS STRING) IS DISTINCT FROM CAST(old_kc.age_group AS STRING) AS diff_age_group,
CAST(kc.profile_country_name AS STRING) IS DISTINCT FROM CAST(old_kc.profile_country_name AS STRING) AS diff_profile_country_name,
CAST(kc.brand_alias AS STRING) IS DISTINCT FROM CAST(old_kc.brand_alias AS STRING) AS diff_brand_alias,
CAST(kc.is_locked AS STRING) IS DISTINCT FROM CAST(old_kc.is_locked AS STRING) AS diff_is_locked,
CAST(kc.flag_rg AS STRING) IS DISTINCT FROM CAST(old_kc.flag_rg AS STRING) AS diff_flag_rg,
CAST(kc.locked_until AS STRING) IS DISTINCT FROM CAST(old_kc.locked_until AS STRING) AS diff_locked_until,
CAST(kc.predicted_risk_category_latest AS STRING) IS DISTINCT FROM CAST(old_kc.predicted_risk_category_latest AS STRING) AS diff_predicted_risk_category_latest,
CAST(kc.profile_welcome_offer AS STRING) IS DISTINCT FROM CAST(old_kc.profile_welcome_offer AS STRING) AS diff_profile_welcome_offer,
CAST(kc.self_exclusion_flag AS STRING) IS DISTINCT FROM CAST(old_kc.self_exclusion_flag AS STRING) AS diff_self_exclusion_flag,
CAST(kc.is_bonus_abuser AS STRING) IS DISTINCT FROM CAST(old_kc.is_bonus_abuser AS STRING) AS diff_is_bonus_abuser,
CAST(kc.is_suspended AS STRING) IS DISTINCT FROM CAST(old_kc.is_suspended AS STRING) AS diff_is_suspended,
CAST(kc.person_id AS STRING) IS DISTINCT FROM CAST(old_kc.person_id AS STRING) AS diff_person_id,
CAST(kc.license_uid AS STRING) IS DISTINCT FROM CAST(old_kc.license_uid AS STRING) AS diff_license_uid,
CAST(kc.legal_entity AS STRING) IS DISTINCT FROM CAST(old_kc.legal_entity AS STRING) AS diff_legal_entity,
CAST(kc.deposit_disabled AS STRING) IS DISTINCT FROM CAST(old_kc.deposit_disabled AS STRING) AS diff_deposit_disabled,
CAST(kc.login_disabled AS STRING) IS DISTINCT FROM CAST(old_kc.login_disabled AS STRING) AS diff_login_disabled,
CAST(kc.play_sports_disabled AS STRING) IS DISTINCT FROM CAST(old_kc.play_sports_disabled AS STRING) AS diff_play_sports_disabled,
CAST(kc.receive_sports_promotions_disabled AS STRING) IS DISTINCT FROM CAST(old_kc.receive_sports_promotions_disabled AS STRING) AS diff_receive_sports_promotions_disabled,
CAST(kc.withdraw_disabled AS STRING) IS DISTINCT FROM CAST(old_kc.withdraw_disabled AS STRING) AS diff_withdraw_disabled,
CAST(kc.longest_restriction AS STRING) IS DISTINCT FROM CAST(old_kc.longest_restriction AS STRING) AS diff_longest_restriction,
CAST(kc.longest_restriction_valid_to AS STRING) IS DISTINCT FROM CAST(old_kc.longest_restriction_valid_to AS STRING) AS diff_longest_restriction_valid_to,
CAST(kc.longest_restriction_type AS STRING) IS DISTINCT FROM CAST(old_kc.longest_restriction_type AS STRING) AS diff_longest_restriction_type,
CAST(kc.player_sb_ngr_last12months AS STRING) IS DISTINCT FROM CAST(old_kc.player_sb_ngr_last12months AS STRING) AS diff_player_sb_ngr_last12months
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
IF(diff_customer_id, 'customer_id', NULL),
IF(diff_customer_uid, 'customer_uid', NULL),
IF(diff_registration_date, 'registration_date', NULL),
IF(diff_predicted_margin, 'predicted_margin', NULL),
IF(diff_ftd_date, 'ftd_date', NULL),
IF(diff_first_sports_bet_date, 'first_sports_bet_date', NULL),
IF(diff_last_sports_bet_date, 'last_sports_bet_date', NULL),
IF(diff_classification_name_current, 'classification_name_current', NULL),
IF(diff_cross_product_historical_value_score_current, 'cross_product_historical_value_score_current', NULL),
IF(diff_cross_product_historical_value_segment_current, 'cross_product_historical_value_segment_current', NULL),
IF(diff_sports_historical_value_score_current, 'sports_historical_value_score_current', NULL),
IF(diff_sports_historical_value_segment_current, 'sports_historical_value_segment_current', NULL),
IF(diff_casino_historical_value_score_current, 'casino_historical_value_score_current', NULL),
IF(diff_casino_historical_value_segment_current, 'casino_historical_value_segment_current', NULL),
IF(diff_manually_classified, 'manually_classified', NULL),
IF(diff_lifetime_bets, 'lifetime_bets', NULL),
IF(diff_test_account_flag, 'test_account_flag', NULL),
IF(diff_age_group, 'age_group', NULL),
IF(diff_profile_country_name, 'profile_country_name', NULL),
IF(diff_brand_alias, 'brand_alias', NULL),
IF(diff_is_locked, 'is_locked', NULL),
IF(diff_flag_rg, 'flag_rg', NULL),
IF(diff_locked_until, 'locked_until', NULL),
IF(diff_predicted_risk_category_latest, 'predicted_risk_category_latest', NULL),
IF(diff_profile_welcome_offer, 'profile_welcome_offer', NULL),
IF(diff_self_exclusion_flag, 'self_exclusion_flag', NULL),
IF(diff_is_bonus_abuser, 'is_bonus_abuser', NULL),
IF(diff_is_suspended, 'is_suspended', NULL),
IF(diff_person_id, 'person_id', NULL),
IF(diff_license_uid, 'license_uid', NULL),
IF(diff_legal_entity, 'legal_entity', NULL),
IF(diff_deposit_disabled, 'deposit_disabled', NULL),
IF(diff_login_disabled, 'login_disabled', NULL),
IF(diff_play_sports_disabled, 'play_sports_disabled', NULL),
IF(diff_receive_sports_promotions_disabled, 'receive_sports_promotions_disabled', NULL),
IF(diff_withdraw_disabled, 'withdraw_disabled', NULL),
IF(diff_longest_restriction, 'longest_restriction', NULL),
IF(diff_longest_restriction_valid_to, 'longest_restriction_valid_to', NULL),
IF(diff_longest_restriction_type, 'longest_restriction_type', NULL),
IF(diff_player_sb_ngr_last12months, 'player_sb_ngr_last12months', NULL)
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
STRUCT('first_sports_bet_date', diff_first_sports_bet_date),
STRUCT('last_sports_bet_date', diff_last_sports_bet_date),
STRUCT('classification_name_current', diff_classification_name_current),
STRUCT('cross_product_historical_value_score_current', diff_cross_product_historical_value_score_current),
STRUCT('cross_product_historical_value_segment_current', diff_cross_product_historical_value_segment_current),
STRUCT('sports_historical_value_score_current', diff_sports_historical_value_score_current),
STRUCT('sports_historical_value_segment_current', diff_sports_historical_value_segment_current),
STRUCT('casino_historical_value_score_current', diff_casino_historical_value_score_current),
STRUCT('casino_historical_value_segment_current', diff_casino_historical_value_segment_current),
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
STRUCT('player_sb_ngr_last12months', diff_player_sb_ngr_last12months)
]) AS diff_map
GROUP BY col_name
HAVING mismatched_customer_count > 0
ORDER BY mismatched_customer_count DESC, col_name;

select count(distinct c.customer_uid) --,c.manually_classified as combined, o.manually_classified as old
from `leo-goldrush-data-sandbox.temp_bi.kpi_customer_combined` c
right join `leo-goldrush-data-sandbox.temp_bi.kpi_customer_bathces_load_initial` o
-- `leo-goldrush-data-sandbox.temp_bi.kpi_customer_bathces_load_initial` o
on o.customer_uid = c.customer_uid
where 
-- c.bet_sports <> o.bet_sports -- 3 makes sense; the three customers placed a their first sports betslip within the time I 
-- 1 = 1 -- 27854463
-- abs(c.ggr_to_handle_lt - o.ggr_to_handle_lt) > 0.1 -- 19601
-- c.most_common_bonus_type <> o.most_common_bonus_type -- 4711
-- c.most_recent_bonus_type <> o.most_recent_bonus_type -- 5438
-- c.most_recent_campaign_name <> o.most_recent_campaign_name -- 211
-- abs(c.bonus_to_ggr_l30 - o.bonus_to_ggr_l30) > 0.1 -- 1364
-- abs(c.ggr_to_handle_l30 - o.ggr_to_handle_l30) > 0.1 - 4771
-- abs(c.real_money_bet_cnt_last7 - o.cnt_real_7) > 0.1 -- 0
-- abs(c.bonus_to_ggr_lt - o.bonus_to_ggr_lt) > 0.1 -- returns 4000
-- abs(c.active_daily_handle_lt - o.active_daily_handle_lt) > 0.1 -- returns 7940
-- c.manually_classified <> o.manually_classified
limit 10

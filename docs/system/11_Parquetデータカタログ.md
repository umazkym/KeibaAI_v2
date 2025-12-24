# Parquet File Contents Report

Last Updated: 2025-12-20

> [!NOTE]
> 本カタログは `docs/system/03_データモデル.md` の実データ定義に基づいています。

================================================================================
File: keibaai/data/parsed/parquet/horses/horses.parquet
================================================================================

--- Columns ---
horse_id, horse_name, birth_date, trainer_name, trainer_id, owner_name, breeder_name, producing_area, birth_year

--- Record Count ---
~62,398 records

================================================================================
File: keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet
================================================================================

--- Columns ---
horse_id, ancestor_id, ancestor_name, generation

--- Record Count ---
~3,677,865 records

================================================================================
File: keibaai/data/parsed/parquet/races/races.parquet
================================================================================

--- Columns (61 total) ---
race_id, race_date, distance_m, track_surface, weather, track_condition, post_time, race_name, prize_1st, prize_2nd, prize_3rd, prize_4th, prize_5th, venue, day_of_meeting, round_of_year, race_class, age_restriction, finish_position, bracket_number, horse_number, horse_id, horse_name, sex_age, sex, age, basis_weight, jockey_id, jockey_name, finish_time_str, finish_time_seconds, margin_str, margin_seconds, passing_order_1, passing_order_2, passing_order_3, passing_order_4, last_3f_time, time_except_last3f, win_odds, popularity, horse_weight, horse_weight_change, trainer_name, pace_index, last3f_rank, position_change_1_2, position_change_2_3, position_change_3_4, final_corner_to_finish, horse_weight_deviation, popularity_finish_diff, trainer_id, owner_name, prize_money, win_probability, relative_odds, distance_category, is_straight_course, course_direction, is_outer_course

--- Record Count ---
~277,826 records

================================================================================
File: keibaai/data/parsed/parquet/shutuba/shutuba.parquet
================================================================================

--- Columns ---
race_id, scratched, bracket_number, horse_number, horse_name, horse_id, sex_age, sex, age, basis_weight, jockey_name, jockey_id, trainer_name, trainer_id, horse_weight, horse_weight_change, race_date, morning_odds, morning_popularity, owner_name, prize_total, career_stats, career_starts, career_wins, career_places, last_5_finishes

================================================================================
File: keibaai/data/parsed/parquet/returns/returns.parquet
================================================================================

--- Columns ---
race_id, horse_number, payout, popularity, bet_type, bracket_1, bracket_2, horse_1, horse_2, horse_3

--- bet_type values ---
tansho, fukusho, wakuren, umaren, wide, umatan, sanrenpuku, sanrentan

================================================================================
File: keibaai/data/parsed/parquet/race_details/race_details.parquet
================================================================================

--- Columns ---
race_id, lap_times, lap_times_str, pace_str, first_half, second_half, corner_1_raw, corner_2_raw, corner_3_raw, corner_4_raw

================================================================================
File: keibaai/data/parsed/parquet/corners/corner_positions.parquet
================================================================================

--- Columns ---
race_id, corner, horse_number, position, gap_from_leader


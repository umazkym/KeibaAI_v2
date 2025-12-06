# Parquet File Contents Report

Last Updated: 2025-12-06

================================================================================
File: keibaai/data/parsed/parquet/horses/horses.parquet
================================================================================

--- Columns ---
horse_id, horse_name, birth_date, trainer_name, trainer_id, owner_name, breeder_name, producing_area

--- Sample Data (5 rows) ---
     horse_id horse_name  birth_date trainer_name trainer_id    owner_name breeder_name producing_area
0  2002100816  ディープインパクト  2002-03-25         池江泰郎      00110  金子真人ホールディングス     ノーザンファーム            早来町
1  2009100502  メイショウオトコギ  2009-05-16         飯田祐史      01139          松本好雄         太陽牧場            浦河町
2  2009102606   サンマルデューク  2009-04-03         和田勇介      01165           相馬勇         田端牧場            日高町


================================================================================
File: keibaai/data/parsed/parquet/pedigrees/pedigrees.parquet
================================================================================

--- Columns ---
horse_id, ancestor_id, ancestor_name, generation

--- Sample Data (5 rows) ---
     horse_id ancestor_id   ancestor_name  generation
0  2002100816  000a00033a       サンデーサイレンス           1
1  2002100816  000a0012bf            Halo           2
2  2002100816  000a000f2b  Hail to Reason           3


================================================================================
File: keibaai/data/parsed/parquet/races/races.parquet
================================================================================

--- Columns (58 total) ---
race_id, race_date, distance_m, track_surface, weather, track_condition, post_time, race_name, prize_1st, prize_2nd, prize_3rd, prize_4th, prize_5th, venue, day_of_meeting, round_of_year, race_class, age_restriction, finish_position, bracket_number, horse_number, horse_id, horse_name, sex_age, sex, age, basis_weight, jockey_id, jockey_name, finish_time_str, finish_time_seconds, margin_str, margin_seconds, passing_order_1, passing_order_2, passing_order_3, passing_order_4, last_3f_time, time_except_last3f, win_odds, popularity, horse_weight, horse_weight_change, trainer_name, pace_index, last3f_rank, position_change_1_2, position_change_2_3, position_change_3_4, final_corner_to_finish, horse_weight_deviation, popularity_finish_diff, win_probability, relative_odds, distance_category, trainer_id, owner_name, prize_money

--- Record Count ---
277,826 records


================================================================================
File: keibaai/data/parsed/parquet/shutuba/shutuba.parquet
================================================================================

--- Columns ---
race_id, scratched, bracket_number, horse_number, horse_name, horse_id, sex_age, sex, age, basis_weight, jockey_name, jockey_id, trainer_name, trainer_id, horse_weight, horse_weight_change, race_date, morning_odds, morning_popularity, owner_name, prize_total, career_stats, career_starts, career_wins, career_places, last_5_finishes


================================================================================
File: keibaai/data/parsed/parquet/returns/returns.parquet (NEW)
================================================================================

--- Columns ---
race_id, horse_number, payout, popularity, bet_type, bracket_1, bracket_2, horse_1, horse_2, horse_3

--- bet_type values ---
tansho (単勝), fukusho (複勝), wakuren (枠連), umaren (馬連), wide (ワイド), umatan (馬単), sanrenpuku (三連複), sanrentan (三連単)

--- Sample Data ---
        race_id  horse_number  payout  popularity bet_type  bracket_1  bracket_2  horse_1  horse_2  horse_3
0  202001010101           6.0    1600           3   tansho        NaN        NaN      NaN      NaN      NaN
1  202001010102           4.0     270           1   tansho        NaN        NaN      NaN      NaN      NaN
2  202001010101           NaN    1410           4   umaren        NaN        NaN      2.0      6.0      NaN

--- Record Count ---
240,333 records (20,157 races)


================================================================================
File: keibaai/data/parsed/parquet/race_details/race_details.parquet (NEW)
================================================================================

--- Columns ---
race_id, lap_times, lap_times_str, pace_str, first_half, second_half, corner_1_raw, corner_2_raw, corner_3_raw, corner_4_raw

--- Column Descriptions ---
- lap_times: ラップタイムリスト (list[float])
- first_half, second_half: 前半・後半3F (秒)
- corner_X_raw: Xコーナー通過順位（生テキスト）

--- Sample Data ---
        race_id  first_half  second_half     corner_1_raw        corner_4_raw
0  202001010101        36.1         35.9       1,3(2,5)6=4       1,3(6,2)4=5
1  202001010102        29.9         39.0  4,8,14,5(7,6,11)...  4,14-3,5(8,10,13)...

--- Record Count ---
20,157 records (1 per race)


================================================================================
File: keibaai/data/parsed/parquet/corners/corner_positions.parquet (NEW)
================================================================================

--- Columns ---
race_id, corner, horse_number, position, gap_from_leader

--- Column Descriptions ---
- corner: コーナー番号 (1-4)
- position: 通過順位（同順位方式：並走馬は同じ順位）
- gap_from_leader: 先頭からの累積馬身差

--- Gap Calculation Rules ---
| 記号 | 加算馬身 | 意味 |
|------|----------|------|
| () 内 | 0 | 並走 |
| なし | +1.0 | デフォルト |
| , | +1.5 | 1-2馬身差 |
| - | +3.5 | 2-5馬身差 |
| = | +7.0 | 5馬身以上 |

--- Sample Data ---
        race_id  corner  horse_number  position  gap_from_leader
0  202001010101       1             1         1              0.0
1  202001010101       1             3         2              1.5
2  202001010101       1             2         3              2.5
3  202001010101       1             5         3              2.5  ← 並走（同順位）
4  202001010101       1             6         5              3.5
5  202001010101       1             4         6             10.5  ← =で+7.0

--- Record Count ---
811,415 records (20,157 races × ~10 horses × ~4 corners)

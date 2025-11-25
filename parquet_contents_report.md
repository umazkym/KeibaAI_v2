# Parquet File Contents Report

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
3  2009103405     スズカルパン  2009-03-16         西橋豊治      00434          永井宏明         岡野牧場          新ひだか町
4  2010100035  シャイニープリンス  2010-04-26         深山雅史      01174          小林昌志     コアレススタッド            平取町


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
3  2002100816  000a001042         Turn-to           4
4  2002100816  000a001181   Royal Charger           5


================================================================================
File: keibaai/data/parsed/parquet/races/races.parquet
================================================================================

--- Columns ---
race_id, race_date, distance_m, track_surface, weather, track_condition, post_time, race_name, prize_1st, prize_2nd
prize_3rd, prize_4th, prize_5th, venue, day_of_meeting, round_of_year, race_class, age_restriction, finish_position, bracket_number
horse_number, horse_id, horse_name, sex_age, sex, age, basis_weight, jockey_id, jockey_name, finish_time_str
finish_time_seconds, margin_str, margin_seconds, passing_order_1, passing_order_2, passing_order_3, passing_order_4, last_3f_time, time_except_last3f, win_odds
popularity, horse_weight, horse_weight_change, trainer_name, pace_index, last3f_rank, position_change_1_2, position_change_2_3, position_change_3_4, final_corner_to_finish
horse_weight_deviation, popularity_finish_diff, win_probability, relative_odds, distance_category, trainer_id, owner_name, prize_money

--- Sample Data (5 rows) ---
        race_id   race_date  distance_m track_surface weather track_condition post_time race_name  prize_1st  prize_2nd  prize_3rd  prize_4th  prize_5th venue  day_of_meeting  round_of_year race_class age_restriction  finish_position  bracket_number  horse_number    horse_id horse_name sex_age sex  age  basis_weight jockey_id jockey_name finish_time_str  finish_time_seconds margin_str  margin_seconds  passing_order_1  passing_order_2  passing_order_3  passing_order_4  last_3f_time  time_except_last3f  win_odds  popularity  horse_weight  horse_weight_change trainer_name  pace_index  last3f_rank  position_change_1_2  position_change_2_3  position_change_3_4  final_corner_to_finish  horse_weight_deviation  popularity_finish_diff  win_probability  relative_odds distance_category trainer_id owner_name  prize_money
0  202001010101  2020-07-25        1800             芝       曇               良     09:55     2歳未勝利       <NA>       <NA>       <NA>       <NA>       <NA>    札幌               1              1        未勝利              2歳                1               6             6  2018101626     ウインルーア      牝2   牝    2          54.0     01170        横山武史          1:49.7                109.7                        NaN                5                5                3                3          35.6                74.1      16.0           3           438                    4                 2.075630            1                  0.0                 -2.0                  0.0                    -2.0               45.351023                    -2.0         0.058824       0.559767              mile      01156       None          NaN
1  202001010101  2020-07-25        1800             芝       曇               良     09:55     2歳未勝利       <NA>       <NA>       <NA>       <NA>       <NA>    札幌               1              1        未勝利              2歳                2               2             2  2018105193     アークライト      牡2   牡    2          54.0     05339        ルメール          1:50.0                110.0      1.3/4            0.35                3                3                4                3          35.8                74.2       1.9           2           510                    0                 2.066852            2                  0.0                  1.0                 -1.0                    -1.0               66.271418                     0.0         0.344828       0.066472              mile      05778       None          NaN
2  202001010101  2020-07-25        1800             芝       曇               良     09:55     2歳未勝利       <NA>       <NA>       <NA>       <NA>       <NA>    札幌               1              1        未勝利              2歳                3               3             3  2018104800  ギャラントウォリア      牡2   牡    2          54.0     01032        池添謙一          1:50.1                110.1        1/2            0.10                2                2                2                2          36.2                73.9       1.8           1           482                   -6                 2.035813            3                  0.0                  0.0                  0.0                     1.0               58.135709                     2.0         0.357143       0.062974              mile      01082       None          NaN
3  202001010101  2020-07-25        1800             芝       曇               良     09:55     2歳未勝利       <NA>       <NA>       <NA>       <NA>       <NA>    札幌               1              1        未勝利              2歳                4               1             1  2018102410     ジュンブーケ      牝2   牝    2          52.0     01176        亀田温心          1:50.5                110.5      2.1/2            0.50                1                1                1                1          36.7                73.8      22.2           4           442                    0                 2.005435            5                  0.0                  0.0                  0.0                     3.0               46.513268                     0.0         0.043103       0.776676              mile      05771       None          NaN
4  202001010101  2020-07-25        1800             芝       曇               良     09:55     2歳未勝利       <NA>       <NA>       <NA>       <NA>       <NA>    札幌               1              1        未勝利              2歳                5               4             4  2018100828    キタノマンゲツ      牡2   牡    2          54.0     01116        藤岡康太          1:51.0                111.0          3            0.60                6                6                5                5          36.6                74.4      55.7           5           426                   -8                 2.027248            4                  0.0                 -1.0                  0.0                     0.0               41.864291                     0.0         0.017637       1.948688              mile      a0272       None          NaN


================================================================================
File: keibaai/data/parsed/parquet/shutuba/shutuba.parquet
================================================================================

--- Columns ---
race_id, scratched, bracket_number, horse_number, horse_name, horse_id, sex_age, sex, age, basis_weight
jockey_name, jockey_id, trainer_name, trainer_id, horse_weight, horse_weight_change, race_date, morning_odds, morning_popularity, owner_name
prize_total, career_stats, career_starts, career_wins, career_places, last_5_finishes

--- Sample Data (5 rows) ---
        race_id  scratched  bracket_number  horse_number horse_name    horse_id sex_age sex  age  basis_weight jockey_name jockey_id trainer_name trainer_id  horse_weight  horse_weight_change  race_date morning_odds morning_popularity owner_name prize_total career_stats career_starts career_wins career_places last_5_finishes
0  202001010101      False               1             1     ジュンブーケ  2018102410      牝2   牝    2          52.0         △亀田     01176            森      00427         442.0                  0.0 2020-07-25         None               None       None        None         None          None        None          None            None
1  202001010101      False               2             2     アークライト  2018105193      牡2   牡    2          54.0        ルメール     05339          藤沢和      00386         510.0                  0.0 2020-07-25         None               None       None        None         None          None        None          None            None
2  202001010101      False               3             3  ギャラントウォリア  2018104800      牡2   牡    2          54.0          池添     01032           平田      01082         482.0                 -6.0 2020-07-25         None               None       None        None         None          None        None          None            None
3  202001010101      False               4             4    キタノマンゲツ  2018100828      牡2   牡    2          54.0         藤岡康     01116           中尾      01069         426.0                 -8.0 2020-07-25         None               None       None        None         None          None        None          None            None
4  202001010101      False               5             5     アイフレンズ  2018106434      牝2   牝    2          54.0           黛     01109           中野      01010         426.0                 -2.0 2020-07-25         None               None       None        None         None          None        None          None            None



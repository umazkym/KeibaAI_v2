"""
修正したparserをテスト
"""
from keibaai.src.modules.parsers.results_parser import parse_results_html

# テストファイル
html_file = 'keibaai/data/raw/html/race/202001010101.bin'
race_id = '202001010101'

print(f"パース中: {html_file}")
df = parse_results_html(html_file, race_id)

if df.empty:
    print("ERROR: DataFrameが空です")
else:
    print(f"\n成功: {len(df)}行")
    print(f"列数: {len(df.columns)}")
    
    # 賞金データを確認
    if 'prize_money' in df.columns:
        prize_data = df[['finish_position', 'horse_name', 'prize_money']].head(10)
        print("\n賞金データ:")
        print(prize_data)
        
        # 統計
        non_null = df['prize_money'].notna().sum()
        non_zero = (df['prize_money'] > 0).sum()
        print(f"\n賞金統計:")
        print(f"  非null: {non_null}/{len(df)}")
        print(f"  非ゼロ: {non_zero}/{len(df)}")
        
        # 1-5位の賞金
        top5 = df[df['finish_position'] <= 5][['finish_position', 'horse_name', 'prize_money']]
        print(f"\n1-5位の賞金:")
        print(top5)
    else:
        print("\nERROR: prize_money列がありません")
    
    # 調教師・馬主も確認
    print(f"\n調教師データ:")
    print(f"  非null: {df['trainer_id'].notna().sum()}/{len(df)}")
    print(f"\n馬主データ:")
    print(f"  非null: {df['owner_name'].notna().sum()}/{len(df)}")

"""
races.parquetの2024年データを正しく統合し直す
"""
import pandas as pd
from pathlib import Path

print("=" * 60)
print("races.parquetの修正")
print("=" * 60)

# 既存のraces.parquetを読み込み
races_path = Path('keibaai/data/parsed/parquet/races/races.parquet')
backup_path = Path('keibaai/data/parsed/parquet/races/races_backup.parquet')

print("\n既存のraces.parquetを読み込み...")
races = pd.read_parquet(races_path)
print(f"総レコード数: {len(races):,}件")

# バックアップ
print(f"\nバックアップを作成: {backup_path}")
races.to_parquet(backup_path, index=False)

# 2024年と非2024年に分離
races_2024 = races[races['race_id'].str.startswith('2024')].copy()
races_other = races[~races['race_id'].str.startswith('2024')].copy()

print(f"\n2024年データ: {len(races_2024):,}件")
print(f"その他データ: {len(races_other):,}件")

# 2024年データの問題確認
prize_1st_nonzero = (races_2024['prize_1st'] > 0).sum() if 'prize_1st' in races_2024.columns else 0
print(f"2024年のprize_1st非ゼロ: {prize_1st_nonzero}件")

if prize_1st_nonzero == 0:
    print("\n✗ 2024年データにprize_1st~5thが欠損しています")
    print("  → 2024年のHTMLを再パースします")
    
    # reparse_2024.pyを再実行して、今度は既存データとマージしない
    import sys
    sys.path.append('keibaai')
    from src.modules.parsers import results_parser
    from tqdm import tqdm
    
    # 2024年のHTMLファイルを探す
    html_dir = Path('keibaai/data/raw/html/race')
    html_files_2024 = []
    
    for root, _, files in html_dir.walk():
        for file in files:
            if file.startswith('2024') and file.endswith(('.html', '.bin')):
                html_files_2024.append(root / file)
    
    print(f"\n2024年のHTMLファイル: {len(html_files_2024)}件")
    
    # パース
    all_results = []
    errors = 0
    
    for html_file in tqdm(html_files_2024, desc="再パース中"):
        try:
            race_id = html_file.stem
            df = results_parser.parse_results_html(str(html_file), race_id)
            if df is not None and not df.empty:
                all_results.append(df)
        except Exception as e:
            errors += 1
            if errors <= 3:
                print(f"\nエラー: {html_file.name} - {str(e)[:100]}")
    
    if all_results:
        races_2024_new = pd.concat(all_results, ignore_index=True)
        print(f"\n再パース完了: {len(races_2024_new):,}件")
        
        # prize_1st確認
        if 'prize_1st' in races_2024_new.columns:
            non_zero = (races_2024_new['prize_1st'] > 0).sum()
            print(f"prize_1st非ゼロ: {non_zero}件")
        
        if 'prize_money' in races_2024_new.columns:
            non_zero = (races_2024_new['prize_money'] > 0).sum()
            print(f"prize_money非ゼロ: {non_zero}件")
        
        # 統合
        print("\n既存データと統合...")
        final_df = pd.concat([races_other, races_2024_new], ignore_index=True)
        print(f"統合後: {len(final_df):,}件")
        
        # 保存
        print(f"\n保存中: {races_path}")
        final_df.to_parquet(races_path, index=False)
        print("✓ 完了")
        
        # 検証
        print("\n検証:")
        final_df_check = pd.read_parquet(races_path)
        races_2024_check = final_df_check[final_df_check['race_id'].str.startswith('2024')]
        if 'prize_1st' in races_2024_check.columns:
            non_zero = (races_2024_check['prize_1st'] > 0).sum()
            print(f"  2024年prize_1st非ゼロ: {non_zero}件")
        if 'prize_money' in races_2024_check.columns:
            non_zero = (races_2024_check['prize_money'] > 0).sum()
            print(f"  2024年prize_money非ゼロ: {non_zero}件")
    else:
        print("\nエラー: 再パースできませんでした")
else:
    print("\n✓ 2024年データは正常です")

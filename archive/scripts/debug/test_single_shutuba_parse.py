"""
1レース分の出馬表HTMLをパースしてmorning_odds抽出を確認
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent
sys.path.insert(0, str(project_root))

from keibaai.src.parsers.shutuba_parser import parse_shutuba_html

def main():
    print("=" * 80)
    print("🔧 単一出馬表HTMLのパーステスト")
    print("=" * 80)
    print()

    # HTMLディレクトリのパス（Windows環境に合わせて変更）
    html_dir = Path(r'keibaai\data\raw\html\shutuba')

    if not html_dir.exists():
        print(f"❌ エラー: {html_dir} が存在しません")
        print(f"   現在のディレクトリ: {Path.cwd()}")
        print()
        print("💡 ヒント: スクリプトをプロジェクトルートから実行してください")
        print("   例: python test_single_shutuba_parse.py")
        return

    # .binファイルを取得（最初の3件をテスト）
    bin_files = sorted(list(html_dir.glob('*.bin')))[:3]

    if not bin_files:
        print(f"❌ エラー: {html_dir} に.binファイルが見つかりません")
        return

    print(f"📁 テスト対象ディレクトリ: {html_dir}")
    print(f"📊 テストファイル数: {len(bin_files)}")
    print()

    success_count = 0
    total_odds_found = 0
    total_popularity_found = 0
    total_horses = 0

    for i, test_file in enumerate(bin_files, 1):
        print(f"--- テスト {i}/{len(bin_files)}: {test_file.name} ---")

        try:
            df = parse_shutuba_html(str(test_file))

            if df.empty:
                print("  ⚠️  パース結果が空です")
                continue

            success_count += 1
            total_horses += len(df)

            # morning_oddsの取得状況
            odds_found = df['morning_odds'].notna().sum()
            popularity_found = df['morning_popularity'].notna().sum()

            total_odds_found += odds_found
            total_popularity_found += popularity_found

            print(f"  ✅ パース成功: {len(df)}頭")
            print(f"     - morning_odds:        {odds_found}/{len(df)}頭 取得")
            print(f"     - morning_popularity:  {popularity_found}/{len(df)}頭 取得")

            # サンプル表示（最初のファイルのみ）
            if i == 1 and odds_found > 0:
                print()
                print("  🔍 サンプルデータ（先頭3頭）:")
                sample = df[['horse_number', 'horse_name', 'morning_odds', 'morning_popularity']].head(3)
                for _, row in sample.iterrows():
                    print(f"     {row['horse_number']:2}番 {row['horse_name']:10} "
                          f"オッズ:{row['morning_odds']:6} 人気:{row['morning_popularity']}")

            print()

        except Exception as e:
            print(f"  ❌ エラー: {e}")
            print()

    # 総合結果
    print("=" * 80)
    print("📊 テスト結果サマリー")
    print("=" * 80)
    print(f"成功: {success_count}/{len(bin_files)} ファイル")
    print(f"総馬数: {total_horses}頭")
    print()

    if total_horses > 0:
        odds_rate = (total_odds_found / total_horses) * 100
        popularity_rate = (total_popularity_found / total_horses) * 100

        print(f"morning_odds 取得率:       {total_odds_found}/{total_horses} ({odds_rate:.1f}%)")
        print(f"morning_popularity 取得率: {total_popularity_found}/{total_horses} ({popularity_rate:.1f}%)")
        print()

        # 判定
        if odds_rate > 80:
            print("✅ morning_odds抽出: 成功！")
        elif odds_rate > 50:
            print("⚠️  morning_odds抽出: 部分的に成功（改善の余地あり）")
        else:
            print("❌ morning_odds抽出: 失敗（パーサーの再確認が必要）")

        print()
        print("💡 次のステップ:")
        if odds_rate > 80:
            print("   → 全体再パース: python keibaai/src/run_parsing_pipeline_local.py")
        else:
            print("   → HTMLの構造を確認して、パーサーを再調整してください")

    print("=" * 80)

if __name__ == '__main__':
    main()

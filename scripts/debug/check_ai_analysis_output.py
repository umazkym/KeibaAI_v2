#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""AI分析シートの実データを詳細に確認"""

import pandas as pd

# 実際のExcelを読み込んで内容を確認
df = pd.read_excel('race_analysis_20251214_中山.xlsx', sheet_name='AI分析_01R', header=1)
print("=== 01R AI分析結果（改良版）===")
print(f"カラム: {df.columns.tolist()}")
print()

# 注目理由カラムを中心に確認
for i, row in df.iterrows():
    print(f"{row['馬番']:>2}. {row['馬名']}: 推奨={row['推奨']}")
    print(f"   注目理由: {row['注目理由']}")
    print(f"   総合スコア={row['総合スコア']}, 妙味指数={row['妙味指数']}, オッズ={row['現オッズ']}")
    print()

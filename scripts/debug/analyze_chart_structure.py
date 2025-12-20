#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Analysis_シートのチャート構造を詳細分析"""

from openpyxl import load_workbook

wb = load_workbook('race_analysis_20251214_中山.xlsx')
sheet = wb['Analysis_01']
print('=== Analysis_01 チャート詳細 ===')
for i, chart in enumerate(sheet._charts):
    print(f'\nChart {i+1}: {type(chart).__name__}')
    if chart.title:
        print(f'  Title: {chart.title.text if hasattr(chart.title, "text") else chart.title}')
    for j, series in enumerate(chart.series):
        print(f'  Series {j}: {series.title}')
        if hasattr(series, 'xVal') and series.xVal:
            print(f'    X-Ref: {series.xVal.numRef.f if series.xVal.numRef else "N/A"}')
        if hasattr(series, 'yVal') and series.yVal:
            print(f'    Y-Ref: {series.yVal.numRef.f if series.yVal.numRef else "N/A"}')

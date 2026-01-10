#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
インタラクティブ競馬分析アプリ（スマホ最適化版 v8.7: Fix Waku Colors）

- Improve:
    1. 【ソート】ボタンを全表示（スクロールなし、Wrap）。
    2. 【比較】対戦マトリクス（早見表）＋詳細アコーディオン。
    3. 【チャート】
        - ③ 末脚(縦) vs 位置取り(横) に変更。
        - ④ 上がり指数(縦) vs RPCI(横) を追加。
    4. 【ロジック】位置取り計算を「(平均通過順 - 1) / (頭数 - 1)」に変更。
    5. 【データ】比較詳細の「通過」が表示されないデータ不備を修正（1C~4Cから合成）。
    6. 【バグ修正】対戦履歴の馬番識別を「馬名」ベースに変更（過去の馬番混入防止）。
    7. 【バグ修正】枠番の色分けをデータ（出走枠番）準拠に変更（変則頭数等に対応）。
"""

import argparse
import os
from openpyxl import load_workbook
import json
import logging
import itertools
from collections import defaultdict
import re
import statistics

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

WAKU_COLORS = {
    1: '#F0F0F0', 2: '#222222', 3: '#EF5350',
    4: '#42A5F5', 5: '#FBC02D', 6: '#66BB6A',
    7: '#FFA726', 8: '#F48FB1',
}

WAKU_TEXT_COLORS = {
    1: '#333', 2: '#FFF', 3: '#FFF',
    4: '#FFF', 5: '#333', 6: '#FFF',
    7: '#FFF', 8: '#333',
}

DISPLAY_COLUMNS = [
    '枠', '番',
    '日付', '場所', 'ｺｰｽ', '距離', 'R', '馬場', '天気', '頭数',
    '枠番', '馬番', '馬名', '斤量', '騎手',
    'タイム', '着差', '人気', 'ｵｯｽﾞ',
    '上がり', 'ﾍﾟｰｽ1', 'ﾍﾟｰｽ2', '通過', '1角', '2角', '3角', '4角',
    '着順', '体重', '増減', 'レース名',
    '平t差', '基t差', 'T指数', 'L指数', '馬場差', 'RPCI'
]

COL_MAP = {
    '出走枠番': '枠', '出走馬番': '番',
    'タイム秒': 'タイム', '上り': '上がり',
    '1C': '1角', '2C': '2角', '3C': '3角', '4C': '4角',
    '馬体重': '体重', 'タイム指数': 'T指数', '上り指数': 'L指数'
}

def normalize_text(text):
    if not text: return ""
    t = str(text).strip()
    t = t.translate(str.maketrans('０１２３４５６７８９', '0123456789'))
    return t

def get_waku_fallback(umaban):
    """Fallback calculation if data missing"""
    try:
        u = int(umaban)
        if u <= 2: return 1
        elif u <= 4: return 2
        elif u <= 6: return 3
        elif u <= 8: return 4
        elif u <= 10: return 5
        elif u <= 12: return 6
        elif u <= 14: return 7
        return 8
    except:
        return 1

def read_sheet_data(sheet):
    if sheet.max_row < 2: return [], []
    headers = [cell.value for cell in sheet[1]]
    rows = []
    for row in sheet.iter_rows(min_row=2, values_only=True):
        row_data = {}
        has_data = False
        for i, val in enumerate(row):
            if i < len(headers) and headers[i]:
                key = str(headers[i])
                val_str = str(val) if val is not None else ''
                row_data[key] = val_str
                if val_str: has_data = True
        if has_data:
            rows.append(row_data)
    return headers, rows

def make_pass_string(row):
    pass_vals = []
    for k in ['1角', '2角', '3角', '4角']:
        val = row.get(k, '')
        if val and str(val).strip() != '':
            pass_vals.append(str(val))
    if not pass_vals:
        return row.get('通過', '-') 
    return '-'.join(pass_vals)

def calculate_dynamic_matchups(race_rows, name_map):
    """
    Raceデータから対戦履歴を動的生成
    name_map: { '馬名': current_umaban }
    """
    past_races = defaultdict(list)
    
    for row in race_rows:
        date = normalize_text(row.get('日付'))
        place = normalize_text(row.get('場所', ''))
        red = normalize_text(row.get('R', ''))
        racename = normalize_text(row.get('レース名', ''))
        
        if not date: continue
        
        place_clean = place.replace('競馬場', '').replace('中央', '').replace('地方', '')
        red_clean = red.replace('R', '')
        
        if red_clean:
            key = f"{date}_{place_clean}_{red_clean}"
        else:
            key = f"{date}_{racename}"
            
        past_races[key].append(row)
    
    matchups = defaultdict(list)
    matrix = defaultdict(dict)
    
    for key, participants in past_races.items():
        if len(participants) < 2: continue
        
        for p1, p2 in itertools.permutations(participants, 2):
            name1 = p1.get('馬名', '')
            name2 = p2.get('馬名', '')
            
            h1 = name_map.get(name1)
            h2 = name_map.get(name2)
            
            if not h1 or not h2: continue
            
            try:
                h1_num = int(h1)
                h2_num = int(h2)
                
                r1_str = p1.get('着順', '99')
                r2_str = p2.get('着順', '99')
                
                r1 = int(r1_str) if r1_str.isdigit() else 999
                r2 = int(r2_str) if r2_str.isdigit() else 999
                
                res_str = 'Draw'
                if r1 < r2: res_str = 'Win'
                elif r1 > r2: res_str = 'Lose'
                
                my_pass_str = make_pass_string(p1)
                op_pass_str = make_pass_string(p2)
                
                matchups[h1_num].append({
                    'opponent_num': h2_num,
                    'opponent_name': name2,
                    'date': p1.get('日付', ''),
                    'race_name': p1.get('レース名', ''),
                    'place': p1.get('場所', ''),
                    'course': p1.get('ｺｰｽ', ''),
                    'dist': p1.get('距離', ''),
                    'time': p1.get('タイム', ''), 
                    
                    'my_rank': p1.get('着順', '-'),
                    'my_idx': p1.get('T指数', '-'),
                    'my_pass': my_pass_str,
                    'my_l3f': p1.get('上がり', '-'),
                    'my_rpci': p1.get('RPCI', '-'),
                    
                    'op_rank': p2.get('着順', '-'),
                    'op_time': p2.get('タイム', '-'),
                    'op_idx': p2.get('T指数', '-'),
                    'op_pass': op_pass_str,
                    'op_l3f': p2.get('上がり', '-'),
                    'op_rpci': p2.get('RPCI', '-'),
                    
                    'result': res_str
                })
                
                cur = matrix[h1_num].get(h2_num, {'w':0, 'l':0, 'd':0})
                if res_str == 'Win': cur['w'] += 1
                elif res_str == 'Lose': cur['l'] += 1
                else: cur['d'] += 1
                matrix[h1_num][h2_num] = cur
                
            except Exception as e:
                continue
                
    return matchups, matrix

def create_interactive_charts(source_file: str, output_file: str = None):
    if not os.path.exists(source_file):
        return None
    
    if not output_file:
        base, _ = os.path.splitext(source_file)
        output_file = f"{base}_interactive.html"
    
    wb = load_workbook(source_file, data_only=True)
    
    all_data = {
        'races': {}, 'matchups': {}, 'matrix': {}, 'chart_data': {}, 'race_info': {}
    }
    
    for sheet_name in wb.sheetnames:
        if sheet_name.startswith('Race_'):
            race_num = sheet_name.replace('Race_', '')
            headers, rows = read_sheet_data(wb[sheet_name])
            
            processed_rows = []
            horse_chart_info = {} 

            name_map = {}
            waku_map = {}
            
            # Pass 1: Build Maps
            for row in rows:
                name = row.get('馬名', '')
                umaban = row.get('出走馬番', '')
                waku = row.get('出走枠番', '') or row.get('枠', '')
                
                if name and umaban and str(umaban).isdigit():
                    u_int = int(umaban)
                    name_map[name] = u_int
                    if waku and str(waku).isdigit():
                        waku_map[u_int] = int(waku)

            # Pass 2: Process Rows
            for row in rows:
                new_row = {}
                for k, v in row.items():
                    nk = COL_MAP.get(k, k)
                    new_row[nk] = v
                
                if '通過' not in new_row or not new_row['通過']:
                    new_row['通過'] = make_pass_string(new_row)

                processed_rows.append(new_row)
                
                try:
                    name = row.get('馬名', '')
                    umaban = name_map.get(name, 0)
                    
                    if umaban > 0:
                        if umaban not in horse_chart_info:
                            real_waku = waku_map.get(umaban, get_waku_fallback(umaban))
                            horse_chart_info[umaban] = {
                                'umaban': umaban, 'waku': real_waku, 'name': name, 'records': []
                            }
                        try:
                            def safe_float(r, k_list, d=0.0):
                                for k in k_list:
                                    if k in r and r[k]:
                                        try: return float(r[k])
                                        except: pass
                                return d
                            
                            time_idx = safe_float(row, ['タイム指数', 'T指数'])
                            l3f_idx = safe_float(row, ['上り指数', 'L指数'])
                            heads = safe_float(row, ['頭数'], 1)
                            c1 = safe_float(row, ['1C', '1角'])
                            c2 = safe_float(row, ['2C', '2角'])
                            c3 = safe_float(row, ['3C', '3角'])
                            c4 = safe_float(row, ['4C', '4角'])
                            
                            valid_corners = [c for c in [c1, c2, c3, c4] if c > 0]
                            if valid_corners:
                                avg_rank = statistics.mean(valid_corners)
                            else: avg_rank = heads / 2 
                            
                            if heads > 1: pos_score = (avg_rank - 1) / (heads - 1)
                            else: pos_score = 0.5
                            pos_score = max(0.0, min(1.0, pos_score))
                            pos_score = round(pos_score, 3)

                            rpci = safe_float(row, ['RPCI'], 50.0)
                            
                            if time_idx > 0:
                                horse_chart_info[umaban]['records'].append({
                                    'time_idx': round(time_idx, 1),
                                    'l3f_idx': round(l3f_idx, 1),
                                    'c1_ratio': pos_score, 
                                    'rpci': round(rpci, 1),
                                    'rank': row.get('着順', ''),
                                    'date': row.get('日付', ''),
                                    'course': row.get('ｺｰｽ', '')
                                })
                        except: pass
                except: pass

            all_data['races'][race_num] = {'rows': processed_rows}
            all_data['chart_data'][race_num] = {'horses': list(horse_chart_info.values())}
            
            if rows:
                first = processed_rows[0]
                all_data['race_info'][race_num] = {
                    'race_name': first.get('レース名', ''), 'date': first.get('日付', ''),
                    'place': first.get('場所', ''), 'course': first.get('ｺｰｽ', ''),
                    'dist': first.get('距離', ''), 'weather': first.get('天気', ''),
                    'cond': first.get('馬場', ''), 'heads': first.get('頭数', '')
                }
            
            m_data, matrix_data = calculate_dynamic_matchups(processed_rows, name_map)
            all_data['matchups'][race_num] = m_data
            all_data['matrix'][race_num] = matrix_data
            
    sorted_races = sorted(all_data['races'].keys(), key=lambda x: int(x) if x.isdigit() else 99)
    html = generate_html(source_file, sorted_races, all_data)
    
    with open(output_file, 'w', encoding='utf-8') as f:
        f.write(html)
    return output_file


def generate_html(source_file: str, races: list, all_data: dict) -> str:
    base_name = os.path.splitext(os.path.basename(source_file))[0]
    title = base_name.replace('race_analysis_', '')
    
    data_json = json.dumps(all_data, ensure_ascii=False)
    races_json = json.dumps(races, ensure_ascii=False)
    waku_color_json = json.dumps(WAKU_COLORS)
    waku_text_json = json.dumps(WAKU_TEXT_COLORS)
    cols_json = json.dumps(DISPLAY_COLUMNS, ensure_ascii=False)
    
    html = f'''<!DOCTYPE html>
<html lang="ja">
<head>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0, maximum-scale=1.0, user-scalable=no">
    <title>{title}</title>
    <script src="https://cdn.plot.ly/plotly-2.27.0.min.js"></script>
    <style>
        :root {{ 
            --primary: #4F46E5; --bg: #F3F4F6; --card: #FFFFFF; 
            --text: #111827; --text-light: #6B7280; --border: #E5E7EB;
            --safe-b: env(safe-area-inset-bottom);
        }}
        body {{ 
            margin: 0; padding: 0; font-family: sans-serif; 
            background: var(--bg); color: var(--text);
            padding-bottom: calc(70px + var(--safe-b));
            -webkit-tap-highlight-color: transparent;
        }}
        .flex {{ display: flex; }} .flex-c {{ display: flex; flex-direction: column; }}
        .flex-ac {{ display: flex; align-items: center; }} .flex-jc {{ display: flex; justify-content: center; }}
        .flex-sb {{ display: flex; justify-content: space-between; }}
        .gap-1 {{ gap: 4px; }} .gap-2 {{ gap: 8px; }}
        .font-bold {{ font-weight: 700; }}
        .text-xs {{ font-size: 10px; }} .text-sm {{ font-size: 12px; }}
        .card {{ background: var(--card); border-radius: 8px; padding: 10px; margin-bottom: 10px; box-shadow: 0 1px 2px rgba(0,0,0,0.05); }}
        .badge {{ background: #E5E7EB; padding: 2px 6px; border-radius: 4px; font-size: 10px; margin-right: 4px; }}
        
        .header {{ position: sticky; top: 0; background: var(--primary); color: white; z-index: 100; padding: 10px; }}
        .race-tabs {{ overflow-x: auto; white-space: nowrap; padding-bottom: 2px; }}
        .race-tab {{ display: inline-block; padding: 5px 12px; margin-right: 6px; background: rgba(255,255,255,0.2); border-radius: 12px; font-size: 12px; font-weight: bold; }}
        .race-tab.active {{ background: white; color: var(--primary); }}
        
        .main {{ padding: 10px; max-width: 600px; margin: 0 auto; }}

        .sort-area {{ display: flex; flex-wrap: wrap; gap: 6px; margin-bottom: 8px; }}
        .sort-chip {{ padding: 6px 10px; background: #fff; border: 1px solid var(--border); border-radius: 14px; font-size: 11px; cursor: pointer; }}
        .sort-chip.active {{ background: var(--primary); color: #fff; border-color: var(--primary); }}

        .grid-dense {{ display: grid; grid-template-columns: repeat(auto-fill, minmax(65px, 1fr)); gap: 4px; }}
        .gd-cell {{ background: #F9FAFB; padding: 4px; border-radius: 4px; border: 1px solid #F3F4F6; }}
        .gd-lbl {{ font-size: 8px; color: var(--text-light); }}
        .gd-val {{ font-size: 10px; font-weight: 600; white-space: nowrap; overflow: hidden; text-overflow: ellipsis; }}

        .matrix-box {{ overflow-x: auto; -webkit-overflow-scrolling: touch; margin-bottom: 12px; }}
        .matrix-table {{ border-collapse: collapse; font-size: 10px; width: 100%; min-width: 300px; }}
        .matrix-table th, .matrix-table td {{ border: 1px solid #eee; padding: 4px; text-align: center; }}
        .matrix-table th {{ background: #f3f4f6; position: sticky; left: 0; z-index: 10; font-weight: bold; }}
        .mx-win {{ background: #d1fae5; color: #065f46; }}
        .mx-lose {{ background: #fee2e2; color: #991b1b; }}
        .mx-draw {{ background: #f3f4f6; color: #9ca3af; }}
        
        .nav {{ position: fixed; bottom: 0; left: 0; width: 100%; background: #fff; border-top: 1px solid var(--border); display: flex; padding-bottom: var(--safe-b); z-index: 200; }}
        .nav-btn {{ flex: 1; padding: 8px; text-align: center; font-size: 10px; color: var(--text-light); }}
        .nav-btn.active {{ color: var(--primary); font-weight: bold; }}
        .nav-icon {{ font-size: 18px; display: block; margin-bottom: 2px; }}
        
        .h-num {{ width: 20px; height: 20px; border-radius: 50%; display: flex; align-items: center; justify-content: center; font-size: 12px; font-weight: bold; margin-right: 6px; }}
        
        .filter-sticky {{ position: sticky; top: 42px; background: var(--bg); z-index: 90; padding: 6px 10px; margin: 0 -10px; border-bottom: 1px solid var(--border); }}
        .filter-row {{ display: flex; flex-wrap: wrap; align-items: center; gap: 4px; margin-bottom: 4px; }}
        .filter-label {{ font-size: 9px; font-weight: bold; color: var(--text-light); margin-right: 4px; white-space: nowrap; }}
        .filter-chip {{ display: inline-flex; align-items: center; padding: 2px 6px; border: 1px solid #ccc; border-radius: 10px; font-size: 10px; cursor: pointer; background: #fff; }}
        .filter-chip .dot {{ width: 6px; height: 6px; border-radius: 50%; margin-right: 3px; }}
    </style>
</head>
<body>
<div id="app"></div>
<script>
    const DATA = {data_json};
    const RACES = {races_json};
    const WAKU = {waku_color_json};
    const TXT = {waku_text_json};
    const COLS = {cols_json};

    let state = {{
        race: RACES[0], tab: 'chart', expanded: {{}}, filters: {{}}, sortCol: null, sortAsc: false, wakuMap: {{}},
        filterDate: '', // 'YYYY/MM/DD'
        venueFilters: [], // excluded venues
        distFilters: [], // excluded distances
        courseFilters: [] // excluded courses (芝/ダート)
    }};

    function init() {{ 
        buildWakuMap();
        render(); 
    }}
    
    function setState(s) {{ 
        state = {{...state, ...s}};
        if(s.race) buildWakuMap(); 
        render(); 
    }}
    
    function buildWakuMap() {{
        const d = DATA.chart_data[state.race];
        const m = {{}};
        if(d && d.horses) {{ d.horses.forEach(h => m[h.umaban] = h.waku); }}
        state.wakuMap = m;
    }}
    
    function getWaku(u) {{
        return state.wakuMap[u] || 1;
    }}

    function render() {{
        const app = document.getElementById('app');
        app.innerHTML = `
            ${{renderHeader()}}
            <div class="main">
                ${{renderInfo()}}
                ${{renderMain()}}
            </div>
            ${{renderNav()}}
        `;
        if(state.tab === 'chart') setTimeout(drawCharts, 50);
    }}

    function renderHeader() {{
        return `<div class="header"><div class="race-tabs">${{RACES.map(r => 
            `<div class="race-tab ${{state.race==r?'active':''}}" onclick="setState({{race:'${{r}}'}})">${{r}}R</div>`
        ).join('')}}</div></div>`;
    }}

    function renderInfo() {{
        const i = DATA.race_info[state.race] || {{}};
        return `<div class="card"><div class="font-bold text-sm">${{state.race}}R ${{i.race_name||''}}</div>
        <div><span class="badge">${{i.course||''}}${{i.dist||''}}</span><span class="badge">${{i.cond||''}}</span><span class="badge">${{i.heads||''}}頭</span></div></div>`;
    }}

    function renderMain() {{
        if(state.tab === 'chart') return renderChart();
        if(state.tab === 'data') return renderData();
        if(state.tab === 'matchup') return renderMatchup();
    }}

    // --- CHART ---
    function renderChart() {{
        const d = DATA.chart_data[state.race] || {{horses:[]}};
        const blocked = state.filters[state.race] || [];
        const horseFilters = d.horses.map(h => 
            `<div onclick="togFilter(${{h.umaban}})" class="filter-chip" style="opacity:${{blocked.includes(h.umaban)?0.4:1}}">
                <span class="dot" style="background:${{WAKU[h.waku]}}"></span>${{h.umaban}}
             </div>`
        ).join('');

        // Collect unique venues and distances from data
        const allRecords = [];
        d.horses.forEach(h => h.records.forEach(r => {{
            allRecords.push({{...r, u:h.umaban, w:h.waku}});
        }}));
        
        const venues = [...new Set(DATA.races[state.race]?.rows.map(r => r['場所']).filter(v => v))].sort();
        const distances = [...new Set(DATA.races[state.race]?.rows.map(r => r['距離']).filter(v => v))].sort((a,b)=>parseInt(a)-parseInt(b));
        const courses = [...new Set(DATA.races[state.race]?.rows.map(r => r['ｺｰｽ']).filter(v => v))].sort();
        
        // Count occurrences for each venue/distance/course
        const rows = DATA.races[state.race]?.rows || [];
        const venueCounts = {{}};
        const distCounts = {{}};
        const courseCounts = {{}};
        rows.forEach(r => {{
            const v = r['場所'];
            const d = r['距離'];
            const c = r['ｺｰｽ'];
            if(v) venueCounts[v] = (venueCounts[v] || 0) + 1;
            if(d) distCounts[d] = (distCounts[d] || 0) + 1;
            if(c) courseCounts[c] = (courseCounts[c] || 0) + 1;
        }});
        
        // Course filter with visual indicator (colored dot)
        const courseFilters = courses.map(c => {{
            const isHidden = state.courseFilters.includes(c);
            const cnt = courseCounts[c] || 0;
            const dotColor = c === '芝' ? '#00C853' : (c === 'ダート' || c === 'ダ') ? '#FF6D00' : '#888';
            return `<div onclick="togCourse('${{c}}')" class="filter-chip" style="opacity:${{isHidden?0.4:1}}"><span class="dot" style="background:${{dotColor}}"></span>${{c}}<span style="font-size:8px;color:#888;margin-left:2px">${{cnt}}</span></div>`;
        }}).join('');
        
        const venueFilters = venues.map(v => {{
            const isHidden = state.venueFilters.includes(v);
            const cnt = venueCounts[v] || 0;
            return `<div onclick="togVenue('${{v}}')" class="filter-chip" style="opacity:${{isHidden?0.4:1}}">${{v}}<span style="font-size:8px;color:#888;margin-left:2px">${{cnt}}</span></div>`;
        }}).join('');
        
        const distFilters = distances.map(dist => {{
            const isHidden = state.distFilters.includes(dist);
            const cnt = distCounts[dist] || 0;
            return `<div onclick="togDist('${{dist}}')" class="filter-chip" style="opacity:${{isHidden?0.4:1}}">${{dist}}<span style="font-size:8px;color:#888;margin-left:2px">${{cnt}}</span></div>`;
        }}).join('');

        return `
            <div class="filter-sticky">
                <div class="filter-row">
                    <span class="filter-label">日付:</span>
                    <input type="text" value="${{state.filterDate}}" placeholder="YYYY/MM/DD以降" 
                           style="border:1px solid #ccc; padding:2px 4px; border-radius:4px; font-size:10px; width:90px"
                           onchange="setState({{filterDate: this.value}})">
                </div>
                <div class="filter-row">
                    <span class="filter-label">コース:</span>${{courseFilters}}
                </div>
                <div class="filter-row">
                    <span class="filter-label">場所:</span>${{venueFilters}}
                </div>
                <div class="filter-row">
                    <span class="filter-label">距離:</span>${{distFilters}}
                </div>
                <div class="filter-row">
                    <span class="filter-label">馬番:</span>${{horseFilters}}
                </div>
            </div>
            <div class="card">
                <div class="font-bold text-xs">① スピード(縦) vs スタミナ(横)</div>
                <div id="c1" style="height:260px;margin-bottom:10px"></div>
                
                <div class="font-bold text-xs">② タイム(縦) vs 位置取り(横)</div>
                <div id="c2" style="height:260px;margin-bottom:10px"></div>
                
                <div class="font-bold text-xs">③ 末脚(縦) vs 位置取り(横)</div>
                <div id="c3" style="height:260px;margin-bottom:10px"></div>

                <div class="font-bold text-xs">④ 上がり指数(縦) vs RPCI(横)</div>
                <div id="c4" style="height:260px"></div>
            </div>`;
    }}
    
    window.togFilter = (u) => {{
        const old = state.filters[state.race] || [];
        const next = old.includes(u) ? old.filter(x=>x!==u) : [...old, u];
        setState({{filters: {{...state.filters, [state.race]: next}} }});
    }};
    
    window.togVenue = (v) => {{
        const next = state.venueFilters.includes(v) ? state.venueFilters.filter(x=>x!==v) : [...state.venueFilters, v];
        setState({{venueFilters: next}});
    }};
    
    window.togDist = (d) => {{
        const next = state.distFilters.includes(d) ? state.distFilters.filter(x=>x!==d) : [...state.distFilters, d];
        setState({{distFilters: next}});
    }};
    
    window.togCourse = (c) => {{
        const next = state.courseFilters.includes(c) ? state.courseFilters.filter(x=>x!==c) : [...state.courseFilters, c];
        setState({{courseFilters: next}});
    }};

    function drawCharts() {{
        const d = DATA.chart_data[state.race];
        const blocked = state.filters[state.race] || [];
        const active = d.horses.filter(h => !blocked.includes(h.umaban));
        if(!active.length) return;
        
        const rows = DATA.races[state.race]?.rows || [];
        const rowMap = {{}}; // date+umaban -> row
        rows.forEach(r => {{
            const key = r['日付'] + '-' + r['番'];
            rowMap[key] = r;
        }});

        const pts = [];
        active.forEach(h => h.records.forEach(r => {{
            // Date Filter
            if(state.filterDate && r.date < state.filterDate) return;
            
            // Lookup row for venue/dist
            const rowKey = r.date + '-' + h.umaban;
            const row = rowMap[rowKey];
            
            // Venue Filter (exclude if in list)
            if(state.venueFilters.length > 0 && row && state.venueFilters.includes(row['場所'])) return;
            
            // Distance Filter (exclude if in list)
            if(state.distFilters.length > 0 && row && state.distFilters.includes(row['距離'])) return;
            
            // Course Filter (exclude if in list)
            if(state.courseFilters.length > 0 && state.courseFilters.includes(r.course)) return;
            
            pts.push({{...r, u:h.umaban, w:h.waku, course:r.course||''}});
        }}));
        
        const layout = {{ margin: {{t:10,b:30,l:30,r:10}}, xaxis:{{zeroline:false}}, yaxis:{{zeroline:false}}, showlegend:false }};
        const cfg = {{displayModeBar:false, responsive:true}};
        
        // 芝=明るい緑, ダート=オレンジ (色覚に配慮した高彩度配色)
        const courseColor = (c) => c === '芝' ? '#00C853' : (c === 'ダート' || c === 'ダ') ? '#FF6D00' : '#888';
        const borderColors = pts.map(p => courseColor(p.course));
        
        Plotly.newPlot('c1', [{{
            x: pts.map(p=>p.l3f_idx), y: pts.map(p=>p.time_idx), mode:'markers+text', type:'scatter',
            marker: {{size:12, color:pts.map(p=>WAKU[p.w]), line:{{width:2,color:borderColors}}}},
            text: pts.map(p=>p.u), textposition:'middle center', textfont:{{size:9, color:pts.map(p=>TXT[p.w])}},
            hovertext: pts.map(p=>`${{p.date}}<br>${{p.u}}番 (${{p.course}})`)
        }}], {{...layout, xaxis:{{title:'上がり指数'}}, yaxis:{{title:'T指数'}}}}, cfg);
        
        Plotly.newPlot('c2', [{{
            x: pts.map(p=>p.c1_ratio), y: pts.map(p=>p.time_idx), mode:'markers+text', type:'scatter',
            marker: {{size:12, color:pts.map(p=>WAKU[p.w]), line:{{width:2,color:borderColors}}}},
            text: pts.map(p=>p.u), textposition:'middle center', textfont:{{size:9, color:pts.map(p=>TXT[p.w])}},
            hovertext: pts.map(p=>`${{p.date}}<br>${{p.u}}番 (${{p.course}})`)
        }}], {{...layout, xaxis:{{title:'位置取り'}}, yaxis:{{title:'T指数'}}}}, cfg);

        Plotly.newPlot('c3', [{{
            x: pts.map(p=>p.c1_ratio), y: pts.map(p=>p.l3f_idx), mode:'markers+text', type:'scatter',
            marker: {{size:12, color:pts.map(p=>WAKU[p.w]), line:{{width:2,color:borderColors}}}},
            text: pts.map(p=>p.u), textposition:'middle center', textfont:{{size:9, color:pts.map(p=>TXT[p.w])}},
            hovertext: pts.map(p=>`${{p.date}}<br>${{p.u}}番 (${{p.course}})`)
        }}], {{...layout, xaxis:{{title:'位置取り'}}, yaxis:{{title:'上がり指数'}}, autorange:'reversed'}}, cfg);

        Plotly.newPlot('c4', [{{
            x: pts.map(p=>p.rpci), y: pts.map(p=>p.l3f_idx), mode:'markers+text', type:'scatter',
            marker: {{size:12, color:pts.map(p=>WAKU[p.w]), line:{{width:2,color:borderColors}}}},
            text: pts.map(p=>p.u), textposition:'middle center', textfont:{{size:9, color:pts.map(p=>TXT[p.w])}},
            hovertext: pts.map(p=>`${{p.date}}<br>${{p.u}}番 (${{p.course}})`)
        }}], {{...layout, xaxis:{{title:'RPCI'}}, yaxis:{{title:'上がり指数'}}}}, cfg);
    }}

    // --- DATA ---
    function renderData() {{
        const rows = DATA.races[state.race]?.rows || [];
        const headers = COLS.map(c => 
            `<div class="sort-chip ${{state.sortCol===c?'active':''}}" onclick="setState({{sortCol:'${{c}}', sortAsc:${{state.sortCol===c?!state.sortAsc:true}} }})">
                ${{c}} ${{state.sortCol===c ? (state.sortAsc?'▲':'▼') : ''}}
             </div>`
        ).join('');

        let content = '';
        if(state.sortCol) {{
            const sorted = [...rows].sort((a,b) => {{
                let va = a[state.sortCol]||'', vb = b[state.sortCol]||'';
                let na = parseFloat(va), nb = parseFloat(vb);
                if(!isNaN(na) && !isNaN(nb)) return state.sortAsc ? na-nb : nb-na;
                return state.sortAsc ? va.localeCompare(vb) : vb.localeCompare(va);
            }});
            content = sorted.map(row => `
                <div class="card" style="padding:8px;">
                    <div class="flex-ac gap-2" style="border-bottom:1px solid #eee; margin-bottom:4px; padding-bottom:4px;">
                        <div class="h-num" style="background:${{WAKU[getWaku(row['番'])] || '#ccc'}}; color:${{TXT[getWaku(row['番'])] || '#fff'}}">${{row['番']}}</div>
                        <div class="text-xs font-bold" style="flex:1">${{row['馬名']}}</div>
                        <div class="text-xs text-light">${{row['日付']}}</div>
                        <div class="font-bold text-xs" style="color:${{state.sortCol}}">${{row[state.sortCol]}}</div>
                    </div>
                    <div class="grid-dense">
                        ${{COLS.map(c => `<div class="gd-cell"><span class="gd-lbl">${{c}}</span><div class="gd-val">${{row[c]||'-'}}</div></div>`).join('')}}
                    </div>
                </div>
            `).join('');
            content = `<div class="card" onclick="setState({{sortCol:null}})" style="text-align:center;padding:8px;color:var(--primary);font-weight:bold;cursor:pointer">↺ グループ表示に戻す</div>` + content;

        }} else {{
            const groups = {{}};
            rows.forEach(r => {{
                const k = r['番']||'0';
                if(!groups[k]) groups[k] = [];
                groups[k].push(r);
            }});
            content = Object.keys(groups).sort((a,b)=>parseInt(a)-parseInt(b)).map(k => {{
                const list = groups[k];
                const meta = list[0];
                const w = getWaku(k);
                const open = state.expanded[state.race+'-'+k];
                const arrow = open ? '▲' : '▼';
                const body = open ? `
                    <div style="margin-top:8px">
                        ${{list.map(row => `
                            <div style="margin-bottom:8px; border-bottom:1px dashed #eee; padding-bottom:4px;">
                                <div class="grid-dense">
                                    ${{COLS.map(c => `<div class="gd-cell"><span class="gd-lbl">${{c}}</span><div class="gd-val">${{row[c]||'-'}}</div></div>`).join('')}}
                                </div>
                            </div>
                        `).join('')}}
                    </div>` : '';
                return `
                <div class="card" style="padding:8px; cursor:pointer" onclick="togExpand('${{k}}')">
                    <div class="flex-ac">
                        <div class="h-num" style="background:${{WAKU[w]}}; color:${{TXT[w]}}">${{k}}</div>
                        <div class="flex-c" style="flex:1">
                            <div class="font-bold text-sm">${{meta['馬名']}}</div>
                            <div class="text-xs text-light">${{meta['騎手']}} (${{list.length}}走)</div>
                        </div>
                        <div class="text-light">${{arrow}}</div>
                    </div>
                    ${{body}}
                </div>`;
            }}).join('');
        }}
        return `<div class="card"><div class="font-bold text-xs mb-2">▼ 並び替え（タップでランキング表示）</div><div class="sort-area">${{headers}}</div></div>${{content}}`;
    }}

    window.togExpand = (k) => {{
        const key = state.race+'-'+k;
        setState({{expanded: {{...state.expanded, [key]:!state.expanded[key]}} }});
    }};
    
    window.togMatchup = (k) => {{
        const key = state.race+'-m-'+k;
        setState({{expanded: {{...state.expanded, [key]:!state.expanded[key]}} }});
    }};

    // --- MATCHUP ---
    function renderMatchup() {{
        const m = DATA.matchups[state.race];
        const mx = DATA.matrix[state.race];
        if(!m) return '<div class="card">データなし</div>';
        
        const sortedKeys = Object.keys(m).sort((a,b)=>parseInt(a)-parseInt(b));
        
        const headRow = sortedKeys.map(k => `<th>${{k}}</th>`).join('');
        const matrixBody = sortedKeys.map(h1 => {{
            const cells = sortedKeys.map(h2 => {{
                if(h1 === h2) return '<td style="background:#ddd">-</td>';
                const rec = (mx[h1] && mx[h1][h2]);
                if(!rec) return '<td></td>';
                // rec: w, l, d
                let cls = 'mx-draw'; 
                let txt = '';
                if(rec.w > 0) {{ cls='mx-win'; txt=`${{rec.w}}勝`; }}
                else if(rec.l > 0) {{ cls='mx-lose'; txt=`${{rec.l}}敗`; }}
                else {{ txt=`${{rec.d}}分`; }}
                return `<td class="${{cls}}">${{txt}}</td>`;
            }}).join('');
            return `<tr><th style="background:${{WAKU[getWaku(h1)]}};color:${{TXT[getWaku(h1)]}}">${{h1}}</th>${{cells}}</tr>`;
        }}).join('');

        const matrixHTML = `
            <div class="card">
                <div class="font-bold text-xs mb-2">対戦早見表 (縦 vs 横)</div>
                <div class="matrix-box">
                    <table class="matrix-table">
                        <thead><tr><th></th>${{headRow}}</tr></thead>
                        <tbody>${{matrixBody}}</tbody>
                    </table>
                </div>
            </div>`;

        const listHTML = sortedKeys.map(k => {{
            const list = m[k];
            const w = getWaku(k);
            const stats = {{w:0, l:0, d:0}};
            list.forEach(x => {{ if(x.result=='Win')stats.w++; else if(x.result=='Lose')stats.l++; else stats.d++; }});
            
            const open = state.expanded[state.race+'-m-'+k];
            const arrow = open ? '▲' : '▼';
            
            const details = open ? list.map(x => {{
                let cls = 'mx-draw';
                if(x.result==='Win') cls='mx-win';
                if(x.result==='Lose') cls='mx-lose';
                return `
                <div style="padding:8px; border-left:4px solid transparent; margin-bottom:6px; font-size:11px; background:#f9fafb; border-radius:4px" class="${{cls}}">
                    <div class="flex-sb" style="margin-bottom:4px;">
                        <div class="font-bold">vs ${{x.opponent_name}} (${{x.opponent_num}}番)</div>
                        <div style="font-weight:bold">${{x.result}}</div>
                    </div>
                    <div class="text-xs text-light" style="margin-bottom:4px;">
                        ${{x.date}} ${{x.place}} ${{x.race_name}} (${{x.course}} ${{x.dist}})
                    </div>
                    <div style="display:grid; grid-template-columns:1fr 1fr; gap:8px;">
                        <div>
                            <div class="text-xs text-light">自分 (${{x.my_rank}}着)</div>
                            <div class="grid-dense" style="grid-template-columns:1fr 1fr; margin-top:2px;">
                                <div class="gd-cell"><span class="gd-lbl">T指数</span><div class="gd-val">${{x.my_idx}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">通過</span><div class="gd-val">${{x.my_pass}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">上がり</span><div class="gd-val">${{x.my_l3f}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">RPCI</span><div class="gd-val">${{x.my_rpci}}</div></div>
                                <div class="gd-cell" style="grid-column:1/3"><span class="gd-lbl">タイム</span><div class="gd-val">${{x.time}}</div></div>
                            </div>
                        </div>
                        <div>
                            <div class="text-xs text-light">相手 (${{x.op_rank}}着)</div>
                            <div class="grid-dense" style="grid-template-columns:1fr 1fr; margin-top:2px;">
                                <div class="gd-cell"><span class="gd-lbl">T指数</span><div class="gd-val">${{x.op_idx}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">通過</span><div class="gd-val">${{x.op_pass}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">上がり</span><div class="gd-val">${{x.op_l3f}}</div></div>
                                <div class="gd-cell"><span class="gd-lbl">RPCI</span><div class="gd-val">${{x.op_rpci}}</div></div>
                                <div class="gd-cell" style="grid-column:1/3"><span class="gd-lbl">タイム</span><div class="gd-val">${{x.op_time}}</div></div>
                            </div>
                        </div>
                    </div>
                </div>`;
            }}).join('') : '';

            // border-bottom style adjust
            const bb = open ? '1px solid #eee' : 'none';

            return `
            <div class="card" style="padding:0">
                <div class="flex-ac" style="padding:8px; border-bottom:${{bb}}; cursor:pointer" onclick="togMatchup('${{k}}')">
                    <div class="h-num" style="background:${{WAKU[w]}}; color:${{TXT[w]}}">${{k}}</div>
                    <div class="font-bold text-sm">対戦成績: ${{stats.w}}勝 ${{stats.l}}敗 ${{stats.d}}分</div>
                    <div class="text-light" style="margin-left:auto">${{arrow}}</div>
                </div>
                ${{open ? `<div style="padding:8px">${{details}}</div>` : ''}}
            </div>`;
        }}).join('');

        return matrixHTML + listHTML;
    }}

    function renderNav() {{
        const tabs = [{{id:'chart',l:'チャート',i:'📊'}},{{id:'data',l:'データ',i:'📋'}},{{id:'matchup',l:'比較',i:'⚔️'}}];
        return `<div class="nav">${{tabs.map(t=>
            `<div class="nav-btn ${{state.tab==t.id?'active':''}}" onclick="setState({{tab:'${{t.id}}'}})">
                <div class="nav-icon">${{t.i}}</div>${{t.l}}
             </div>`).join('')}}</div>`;
    }}
    
    init();
</script>
</body>
</html>
'''
    return html

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="インタラクティブレポート生成")
    parser.add_argument("source", help="元のレポートファイル")
    parser.add_argument("-o", "--output", help="出力ファイル")
    args = parser.parse_args()
    result = create_interactive_charts(args.source, args.output)
    print(f"完了: {result}" if result else "失敗")

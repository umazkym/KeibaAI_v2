# -*- coding: utf-8 -*-
"""
オッズHTMLパーサー

.binファイル（HTML）をパースしてJSONに変換。
既存パーサー（results_parser.py等）と同様のパターン。
"""
import re
import json
import logging
from pathlib import Path
from typing import Dict, List, Optional, Any
from datetime import datetime
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


def parse_odds_win_place(html_content: str, race_id: str) -> Dict[str, Any]:
    """
    単勝/複勝オッズ HTMLをパース
    
    複勝オッズが範囲形式（3.3-5.2）の場合は最小値を使用
    
    Returns:
        {
            'race_id': str,
            'bet_type': 'win_place',
            'data': [
                {'horse_number': int, 'win_odds': float, 'place_odds': float},
                ...
            ]
        }
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = []
    
    # 2つのテーブル（単勝・複勝）を取得
    tables = soup.find_all('table', class_='RaceOdds_HorseList_Table')
    
    win_odds_map = {}
    place_odds_map = {}
    
    for idx, table in enumerate(tables[:2]):
        rows = table.find_all('tr')
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 6:
                continue
            
            # 馬番（2番目のセル）
            try:
                horse_num = int(cells[1].get_text(strip=True))
            except:
                continue
            
            # オッズ（6番目のセル内のspan）
            odds_cell = cells[5]
            odds_span = odds_cell.find('span', class_='Odds')
            if not odds_span:
                odds_span = odds_cell.find('span')
            
            if odds_span:
                odds_text = odds_span.get_text(strip=True).replace(',', '')
                
                if idx == 0:  # 単勝テーブル
                    try:
                        win_odds_map[horse_num] = float(odds_text)
                    except:
                        pass
                else:  # 複勝テーブル
                    # "1.5-2.3" 形式 or "1.5 - 2.3" 形式	→ 最小値を使用
                    match = re.match(r'([\d.]+)\s*-\s*([\d.]+)', odds_text)
                    if match:
                        place_odds_map[horse_num] = float(match.group(1))  # 最小値のみ
                    else:
                        try:
                            place_odds_map[horse_num] = float(odds_text)
                        except:
                            pass
    
    # 結合
    all_horses = set(win_odds_map.keys()) | set(place_odds_map.keys())
    for horse_num in sorted(all_horses):
        entry = {
            'horse_number': horse_num,
            'win_odds': win_odds_map.get(horse_num),
            'place_odds': place_odds_map.get(horse_num)  # 最小値のみ
        }
        data.append(entry)
    
    return {
        'race_id': race_id,
        'bet_type': 'win_place',
        'data': data,
        'fetched_at': datetime.now().isoformat()
    }


def parse_odds_matrix(html_content: str, race_id: str, bet_type: str) -> Dict[str, Any]:
    """
    馬連/ワイド/馬単マトリックスHTMLをパース
    
    馬連/ワイド: 各Odds_Tableが1軸馬の組み合わせを表す
    馬単: 1着-2着の順序あり組み合わせ
    
    Returns:
        {
            'race_id': str,
            'bet_type': str,
            'data': [
                {'horse1': int, 'horse2': int, 'odds': float},
                ...
            ]
        }
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = []
    
    # 全てのOdds_Tableを取得
    tables = soup.find_all('table', class_='Odds_Table')
    
    if not tables:
        logger.warning(f"マトリックステーブルが見つかりません: {race_id} ({bet_type})")
        return {'race_id': race_id, 'bet_type': bet_type, 'data': []}
    
    for table in tables:
        # ヘッダー行から軸馬番号を取得
        header_row = table.find('tr')
        if not header_row:
            continue
        
        header_cells = header_row.find_all(['th', 'td'])
        axis_horse = None
        for cell in header_cells:
            text = cell.get_text(strip=True)
            if text.isdigit():
                axis_horse = int(text)
                break
        
        if axis_horse is None:
            continue
        
        # データ行をパース
        rows = table.find_all('tr')[1:]  # ヘッダー除く
        for row in rows:
            cells = row.find_all('td')
            if len(cells) < 2:
                continue
            
            # 馬番（最初のセル）
            try:
                partner_horse = int(cells[0].get_text(strip=True))
            except:
                continue
            
            # オッズ（2番目のセル）- ワイドは範囲形式で複数spanの場合あり
            odds_cell = cells[1]
            
            # 最初のspan要素からオッズを取得（連結問題を回避）
            odds_span = odds_cell.find('span')
            if odds_span:
                odds_text = odds_span.get_text(strip=True).replace(',', '')
            else:
                odds_text = odds_cell.get_text(strip=True).replace(',', '')
            
            # 範囲形式（3.3-5.2）の場合は最小値を使用
            match = re.match(r'([\d.]+)\s*-\s*([\d.]+)', odds_text)
            if match:
                odds = float(match.group(1))  # 最小値
            else:
                try:
                    odds = float(odds_text)
                except:
                    continue
            
            # 馬単は順序あり（axis_horse=1着, partner_horse=2着）
            if bet_type == 'umatan':
                data.append({'horse1': axis_horse, 'horse2': partner_horse, 'odds': odds})
            else:
                # 馬連/ワイド: 順序なし（小さい番号, 大きい番号）
                h1, h2 = min(axis_horse, partner_horse), max(axis_horse, partner_horse)
                data.append({'horse1': h1, 'horse2': h2, 'odds': odds})
    
    # 重複除去（馬連/ワイドの場合）
    if bet_type != 'umatan':
        seen = set()
        unique_data = []
        for d in data:
            key = (d['horse1'], d['horse2'])
            if key not in seen:
                seen.add(key)
                unique_data.append(d)
        data = unique_data
    
    return {
        'race_id': race_id,
        'bet_type': bet_type,
        'data': data,
        'fetched_at': datetime.now().isoformat()
    }


def parse_odds_trio(html_content: str, race_id: str, bet_type: str) -> Dict[str, Any]:
    """
    三連複/三連単オッズ一覧HTMLをパース（軸馬ベース）
    
    オッズ一覧ビューの構造:
    - 軸馬（1着/三連単、または軸馬/三連複）: Axis_Horse div
    - 2着馬: Odds_Tableのヘッダー（th）
    - 3着馬: 各行のtd.Waku_Normal
    - オッズ: span[id^="odds-"]（例: odds-8-010203 = 1→2→3）
    
    Returns:
        {
            'race_id': str,
            'bet_type': str,
            'data': [
                {'horse1': int, 'horse2': int, 'horse3': int, 'odds': float},
                ...
            ]
        }
    """
    soup = BeautifulSoup(html_content, 'html.parser')
    data = []
    seen = set()  # 重複除去用
    
    # 方法1: span[id^="odds-"]から直接組み合わせを抽出
    odds_spans = soup.find_all('span', id=re.compile(r'^odds-[78]-'))
    
    for span in odds_spans:
        span_id = span.get('id', '')
        odds_text = span.get_text(strip=True).replace(',', '')
        
        # ID形式: odds-8-010203 (三連単) or odds-7-010203 (三連複)
        match = re.match(r'odds-[78]-(\d{2})(\d{2})(\d{2})', span_id)
        if match:
            try:
                horse1 = int(match.group(1))
                horse2 = int(match.group(2))
                horse3 = int(match.group(3))
                odds = float(odds_text)
                
                # 三連複は順序なし、三連単は順序あり
                if bet_type == 'sanrenpuku':
                    key = tuple(sorted([horse1, horse2, horse3]))
                else:
                    key = (horse1, horse2, horse3)
                
                if key not in seen:
                    seen.add(key)
                    if bet_type == 'sanrenpuku':
                        data.append({
                            'horse1': key[0],
                            'horse2': key[1],
                            'horse3': key[2],
                            'odds': odds
                        })
                    else:
                        data.append({
                            'horse1': horse1,
                            'horse2': horse2,
                            'horse3': horse3,
                            'odds': odds
                        })
            except:
                continue
    
    # オッズでソート
    data.sort(key=lambda x: x['odds'])
    
    return {
        'race_id': race_id,
        'bet_type': bet_type,
        'data': data,
        'fetched_at': datetime.now().isoformat()
    }


def parse_odds_bin_file(file_path: Path, race_id: str, bet_type: str) -> Dict[str, Any]:
    """
    .binファイルを読み込んでパース
    """
    try:
        # エンコーディングを試行
        for encoding in ['utf-8', 'euc_jp', 'shift_jis']:
            try:
                with open(file_path, 'r', encoding=encoding) as f:
                    html_content = f.read()
                break
            except UnicodeDecodeError:
                continue
        else:
            with open(file_path, 'rb') as f:
                html_content = f.read().decode('utf-8', errors='replace')
        
        if bet_type == 'tansho_fukusho':
            return parse_odds_win_place(html_content, race_id)
        elif bet_type in ['umaren', 'wide', 'umatan']:
            return parse_odds_matrix(html_content, race_id, bet_type)
        elif bet_type in ['sanrenpuku', 'sanrentan']:
            return parse_odds_trio(html_content, race_id, bet_type)
        else:
            logger.warning(f"未知の券種: {bet_type}")
            return {'race_id': race_id, 'bet_type': bet_type, 'data': []}
            
    except Exception as e:
        logger.error(f"パースエラー: {file_path}: {e}")
        return {'race_id': race_id, 'bet_type': bet_type, 'data': [], 'error': str(e)}


def parse_all_odds_for_race(
    race_id: str,
    input_base_dir: Path,
    output_dir: Optional[Path] = None
) -> Dict[str, Any]:
    """
    1レースの全券種オッズをパースしてParquetに保存
    
    Args:
        race_id: レースID
        input_base_dir: 入力ベースディレクトリ（data/raw/html/odds）
        output_dir: 出力ディレクトリ（data/parsed/parquet/odds）
    
    Returns:
        統合されたオッズデータ
    """
    import pandas as pd
    
    result = {}
    
    bet_types = ['tansho_fukusho', 'umaren', 'wide', 'umatan', 'sanrenpuku', 'sanrentan']
    
    for bet_type in bet_types:
        input_file = input_base_dir / bet_type / f"{race_id}.bin"
        if input_file.exists():
            parsed = parse_odds_bin_file(input_file, race_id, bet_type)
            result[bet_type] = parsed
    
    # Parquet形式で保存
    if output_dir:
        output_dir.mkdir(parents=True, exist_ok=True)
        
        for bet_type, data in result.items():
            if bet_type == 'metadata':
                continue
            if not data.get('data'):
                continue
            
            # DataFrameに変換
            df = pd.DataFrame(data['data'])
            df['race_id'] = race_id
            
            # bet_type別ディレクトリに保存
            bt_dir = output_dir / bet_type
            bt_dir.mkdir(parents=True, exist_ok=True)
            output_file = bt_dir / f"{race_id}.parquet"
            df.to_parquet(output_file, index=False)
    
    return result


def parse_odds_batch(
    input_base_dir: Path,
    output_dir: Path,
    skip_existing: bool = True
) -> int:
    """
    全レースのオッズをパース
    
    Returns:
        処理したレース数
    """
    from tqdm import tqdm
    
    # race_idリストを取得（tansho_fukushoディレクトリから）
    tansho_dir = input_base_dir / 'tansho_fukusho'
    if not tansho_dir.exists():
        logger.warning(f"入力ディレクトリが存在しません: {tansho_dir}")
        return 0
    
    race_ids = [f.stem for f in tansho_dir.glob('*.bin')]
    logger.info(f"パース対象: {len(race_ids)}レース")
    
    count = 0
    for race_id in tqdm(race_ids, desc="オッズパース", unit="レース"):
        output_file = output_dir / f"{race_id}.json"
        
        if skip_existing and output_file.exists():
            continue
        
        parse_all_odds_for_race(race_id, input_base_dir, output_dir)
        count += 1
    
    logger.info(f"パース完了: {count}件")
    return count

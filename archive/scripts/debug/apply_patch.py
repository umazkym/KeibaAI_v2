# Quick patch for results_parser.py の Line 357-390を修正
# 手動パッチファイル

ORIGINAL_CODE = """    # 列数に応じた柔軟な処理
    if len(cells) >= 18:
        # 通常のフォーマット（調教師、馬主、賞金の順）
        trainer_idx = 15
        owner_idx = 16
        prize_idx = 17
        
        # 賞金があるかチェック（1着の場合）
        if row_data['finish_position'] == 1 and len(cells) > prize_idx:
            prize_text = cells[prize_idx].get_text(strip=True)
            if prize_text and prize_text.replace(',', '').replace('.', '').isdigit():
                row_data['prize_money'] = parse_prize_money(prize_text)
            else:
                # 賞金がない場合、インデックスをシフト
                trainer_idx = 15
                owner_idx = 16
        
        # 調教師
        if len(cells) > trainer_idx:
            trainer_cell = cells[trainer_idx]
            trainer_link = trainer_cell.find('a')
            if trainer_link:
                row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
                row_data['trainer_name'] = trainer_link.get_text(strip=True)
        
        # 馬主
        if len(cells) > owner_idx:
            owner_cell = cells[owner_idx]
            owner_text = owner_cell.get_text(strip=True)
            if owner_text and owner_text not in ['---', '']:
                row_data['owner_name'] = normalize_owner_name(owner_text)"""

FIXED_CODE = """    # 列数に応じた柔軟な処理 (修正版)
    if len(cells) >= 21:
        trainer_idx, owner_idx, prize_idx = 18, 19, 20
    elif len(cells) >= 18:
        trainer_idx, owner_idx, prize_idx = 15, 16, 17
    else:
        return row_data
    
    # 調教師
    trainer_cell = cells[trainer_idx]
    trainer_link = trainer_cell.find('a')
    if trainer_link:
        row_data['trainer_id'] = get_id_from_href(trainer_link.get('href'), 'trainer')
        row_data['trainer_name'] = trainer_link.get_text(strip=True)
    
    # 馬主
    owner_cell = cells[owner_idx]
    owner_text = owner_cell.get_text(strip=True)
    if owner_text and owner_text not in ['---',  '']:
        row_data['owner_name'] = normalize_owner_name(owner_text)
    
    # 賞金（1-5位）
    finish_pos = row_data.get('finish_position')
    if finish_pos and 1 <= finish_pos <= 5:
        prize_text = cells[prize_idx].get_text(strip=True)
        if prize_text and prize_text.replace(',', '').replace('.', '').isdigit():
            row_data['prize_money'] = parse_prize_money(prize_text)"""

# ファイルを読み込み
with open('keibaai/src/modules/parsers/results_parser.py', 'r', encoding='utf-8') as f:
    content = f.read()

# 置換
if ORIGINAL_CODE.strip() in content:
    new_content = content.replace(ORIGINAL_CODE, FIXED_CODE)
    
    # 保存
    with open('keibaai/src/modules/parsers/results_parser.py', 'w', encoding='utf-8') as f:
        f.write(new_content)
    
    print("✓ 修正完了！")
else:
    print("✗ 元のコードが見つかりません。手動で確認が必要です。")

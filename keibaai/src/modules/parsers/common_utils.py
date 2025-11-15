import re
from typing import Optional

def parse_int_or_none(text: str) -> Optional[int]:
    """
    文字列をintに変換、失敗時はNone
    """
    if not text or text in ['---', '---.-', ''] or text.strip() == '':
        return None

    # 数字のみ抽出
    digits = re.sub(r'[^\d]', '', text).strip()

    if not digits:
        return None

    try:
        result = int(digits)
        # 0以下の値は無効とする（人気、出走数等は正数）
        return result if result > 0 else None
    except (ValueError, TypeError):
        return None


def parse_float_or_none(text: str) -> Optional[float]:
    """
    文字列をfloatに変換、失敗時はNone
    """
    if not text or text in ['---', '---.-', ''] or text.strip() == '':
        return None

    # 数字とドットのみ抽出
    cleaned = re.sub(r'[^\d.]', '', text).strip()

    if not cleaned or cleaned in ['.', '']:
        return None

    try:
        result = float(cleaned)
        # 0以下の値は無効とする（確率/オッズは正数）
        return result if result > 0 else None
    except (ValueError, TypeError):
        return None


def parse_sex_age(sex_age_text: str) -> tuple:
    """
    性齢文字列をパース
    例: "牡3" → ("牡", 3)
    """
    if not sex_age_text:
        return (None, None)
    
    # 性別（1文字目）
    sex = sex_age_text[0] if len(sex_age_text) > 0 else None
    
    # 年齢（残り）
    age_str = sex_age_text[1:]
    age = parse_int_or_none(age_str)
    
    return (sex, age)


def parse_time_to_seconds(time_str: str) -> Optional[float]:
    """
    タイム文字列を秒数に変換
    例: "1:59.8" → 119.8
    """
    if not time_str or time_str == '---':
        return None
    
    # 分:秒.小数 の形式
    match = re.match(r'(\d+):(\d+)\.(\d+)', time_str)
    if match:
        minutes = int(match.group(1))
        seconds = int(match.group(2))
        decimal = int(match.group(3))
        
        total_seconds = minutes * 60 + seconds + decimal / 10.0
        return total_seconds
    
    # 秒.小数 の形式
    match = re.match(r'(\d+)\.(\d+)', time_str)
    if match:
        seconds = int(match.group(1))
        decimal = int(match.group(2))
        return seconds + decimal / 10.0
    
    return None


def parse_margin_to_seconds(margin_str: str) -> Optional[float]:
    """
    着差文字列を秒数に変換（推定値）
    
    Args:
        margin_str: 着差文字列
    
    Returns:
        着差秒数
    """
    if not margin_str or margin_str in ['---', '']:
        # 1着の場合は着差がないのでNoneを返す
        return None
    
    # JRAの基準などを参考に、一般的な値をマッピング
    # ▼▼▼ 修正箇所: "大"を追加 ▼▼▼
    special_margins = {
        '同着': 0.0,
        'ハナ': 0.02,
        'アタマ': 0.04,
        'クビ': 0.05,
        '大差': 5.0,
        '大': 5.0,  # ← 追加（"大差"と同等の扱い）
    }
    # ▲▲▲ 修正箇所 ▲▲▲
    
    if margin_str in special_margins:
        return special_margins[margin_str]
    
    # 分数表記（例: "1.1/2", "3/4"）
    # 1馬身 = 0.2秒として換算
    SECONDS_PER_LENGTH = 0.2
    total_length = 0.0
    
    try:
        # "1.1/2" のような形式
        if '.' in margin_str and '/' in margin_str:
            integer_part_str, fraction_str = margin_str.split('.')
            total_length += float(integer_part_str)
            
            numerator, denominator = fraction_str.split('/')
            total_length += float(numerator) / float(denominator)
        
        # "3/4" のような形式
        elif '/' in margin_str:
            numerator, denominator = margin_str.split('/')
            total_length += float(numerator) / float(denominator)
            
        # "5" のような整数形式
        else:
            total_length = float(margin_str)
            
        return round(total_length * SECONDS_PER_LENGTH, 3)
        
    except (ValueError, ZeroDivisionError):
        return None


def parse_horse_weight(weight_text: str) -> tuple:
    """
    馬体重文字列をパース
    例: "478(+2)" → (478, 2)
    例: "450(-5)" → (450, -5)
    
    Returns:
        (馬体重, 増減)
    """
    if not weight_text or weight_text == '---':
        return (None, None)
    
    # パターン: 数字(+/-数字)
    match = re.match(r'(\d+)\(([+-]?\d+)\)', weight_text)
    if match:
        weight = int(match.group(1))
        change = int(match.group(2))
        return (weight, change)
    
    # 数字のみ
    match = re.match(r'(\d+)', weight_text)
    if match:
        weight = int(match.group(1))
        return (weight, None)
    
    return (None, None)


def parse_prize_money(prize_text: str) -> Optional[int]:
    """
    賞金文字列をパース（万円単位）
    例: "1,348.6" → 1349
    例: "---.-" → None
    """
    if not prize_text or prize_text == '---' or prize_text.strip() == '---.-' or prize_text.strip() == '':
        return None

    # カンマ除去、不正フォーマット除去
    cleaned = prize_text.replace(',', '').strip()

    if cleaned in ['---', '---.-', ''] or '---' in cleaned:
        return None

    try:
        # floatに変換してからintに丸める
        return int(round(float(cleaned)))
    except (ValueError, TypeError):
        return None


def normalize_owner_name(owner_text: str) -> Optional[str]:
    """
    馬主名を正規化
    例: "---.-" → None
    例: "Owner Name" → "Owner Name"
    """
    if not owner_text or owner_text.strip() == '':
        return None

    cleaned = owner_text.strip()

    # 不正フォーマット除去
    if cleaned in ['---', '---.-', '編集']:
        return None

    return cleaned


def parse_owner_odds(odds_text: str) -> Optional[float]:
    """
    馬主に関連するオッズ/確率文字列をパース
    例: "2.5" → 2.5
    例: "---" → None
    例: "---.-" → None
    """
    if not odds_text or odds_text.strip() == '':
        return None

    cleaned = odds_text.replace(',', '').strip()

    if cleaned in ['---', '---.-', '']:
        return None

    try:
        return float(cleaned)
    except (ValueError, TypeError):
        return None

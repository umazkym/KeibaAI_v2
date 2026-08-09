#!/usr/bin/env bash
# lancers-design-proposal ローカル導入スクリプト（macOS / Linux / WSL）
#
# 実行するもの:
#   1. スキルを ~/.claude/skills/lancers-design-proposal/ に配置
#   2. 作業フォルダを ~/lancers/ に配置
#
# 既存のものは上書きせず退避する。何度実行しても壊れない。

set -euo pipefail

HERE="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
SKILL_DST="$HOME/.claude/skills/lancers-design-proposal"
WORK_DST="$HOME/lancers"
STAMP="$(date +%Y%m%d%H%M%S)"

echo "==> スキルを配置します"
mkdir -p "$HOME/.claude/skills"
if [ -e "$SKILL_DST" ]; then
  mv "$SKILL_DST" "$SKILL_DST.bak.$STAMP"
  echo "    既存のスキルを退避: $SKILL_DST.bak.$STAMP"
fi
cp -R "$HERE/lancers-design-proposal" "$SKILL_DST"
echo "    配置しました: $SKILL_DST"

echo "==> 作業フォルダを配置します"
if [ -e "$WORK_DST" ]; then
  echo "    既に $WORK_DST があります。中身には触れていません。"
  if [ ! -e "$WORK_DST/_templates" ]; then
    cp -R "$HERE/lancers/_templates" "$WORK_DST/_templates"
    echo "    _templates/ だけ追加しました"
  fi
else
  cp -R "$HERE/lancers" "$WORK_DST"
  echo "    配置しました: $WORK_DST"
fi

echo
echo "==> 確認"
for p in \
  "$SKILL_DST/SKILL.md" \
  "$SKILL_DST/assets/user-profile.md" \
  "$SKILL_DST/references/anti-ai-design.md" \
  "$WORK_DST/README.md" \
  "$WORK_DST/_templates" \
  "$WORK_DST/_archive/採用" \
  "$WORK_DST/_archive/不採用"
do
  if [ -e "$p" ]; then echo "    OK   $p"; else echo "    NG   $p  ← 見つかりません"; fi
done

echo
echo "==> 次にやること"
echo "    1. プロフィールを埋める（提案文と見積の精度が上がる）"
echo "       \$EDITOR $SKILL_DST/assets/user-profile.md"
echo
echo "    2. 案件フォルダを作って起動する"
echo "       cd ~/lancers"
echo "       cp -r _templates/YYYYMMDD_業種_案件概要 \$(date +%Y%m%d)_業種_案件概要"
echo "       cd \$(date +%Y%m%d)_業種_案件概要 && claude"
echo
echo "    3. 起動後 /model でモデルを選び、/skills に"
echo "       lancers-design-proposal が出ているか確認する"

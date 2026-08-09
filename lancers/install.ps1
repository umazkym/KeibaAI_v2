# lancers-design-proposal ローカル導入スクリプト（Windows PowerShell）
#
# 実行するもの:
#   1. スキルを %USERPROFILE%\.claude\skills\lancers-design-proposal\ に配置
#   2. 作業フォルダを %USERPROFILE%\lancers\ に配置
#
# 既存のものは上書きせず退避する。何度実行しても壊れない。

$ErrorActionPreference = "Stop"

$Here     = Split-Path -Parent $MyInvocation.MyCommand.Path
$SkillDst = Join-Path $env:USERPROFILE ".claude\skills\lancers-design-proposal"
$WorkDst  = Join-Path $env:USERPROFILE "lancers"
$Stamp    = Get-Date -Format "yyyyMMddHHmmss"

Write-Host "==> スキルを配置します"
New-Item -ItemType Directory -Force -Path (Join-Path $env:USERPROFILE ".claude\skills") | Out-Null
if (Test-Path $SkillDst) {
    $Bak = "$SkillDst.bak.$Stamp"
    Move-Item -Path $SkillDst -Destination $Bak
    Write-Host "    既存のスキルを退避: $Bak"
}
Copy-Item -Path (Join-Path $Here "lancers-design-proposal") -Destination $SkillDst -Recurse
Write-Host "    配置しました: $SkillDst"

Write-Host "==> 作業フォルダを配置します"
if (Test-Path $WorkDst) {
    Write-Host "    既に $WorkDst があります。中身には触れていません。"
    $Tpl = Join-Path $WorkDst "_templates"
    if (-not (Test-Path $Tpl)) {
        Copy-Item -Path (Join-Path $Here "lancers\_templates") -Destination $Tpl -Recurse
        Write-Host "    _templates\ だけ追加しました"
    }
} else {
    Copy-Item -Path (Join-Path $Here "lancers") -Destination $WorkDst -Recurse
    Write-Host "    配置しました: $WorkDst"
}

Write-Host ""
Write-Host "==> 確認"
$Checks = @(
    (Join-Path $SkillDst "SKILL.md"),
    (Join-Path $SkillDst "assets\user-profile.md"),
    (Join-Path $SkillDst "references\anti-ai-design.md"),
    (Join-Path $WorkDst  "README.md"),
    (Join-Path $WorkDst  "_templates"),
    (Join-Path $WorkDst  "_archive\採用"),
    (Join-Path $WorkDst  "_archive\不採用")
)
foreach ($p in $Checks) {
    if (Test-Path $p) { Write-Host "    OK   $p" } else { Write-Host "    NG   $p  <- 見つかりません" }
}

Write-Host ""
Write-Host "==> 次にやること"
Write-Host "    1. プロフィールを埋める"
Write-Host "       notepad $SkillDst\assets\user-profile.md"
Write-Host ""
Write-Host "    2. 案件フォルダを作って起動する"
Write-Host "       cd `$env:USERPROFILE\lancers"
Write-Host "       Copy-Item '_templates\YYYYMMDD_業種_案件概要' '20260812_業種_案件概要' -Recurse"
Write-Host "       cd '20260812_業種_案件概要'; claude"
Write-Host ""
Write-Host "    3. 起動後 /model でモデルを選び、/skills に"
Write-Host "       lancers-design-proposal が出ているか確認する"

# ランサーズ案件をローカルで回すための導入セット

Claude Desktop にアップロードしたスキルは、**ローカルの Claude Code には降りてきません。**
同期は「claude.aiアカウント → Cowork／クラウドセッション」の方向だけで、
ローカルセッションは `~/.claude/skills/` を独立して読みます。

なのでローカルで動かすには、同じスキルを `~/.claude/skills/` にも置く必要があります。
このセットはそれを1コマンドで行います。

## 中身

```
lancers-design-proposal/   スキル本体 → ~/.claude/skills/ へ
lancers/                   作業フォルダ → ~/ へ
install.sh                 macOS / Linux / WSL 用
install.ps1                Windows PowerShell 用
```

## 手順

### 1. Claude Code をインストール

既に入っているなら飛ばしてください（`claude --version` で確認）。

**macOS / Linux / WSL**
```bash
curl -fsSL https://claude.ai/install.sh | bash
```

**Windows PowerShell**
```powershell
irm https://claude.ai/install.ps1 | iex
```

**Homebrew を使うなら**
```bash
brew install --cask claude-code
```

Pro / Max / Team / Enterprise のいずれかのプランが必要です（無料プランは対象外）。
インストール後、`claude` を実行するとブラウザでログインを求められます。

Windows では [Git for Windows](https://git-scm.com/downloads/win) を入れておくと
Bash ツールが使えます。無い場合は PowerShell ツールで動きます。

### 2. このセットを配置

**macOS / Linux / WSL**
```bash
bash install.sh
```

**Windows PowerShell**
```powershell
powershell -ExecutionPolicy Bypass -File .\install.ps1
```

既存の `~/.claude/skills/lancers-design-proposal/` や `~/lancers/` があれば
上書きせず退避またはスキップします。何度実行しても壊れません。

### 3. プロフィールを埋める

```bash
open -e ~/.claude/skills/lancers-design-proposal/assets/user-profile.md   # macOS
notepad %USERPROFILE%\.claude\skills\lancers-design-proposal\assets\user-profile.md   # Windows
```

単価・稼働・実績を埋めておくと、見積と応募文が毎回ゼロから組み立てにならずに済みます。
**ここはローカルのただのテキストファイルなので、ZIP再アップロードは不要です。**
Claude Code は `~/.claude/skills/` の変更をセッション中に自動で拾います（再起動不要）。

### 4. 案件を始める

```bash
cd ~/lancers
cp -r _templates/YYYYMMDD_業種_案件概要 20260812_歯科_リニューアル
cd 20260812_歯科_リニューアル
claude
```

起動したら `/model` でモデルを選び、`/skills` に `lancers-design-proposal` が
出ているか確認します。出ていれば「ランサーズの案件を進めたい」で Step 0 から始まります。

`依頼内容/依頼文.md` に依頼文の原文を貼ってから話しかけると、読み込みが速いです。

## ターミナルを使いたくない場合

Claude Desktop アプリには Claude Code が内蔵されていて、**ローカルで動きます**。
GUI から使えて、スキルの読み込み先も通常のローカルセッションと同じ `~/.claude/skills/` です。
`install.sh` を一度走らせておけば、そちらからでも同じように使えます。

ただし Desktop の**通常のチャット**や Cowork セッションは別物で、
そちらはアカウント側のスキルを読み、ローカルファイルには届きません。

## なぜローカルでないと駄目なのか

SKILL.md にも書いてありますが、リモート／クラウドのセッションは
ネットワークが許可ドメインに限定されています。

- 参考サイトや依頼者の既存サイトを取得しようとすると `EGRESS_BLOCKED` で失敗する
- 参考サイトの分析はこのスキルの中核工程なので、回避策がない
- 生成したファイルがセッション終了時に消える

`claude` はターミナルの現在地を作業フォルダにするので、
案件フォルダに `cd` してから起動すると、作業フォルダの取り違えも同時に防げます。

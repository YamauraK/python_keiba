# python_keiba

競馬データを蓄積し、過去の傾向から購入すべき馬券を分析するためのローカル実行向けツールです。Mac 上でそのまま実行することも、Docker コンテナを用いて環境を固定することもできます。

## データファイルのフォーマット

本ツールは以下のカラムを持つ CSV ファイルを想定しています。1 行が 1 頭の出走馬に対応し、同じ `race_id` の行が 1 つのレースを構成します。

| カラム名 | 説明 |
| --- | --- |
| `race_id` | レースを一意に識別する ID（文字列） |
| `date` | レース開催日（例: `2024-05-26`） |
| `racecourse` | 競馬場名 |
| `distance` | 距離 (m) |
| `track_condition` | 馬場状態（良 / 稍重 / 重 / 不良 等） |
| `num_runners` | 出走頭数 |
| `track_direction` | コースの回り（左 / 右 等） |
| `weather` | 天候 |
| `horse_number` | 馬番 |
| `horse_name` | 馬名 |
| `popularity` | 人気順（1 が最も人気） |
| `finish_position` | 着順（数値。1 が 1 着） |
| `odds_win` | 単勝オッズ |
| `odds_place` | 複勝オッズ |
| `return_win` | 単勝 100 円購入時の払戻金（外れた場合は 0） |
| `return_place` | 複勝 100 円購入時の払戻金（外れた場合は 0） |

## ローカル環境での使い方

1. Python 3.11 以上をインストールします。
2. リポジトリ直下で依存関係をセットアップします（現時点では追加ライブラリはありませんが、
   環境構築手順を統一するため `requirements.txt` をインストールする手順を残しています）。

   ```bash
   python -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```

3. データベースを初期化します。

   ```bash
   python -m keiba.cli init-db --db-path keiba.db
   ```

4. CSV を取り込んでデータを蓄積します。

   ```bash
   python -m keiba.cli ingest data/races_2024.csv --db-path keiba.db
   ```

5. 購入シミュレーションを実行します。`--horse-popularities` は出走予定馬の人気順を入力します（例: 1,2,4,6,...）。

   ```bash
   python -m keiba.cli suggest \
       --racecourse "東京" \
       --distance 2400 \
       --track-condition "良" \
       --num-runners 18 \
       --track-direction "左" \
       --weather "晴" \
       --horse-popularities 1 --horse-popularities 2 --horse-popularities 4 \
       --horse-popularities 5 --horse-popularities 7 --horse-popularities 8 \
       --horse-popularities 9 --horse-popularities 10 --horse-popularities 12 \
       --horse-popularities 13 --db-path keiba.db
   ```

   デフォルトでは予算 1 万円を 10 点に均等配分します。`--budget` と `--num-tickets` オプションで変更できます。

## Docker での実行

1. イメージをビルドします。

   ```bash
   docker build -t keiba-analytics .
   ```

2. CSV を取り込む場合は、ホスト側のファイルを `/data` などにマウントして実行します。

   ```bash
   docker run --rm -v $(pwd)/data:/data -v $(pwd)/storage:/storage \
       keiba-analytics init-db --db-path /storage/keiba.db

   docker run --rm -v $(pwd)/data:/data -v $(pwd)/storage:/storage \
       keiba-analytics ingest /data/races_2024.csv --db-path /storage/keiba.db
   ```

3. 予想を出す場合も同様に実行します。

   ```bash
   docker run --rm -v $(pwd)/storage:/storage keiba-analytics suggest \
       --racecourse "東京" --distance 2400 --track-condition "良" \
       --num-runners 18 --track-direction "左" --weather "晴" \
       --horse-popularities 1 --horse-popularities 2 --horse-popularities 3 \
       --horse-popularities 5 --horse-popularities 6 --horse-popularities 7 \
       --horse-popularities 8 --horse-popularities 9 --horse-popularities 10 \
       --horse-popularities 12 --db-path /storage/keiba.db
   ```

   `storage` ディレクトリに SQLite データベースが保存されるので、コンテナを破棄してもデータが保持されます。

## 推奨ワークフロー

1. ローカルで日々のレース結果を CSV にエクスポートして `data/` 以下に保存する。
2. `ingest` コマンドで定期的に取り込むことでデータベースを更新する。
3. レース条件と想定人気を入力して `suggest` を実行し、期待値が高い組み合わせを抽出する。

将来的には機械学習モデルやより高度な指標を組み込むことも可能です。その際も SQLite にデータが蓄積されていれば柔軟に分析を追加できます。


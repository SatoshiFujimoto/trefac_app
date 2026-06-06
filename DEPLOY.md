# trefac_app デプロイ手順（furima 5台への相乗り）

trefac在庫監視を、既存のYahooフリマ用 DigitalOcean droplet 5台に相乗りさせる。
sushi（旧専用機）は移行後に廃止。各箱は **自分のshardだけ** を、furimaの稼働の谷で処理する。

- 入力CSVは **DO Spaces** から実行時にDL（手配り不要・バージョンずれ防止）。
- trefac用は **`/home/fujiken/trefac_app`** に置く。furimaの `/home/fujiken/my_app` には触らない。
- `ebay-pkg` は各箱に既設の `/home/fujiken/ebay-pkg` を再利用。

## 0. 事前準備（1回だけ）

マスター入力CSVを Spaces にアップロード（パスは `trefac/input.csv` 固定）:

```bash
# 例: s3cmd / aws-cli / DOコンソールのいずれかで
#   s3://sushi-onsen-storage/trefac/input.csv
# として現行の "aki - sushionsen_up_04*.csv" を置く
```

更新時はこの1ファイルを上げ替えるだけで全箱に反映される。

## 1. 各箱でのセットアップ

```bash
# (1) コード取得
git clone https://github.com/SatoshiFujimoto/trefac_app.git /home/fujiken/trefac_app
cd /home/fujiken/trefac_app

# (2) venv 作成 + 依存導入（requests, boto3）
python3 -m venv venv
venv/bin/pip install -U pip
venv/bin/pip install -r requirements-trefac.txt

# (3) 設定（秘密はgit外。sushiの実config.iniをコピーするのが最短）
#   setting/config.ini を用意し、最低限:
#     [Credentials] ... 我々のeBayアカウントのトークン等（sushiと同じ値）
#     [spaces]      ... access_key / secret_key / bucket=sushi-onsen-storage / region=sgp1
#                       object_key=trefac/input.csv / local_file=aki - sushionsen_up_04_master.csv
#     [Settings]    ... shard_count = 5      （全台同じ値）
#                       shard_index = auto   （ホスト名 yahoo-inv-0N から自動算出 -> N-1）
#   ★ shard_index=auto なので config.ini は 5台すべて完全に同一でよい（台ごとの手編集不要）。
#     → sushi の実 config.ini をそのまま5台にコピーするのが最短。
#   テンプレートは setting/config.ini.example を参照
cp setting/config.ini.example setting/config.ini   # その後、実値を埋める

# (4) OAuth鍵（gitに入れていない）を配置
#   key/authorized_user.json, key/client_secret.json を sushi からコピー

# (5) 動作確認
venv/bin/python3 fetch_input.py        # SpacesからCSVがDLできるか
bash run_trefac.sh                      # 一連（DL→本体）を手動で1回
```

## 2. cron 登録（furimaの谷に、箱ごとに時刻をずらす）

furima は箱ごとに1hズレ（例: inv-01=`0 1,7,13,19`）。trefac はその **約2.5h後** に置く。
flock（二重起動防止・furimaと別ロック）+ timeout（暴走上限）必須。

```cron
# 例: inv-01 (furima 1,7,13,19) → trefac 3:30,9:30,15:30,21:30
30 3,9,15,21 * * * /usr/bin/flock -n /tmp/trefac.lock /usr/bin/timeout 3h /home/fujiken/trefac_app/run_trefac.sh >> /home/fujiken/trefac_app/log/wrapper.log 2>&1
```

| 箱 | furima | trefac cron | shard_index(自動) |
|---|---|---|---|
| yahoo-inv-01 | 1,7,13,19  | `30 3,9,15,21`  | 0 |
| yahoo-inv-02 | 2,8,14,20  | `30 4,10,16,22` | 1 |
| yahoo-inv-03 | 3,9,15,21  | `30 5,11,17,23` | 2 |
| yahoo-inv-04 | 4,10,16,22 | `30 6,12,18,0`  | 3 |
| yahoo-inv-05 | 5,11,17,23 | `30 7,13,19,1`  | 4 |

shard_index はホスト名から自動算出されるので config では設定不要（全台 `auto`）。
cron時刻だけ各箱で異なる（その箱のfurima時刻を `crontab -l` で確認して+約2.5h）。

## 入力CSVの形式

Spaces の `trefac/input.csv` に置くCSVの列（ヘッダ）:

| 列名 | 必須 | 内容 |
|---|---|---|
| `仕入れURL` | ○ | trefacの商品ページURL（在庫判定に使用） |
| `eBay Item Number` | ○ | 対象eBay出品の商品番号（在庫変更対象。シャード割当のキー） |
| `account` | 推奨 | eBayアカウント識別名（現状は全行 `hiroshima`）。将来の2アカウント対応の布石 |

`account` 列は今は使わない（全行 `hiroshima`）が、付けておくと将来2アカウント化する時に
過去データの作り直しが不要になる。csv.DictReader 読みなので列が増えても既存処理は壊れない。

## 3. 切替

5台が安定稼働したら sushi の trefac cron を停止 → sushi 廃止。

## 設定の更新（運用）

- 入力リストの変更 → Spaces の `trefac/input.csv` を上げ替えるだけ。
- コードの変更 → 各箱で `git -C /home/fujiken/trefac_app pull`。

> 将来: CSV配布を共有DB（クレーム方式）へ移行すると、shard_index 管理が不要になり、
> 新しい箱は建てるだけで自動的にtrefacワーカーに参加できる。

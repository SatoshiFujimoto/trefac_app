#!/usr/bin/env bash
# trefac在庫監視の実行ラッパー。
#   1) Spacesから最新の入力CSVを取得 (fetch_input.py)
#   2) trefac本体を実行 (aki_requests.py) ※自分のshardだけ処理
#
# cron からは flock(二重起動防止) + timeout(暴走上限) 付きで呼ぶ。各箱で時刻をずらす:
#   30 3,9,15,21 * * * /usr/bin/flock -n /tmp/trefac.lock /usr/bin/timeout 3h \
#       /home/fujiken/trefac_app/run_trefac.sh >> /home/fujiken/trefac_app/log/wrapper.log 2>&1
#
# 注意: furima(my_app)とはロックファイルを共有しないこと(/tmp/trefac.lock 専用)。
set -uo pipefail

APP_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$APP_DIR" || exit 1
PY="$APP_DIR/venv/bin/python3"

echo "===== $(date '+%F %T') run_trefac start ====="

# 1) 最新CSVを取得（DL失敗でもキャッシュがあれば fetch_input.py が0で継続を選ぶ）
"$PY" fetch_input.py
fetch_rc=$?
if [ "$fetch_rc" -ne 0 ]; then
    echo "[run_trefac] 入力CSVを用意できなかったため中止 (rc=$fetch_rc)"
    echo "===== $(date '+%F %T') run_trefac aborted ====="
    exit "$fetch_rc"
fi

# 2) trefac本体
"$PY" aki_requests.py
run_rc=$?

echo "===== $(date '+%F %T') run_trefac end (rc=$run_rc) ====="
exit "$run_rc"

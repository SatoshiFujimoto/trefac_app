"""司令塔(ctrl-01)への在庫報告クライアント（方式A・設計書§11.2）。

ワーカーはeBayを直接叩かず、在庫あり/なしを ctrl-01 の `POST /stock` に報告するだけにする。
実際のeBay反映は司令塔が一括で行う（token_lock奪い合い解消・eBay認証情報をワーカーから排除）。

config.ini に [ctrl] セクションを追加して使う:
  [ctrl]
  base_url     = http://<ctrl-01の内部IP>:8000   ; nginx公開後は https://<ドメイン>
  worker_token = <CTRL_WORKER_TOKEN と同じ値>
  account      = ritz                            ; このワーカーが扱うeBayアカウント(司令塔の登録名)
  timeout      = 15
"""
import configparser
import os
import socket
import sys
import time
from logging import getLogger

import requests

logger = getLogger(__name__)

_HERE = os.path.dirname(os.path.abspath(__file__))
_CONFIG_PATH = os.path.join(_HERE, "setting", "config.ini")

_session = requests.Session()
_cfg: configparser.SectionProxy | None = None


def _conf() -> configparser.SectionProxy:
    global _cfg
    if _cfg is None:
        parser = configparser.ConfigParser()
        if not parser.read(_CONFIG_PATH, encoding="utf-8"):
            raise RuntimeError(f"config.ini が読めません: {_CONFIG_PATH}")
        if not parser.has_section("ctrl"):
            raise RuntimeError("config.ini に [ctrl] セクションがありません（base_url/worker_token）")
        _cfg = parser["ctrl"]
    return _cfg


def report_stock(item_num, in_stock: bool, account: str | None = None) -> bool:
    """在庫あり/なしを司令塔へ報告。司令塔が受理(キュー投入)できたら True。

    account 省略時は config.ini [ctrl] account を使う（ワーカーアプリ単位で固定するのが基本。
    例: trefac ワーカー → account=ritz）。どちらも無ければ送らず司令塔の既定に委ねる。
    司令塔は jobs(type='ebay_apply') に積むだけで即返るため、この呼び出しは軽量。
    実反映の成否はこの時点では分からない（司令塔側で非同期に反映・リトライ）。
    """
    conf = _conf()
    base = conf.get("base_url", "").rstrip("/")
    token = conf.get("worker_token", "")
    timeout = conf.getint("timeout", fallback=15)
    attempts = max(1, conf.getint("report_attempts", fallback=5))  # 司令塔への報告 再送回数
    # 既定5回。バックオフ 1+2+4+8=15s 粘る＝司令塔の再起動窓(~6s)をまたいで成功させる
    # （3回=3sでは再起動デプロイ中の報告が取りこぼされLINE誤通知になっていた）。
    acct = account or conf.get("account", fallback=None) or None
    if not base or not token:
        logger.error("config.ini [ctrl] の base_url / worker_token が未設定です")
        return False
    payload = {"item_num": str(item_num), "in_stock": bool(in_stock)}
    if acct:
        payload["account"] = acct
    # 司令塔への報告は一過性の失敗(ネット/タイムアウト/一時的5xx)があり得るため数回再送。
    # 全部失敗したときだけ False を返す＝呼び出し側がLINE通知する（eBayへは司令塔が後で1回反映）。
    last_exc: Exception | None = None
    for attempt in range(1, attempts + 1):
        try:
            resp = _session.post(
                f"{base}/stock",
                json=payload,
                headers={"Authorization": f"Bearer {token}"},
                timeout=timeout,
            )
            resp.raise_for_status()
            return bool(resp.json().get("ok"))
        except requests.RequestException as exc:
            last_exc = exc
            logger.warning(
                "在庫報告に失敗(%d/%d) item=%s in_stock=%s account=%s: %s",
                attempt, attempts, item_num, in_stock, acct, exc,
            )
            if attempt < attempts:
                time.sleep(min(2 ** (attempt - 1), 10))  # 指数バックオフ 1s,2s,4s…上限10s
    logger.error(
        "在庫報告に最終失敗(%d回) item=%s in_stock=%s account=%s: %s",
        attempts, item_num, in_stock, acct, last_exc,
    )
    return False


def fetch_crawl(job_type: str = "trefac") -> list:
    """司令塔(ctrl-01)から巡回対象を取得する（DB主導巡回）。

    返り値は aki_requests がそのまま使える形式:
      [{"仕入れURL": <url>, "eBay Item Number": <item_num>}, ...]
    司令塔は active かつ在庫品/Revise済を除いた対象だけを返す（売り切れたら巡回しない）。
    """
    conf = _conf()
    base = conf.get("base_url", "").rstrip("/")
    token = conf.get("worker_token", "")
    timeout = conf.getint("timeout", fallback=15)
    if not base or not token:
        raise RuntimeError("config.ini [ctrl] の base_url / worker_token が未設定です")
    resp = _session.get(
        f"{base}/crawl",
        params={"type": job_type},
        headers={"Authorization": f"Bearer {token}"},
        timeout=max(timeout, 60),  # 全件取得のため余裕を持たせる
    )
    resp.raise_for_status()
    data = resp.json()
    return [{"仕入れURL": it["url"], "eBay Item Number": it["item_num"]} for it in data.get("items", [])]


def _local_ip() -> str:
    """ベストエフォートで自機のVPC内IPを得る（取得失敗は空文字・送信はしない）。"""
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        try:
            s.connect(("10.104.0.5", 80))  # ctrl-01のVPC内IP宛て（UDPなので実送信なし）
            return s.getsockname()[0]
        finally:
            s.close()
    except OSError:
        return ""


def report_start(role: str, server_id: str | None = None) -> bool:
    """cron開始時に司令塔へ自己登録（生存報告）。fleet 表に upsert される。

    /register は upsert（行が無ければ作成・あれば last_heartbeat=now() に更新）なので、
    「開始の確認＝最終起動時刻」がそのまま fleet で見える。失敗してもワーカー本処理は止めない。
    server_id 省略時は hostname（例: yahoo-inv-01）。role は trefac/furima/hiro 等。
    """
    conf = _conf()
    base = conf.get("base_url", "").rstrip("/")
    token = conf.get("worker_token", "")
    timeout = conf.getint("timeout", fallback=15)
    if not base or not token:
        logger.error("config.ini [ctrl] の base_url / worker_token が未設定です")
        return False
    sid = server_id or f"{socket.gethostname()}/{role}"
    payload = {"server_id": sid, "role": role, "ip": _local_ip()}
    try:
        resp = _session.post(
            f"{base}/register",
            json=payload,
            headers={"Authorization": f"Bearer {token}"},
            timeout=timeout,
        )
        resp.raise_for_status()
        return bool(resp.json().get("ok"))
    except requests.RequestException as exc:
        logger.error("開始報告(/register)に失敗 server=%s role=%s: %s", sid, role, exc)
        return False


if __name__ == "__main__":
    # 起動スクリプトから `python fleet_report.py <role>` で呼ぶ用（開始の心拍）。
    _role = sys.argv[1] if len(sys.argv) > 1 else "worker"
    ok = report_start(_role)
    print(f"report_start role={_role} -> {ok}")
    sys.exit(0 if ok else 1)

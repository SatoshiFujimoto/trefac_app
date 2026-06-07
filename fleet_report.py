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
    acct = account or conf.get("account", fallback=None) or None
    if not base or not token:
        logger.error("config.ini [ctrl] の base_url / worker_token が未設定です")
        return False
    payload = {"item_num": str(item_num), "in_stock": bool(in_stock)}
    if acct:
        payload["account"] = acct
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
        logger.error("在庫報告に失敗 item=%s in_stock=%s account=%s: %s", item_num, in_stock, acct, exc)
        return False

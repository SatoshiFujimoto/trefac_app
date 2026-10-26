import csv
import glob
import configparser
import hashlib
import json
import os
import random
import re
import socket
import sys
import time
from datetime import datetime
from time import sleep
from typing import List, Dict, Optional, Union
from concurrent.futures import ThreadPoolExecutor, as_completed
import threading
from threading import Lock, Event
import requests
from logging import getLogger, config as logging_config

import sys
sys.path.insert(0, "/home/fujiken/ebay-pkg")
from ebay_pkg.trading import EbayApi

from utils.MailSender import MailSender
# 在庫報告は司令塔(ctrl-01)へ。eBayは司令塔が一括反映する（方式A・設計書§11.2）。
# 巡回対象も司令塔(items DB)から取得（DB主導巡回。売り切れたら次から巡回しない）。
from fleet_report import report_stock, fetch_crawl, report_cron_beat

# logger
logger = getLogger(__name__)

# log/aki_requests_log_config.json 設定ファイルを読み込む
# 開始日時をファイル名に含めて上書きを防ぐ
try:
    with open('log/aki_requests_log_config.json', 'r', encoding='utf-8') as f:
        log_conf = json.load(f)
    # ファイルハンドラのfilenameを「日時のみ」に差し替え（例: 013114.log）
    if 'handlers' in log_conf and 'fileHandler' in log_conf['handlers']:
        base_filename = log_conf['handlers']['fileHandler'].get('filename', 'log/aki_requests.log')
        # 拡張子とディレクトリを取得し、ファイル名は MMDDHH.log のみにする
        if '.' in base_filename:
            base_path, ext = base_filename.rsplit('.', 1)
            time_suffix = datetime.now().strftime('%m%d%H')
            if '/' in base_path:
                log_dir = base_path.rsplit('/', 1)[0]
                new_filename = f"{log_dir}/{time_suffix}.{ext}"
            else:
                new_filename = f"{time_suffix}.{ext}"
        else:
            time_suffix = datetime.now().strftime('%m%d%H')
            new_filename = f"{time_suffix}"
        log_conf['handlers']['fileHandler']['filename'] = new_filename
    logging_config.dictConfig(log_conf)
except FileNotFoundError:
    logger.error('ログ設定ファイル log/aki_requests_log_config.json が見つかりません。')
    sys.exit(1)
except json.JSONDecodeError as e:
    logger.error(f'ログ設定ファイルのJSON形式が不正です: {e}')
    sys.exit(1)
except Exception as e:
    logger.error(f'ログ設定ファイルの読み込み中にエラーが発生しました: {e}')
    sys.exit(1)


# 使用するクラスのインスタンス化、パスワードの読み込み
# config.ini 設定ファイルを読み込む
app_config = configparser.ConfigParser()
try:
    read_files = app_config.read('setting/config.ini')
    if not read_files:
        logger.error('設定ファイル setting/config.ini が見つかりません。')
        sys.exit(1)
except Exception as e:
    logger.error(f'設定ファイルの読み込み中にエラーが発生しました: {e}')
    sys.exit(1)

# ===== 認証情報の読み込み =====
try:
    #ebay
    ritz_refresh_token = app_config['Credentials']['ritz_refresh_token']
    ebay_basic_auth = app_config['Credentials']['ebay_basic_auth']

    #line
    line_access_token = app_config['Credentials']['line_messaging_api']
    line_user_id = app_config['Settings']['line_user_id']

    #mail
    gmail_app_pass = app_config['Credentials']['gmail_app_pass']
    mail_sender = MailSender(gmail_app_pass)
except KeyError as e:
    logger.error(f'設定ファイルに必要なキーがありません: {e}')
    sys.exit(1)
except Exception as e:
    logger.error(f'認証情報の読み込み中にエラーが発生しました: {e}')
    sys.exit(1)

# ===== アプリケーション設定の読み込み =====
def derive_shard_index_from_hostname() -> int:
    """shard_index='auto' のとき、ホスト名末尾の数字から番号を算出する。

    例: yahoo-inv-01 -> 0, yahoo-inv-05 -> 4 （末尾の数 - 1）。
    全台で config.ini を完全に同一にでき、台ごとの手編集（shard_index書き換え）が不要になる。
    末尾に数字が無いホスト名なら起動を中止する（誤った担当でチェック漏れ/重複を防ぐ）。
    """
    hostname = socket.gethostname()
    m = re.search(r'(\d+)\s*$', hostname)
    if not m:
        logger.error(f'shard_index=auto ですが、ホスト名 "{hostname}" の末尾に番号がありません。')
        sys.exit(1)
    index = int(m.group(1)) - 1  # 01始まり -> 0始まり
    logger.info(f'shard_index=auto: ホスト名 "{hostname}" から shard_index={index} を算出しました。')
    return index


# fleet.json: 全サーバー台数(shard_count)を一元管理する設定ファイル。
# run_trefac.sh -> fetch_input.py が Spaces からDLしてこの場所に置く。
# 台数を変える時は Spaces の fleet.json を1か所直すだけで、全台が次回runで追従する。
FLEET_FILE = os.path.join(os.path.dirname(os.path.abspath(__file__)), 'fleet.json')


def derive_shard_count_from_fleet() -> int:
    """shard_count='auto' のとき、fleet.json の shard_count を読む。

    全台で台数を一元管理するための仕組み（増設時は fleet.json を1か所直すだけ）。
    読めない/不正な場合は、誤った分割（チェック漏れ・重複）を避けるため起動を中止する。
    """
    try:
        with open(FLEET_FILE, 'r', encoding='utf-8') as f:
            count = int(json.load(f)['shard_count'])
    except (FileNotFoundError, json.JSONDecodeError, KeyError, ValueError, TypeError, OSError) as e:
        logger.error(
            f'shard_count=auto ですが fleet.json を読めません（{FLEET_FILE}）: {e}。'
            '誤分割防止のため中止します。'
        )
        sys.exit(1)
    if count < 1:
        logger.error(f'fleet.json の shard_count={count} は不正です（1以上必須）。中止します。')
        sys.exit(1)
    logger.info(f'shard_count=auto: fleet.json から shard_count={count} を取得しました。')
    return count


class AppSettings:
    """アプリケーション設定を保持するクラス"""
    def __init__(self, config: configparser.ConfigParser):
        try:
            settings = config['Settings']
            # requests設定（User-Agentのリストを読み込む）
            user_agents_str = settings.get('user_agents', settings.get('user_agent', ''))
            # カンマ区切りで分割してリスト化
            self.user_agents_list = [ua.strip() for ua in user_agents_str.split(',') if ua.strip()]
            if not self.user_agents_list:
                # フォールバック: デフォルトのUser-Agent
                self.user_agents_list = ['Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36']
            # 後方互換性のため、最初のUser-Agentをデフォルトとして保持
            self.user_agent_str = self.user_agents_list[0]
            self.user_agent = {'User-Agent': self.user_agent_str}

            # 在庫チェック設定
            self.judgment_word = settings['judgment_word']
            self.request_timeout = int(settings['request_timeout'])
            self.max_retries = int(settings['max_retries'])
            self.check_interval = int(settings['check_interval'])
            self.retry_check_interval = int(settings['retry_check_interval'])
            self.token_refresh_interval = int(settings['token_refresh_interval'])
            self.min_response_length = int(settings['min_response_length'])

            # 各リクエスト送信前のランダム待機時間（秒）の範囲
            # アクセス頻度を下げてブロックを回避するために使用
            self.request_interval_min = float(settings.get('request_interval_min', '1.0'))
            self.request_interval_max = float(settings.get('request_interval_max', '3.0'))
            if self.request_interval_min > self.request_interval_max:
                logger.warning('request_interval_minがmaxより大きいため、値を入れ替えます。')
                self.request_interval_min, self.request_interval_max = (
                    self.request_interval_max, self.request_interval_min
                )

            # 429切り分けの試験用（2026-10-26: monitor-01〜06が連続45分で必ず429になる件）。
            # 既定0=従来どおり（休憩なし・429で即停止）。試験する台の config.ini だけで有効にする。
            #   work_minutes/rest_minutes: 連続work分走ったらrest分休む（45分の前に休めばリセットされるか）
            #   block_probes/block_probe_wait_min: 429後に wait分待って1件試す、をprobes回まで（何分で解けるか）
            self.work_minutes = float(settings.get('work_minutes', '0'))
            self.rest_minutes = float(settings.get('rest_minutes', '0'))
            self.block_probes = int(settings.get('block_probes', '0'))
            self.block_probe_wait_min = float(settings.get('block_probe_wait_min', '10'))
            self.block_probe_first_wait_min = float(
                settings.get('block_probe_first_wait_min', str(self.block_probe_wait_min)))
            # 出口IPの交代（2026-10-26: trefacはIPごとに連続約45分で429）。1台に複数IPを持たせ、
            # ip_switch_minutes ごと／429が来た時に次のIPへ切り替える。未設定なら従来どおり既定の出口。
            # trefacへのアクセスだけに使う（司令塔への報告・メルカリ/フリマは既定の出口のまま）。
            self.source_ips = [s.strip() for s in settings.get('source_ips', '').split(',') if s.strip()]
            self.ip_switch_minutes = float(settings.get('ip_switch_minutes', '0'))
            # 臨時の追いつき巡回用: 司令塔の /crawl の代わりにこのJSON([{url,item_num},...])を回す。
            # 未設定なら従来どおり /crawl（2026-10-26 長期未巡回の約3,300件を臨時サーバーで一掃した時に追加）
            self.crawl_list_file = settings.get('crawl_list_file', '').strip()

            # ファイル設定
            self.input_file_pattern = settings['input_file_pattern']
            self.output_file_name = settings['output_file_name']
            self.mail_subject = settings['mail_subject']

            # 並列処理設定
            self.max_workers = int(settings.get('max_workers', '1'))
            if self.max_workers < 1:
                logger.warning('max_workersが1未満のため、1に設定します。')
                self.max_workers = 1

            # シャーディング設定（複数サーバーで担当を分割してアクセスを分散）
            # shard_count: 全サーバー台数, shard_index: このサーバーの番号(0始まり)
            # 各商品は eBay Item Number から計算で担当サーバーが一意に決まる
            raw_count = str(settings.get('shard_count', '1')).strip()
            if raw_count.lower() == 'auto':
                # fleet.json から取得（増設時は fleet.json を1か所直すだけで全台追従）
                self.shard_count = derive_shard_count_from_fleet()
            else:
                self.shard_count = int(raw_count)
            raw_index = str(settings.get('shard_index', '0')).strip()
            if raw_index.lower() == 'auto':
                # ホスト名から自動算出（例 yahoo-inv-01 -> 0）。全台で config.ini を同一にできる
                self.shard_index = derive_shard_index_from_hostname()
            else:
                self.shard_index = int(raw_index)
            if self.shard_count < 1:
                logger.warning('shard_countが1未満のため、1に設定します。')
                self.shard_count = 1
            if not (0 <= self.shard_index < self.shard_count):
                logger.error(
                    f'shard_indexは0以上shard_count未満にしてください: '
                    f'shard_index={self.shard_index}, shard_count={self.shard_count}'
                )
                sys.exit(1)

            # メール件名のプレースホルダを展開（全台で config.ini を同一にできる）
            #   {shard} -> このサーバーの番号(1始まり, 人が読む用)
            #   {total} -> 全サーバー台数
            #   {host}  -> ホスト名
            # 例: "Sushi {shard}/{total}の結果" -> inv-01 で "Sushi 1/5の結果"
            self.mail_subject = (
                self.mail_subject
                .replace('{shard}', str(self.shard_index + 1))
                .replace('{total}', str(self.shard_count))
                .replace('{host}', socket.gethostname())
            )
        except KeyError as e:
            logger.error(f'設定ファイルに必要なキーがありません: {e}')
            sys.exit(1)
        except ValueError as e:
            logger.error(f'設定ファイルの値が不正です（数値が必要です）: {e}')
            sys.exit(1)
        except Exception as e:
            logger.error(f'アプリケーション設定の読み込み中にエラーが発生しました: {e}')
            sys.exit(1)

# 設定インスタンスの作成
app_settings = AppSettings(app_config)

# 後方互換性のため、グローバル変数も保持（既存コードとの互換性）
user_agent = app_settings.user_agent
judgment_word = app_settings.judgment_word
request_timeout = app_settings.request_timeout
max_retries = app_settings.max_retries
check_interval = app_settings.check_interval
retry_check_interval = app_settings.retry_check_interval
token_refresh_interval = app_settings.token_refresh_interval
min_response_length = app_settings.min_response_length
input_file_pattern = app_settings.input_file_pattern
output_file_name = app_settings.output_file_name
mail_subject = app_settings.mail_subject
max_workers = app_settings.max_workers

# ブロック検知時に全ワーカースレッドを即座に停止させるための共有フラグ
# 403/429（ブロック）を検知したワーカーがこれをセットすると、
# 他のワーカーはリクエスト送信前にスキップして処理を打ち切る
stop_event = Event()

# 休憩/429後の再試行（試験用・既定は無効）。_pace_lock を握って sleep する間、
# もう一方のスレッドも次の商品の前で _pace_lock 待ちになる＝全体が止まって休める。
_pace_lock = Lock()
_run_started = time.monotonic()
_work_started = _run_started
_last_recover = 0.0


def _elapsed_min(since: float | None = None) -> float:
    return (time.monotonic() - (since if since is not None else _run_started)) / 60


def _maybe_rest() -> None:
    """連続 work_minutes 走ったら rest_minutes 休む（無効なら何もしない）。"""
    global _work_started
    if app_settings.work_minutes <= 0 or app_settings.rest_minutes <= 0:
        return
    with _pace_lock:
        if stop_event.is_set() or _elapsed_min(_work_started) < app_settings.work_minutes:
            return
        logger.info(f'[試験] 連続{_elapsed_min(_work_started):.1f}分走ったので{app_settings.rest_minutes:g}分休みます'
                    f'（開始から{_elapsed_min():.1f}分）')
        sleep(app_settings.rest_minutes * 60)
        _work_started = time.monotonic()
        logger.info(f'[試験] 休憩明け・再開します（開始から{_elapsed_min():.1f}分）')


class _SourceIPAdapter(requests.adapters.HTTPAdapter):
    """出口(送信元)IPを固定するアダプタ。"""

    def __init__(self, source_ip: str, **kwargs):
        self._source_ip = source_ip
        super().__init__(**kwargs)

    def init_poolmanager(self, *args, **kwargs):
        kwargs['source_address'] = (self._source_ip, 0)
        return super().init_poolmanager(*args, **kwargs)


_ip_lock = Lock()
_ip_index = 0
_ip_started = time.monotonic()


def _current_ip() -> str | None:
    """今使う出口IP。ip_switch_minutes を超えたら次のIPへ（最後のIPに来たらそのまま）。"""
    global _ip_index, _ip_started
    ips = app_settings.source_ips
    if not ips:
        return None
    with _ip_lock:
        if (app_settings.ip_switch_minutes > 0 and _ip_index < len(ips) - 1
                and _elapsed_min(_ip_started) >= app_settings.ip_switch_minutes):
            _ip_index += 1
            _ip_started = time.monotonic()
            logger.info(f'[IP交代] {app_settings.ip_switch_minutes:g}分たったので出口IPを {ips[_ip_index]} に切り替えます'
                        f'（開始から{_elapsed_min():.1f}分）')
        return ips[_ip_index]


def _switch_ip_on_block(blocked_ip: str | None) -> bool:
    """429が来たら、まだ使っていない次のIPへ切り替える。切り替えられたら True。"""
    global _ip_index, _ip_started
    ips = app_settings.source_ips
    with _ip_lock:
        if blocked_ip is not None and ips and ips[_ip_index] != blocked_ip:
            return True  # 他スレッドが既に切り替え済み
        if not ips or _ip_index >= len(ips) - 1:
            return False
        _ip_index += 1
        _ip_started = time.monotonic()
        logger.warning(f'[IP交代] {blocked_ip} で429 → 出口IPを {ips[_ip_index]} に切り替えて続けます'
                       f'（開始から{_elapsed_min():.1f}分）')
        return True


_thread_ip = threading.local()  # このスレッドが直前に使った出口IP（429の切り替え判定用）


def _http_get(url: str, headers: dict, timeout):
    """trefacへのGET。source_ips 設定時はその出口IPから出す（Cookieは持ち越さない＝従来と同じ）。"""
    ip = _current_ip()
    _thread_ip.ip = ip
    if ip is None:
        return requests.get(url, headers=headers, timeout=timeout, allow_redirects=True)
    with requests.Session() as s:
        adapter = _SourceIPAdapter(ip)
        s.mount('http://', adapter)
        s.mount('https://', adapter)
        return s.get(url, headers=headers, timeout=timeout, allow_redirects=True)


def _probe_after_block(url: str, judgment_word: str):
    """429/403 の後、block_probe_wait_min 待って同じURLを1件だけ試す。最大 block_probes 回。

    回復したら結果をそのまま返して巡回を続ける。全部だめなら "blocked"（従来どおり停止）。
    """
    global _work_started, _last_recover
    with _pace_lock:
        if stop_event.is_set():
            return "blocked"
        # 他スレッドが待って回復を確認した直後なら、待たずに1回だけ再試行
        if time.monotonic() - _last_recover < 120:
            return send_request(url, judgment_word)
        waited = 0.0
        for i in range(1, app_settings.block_probes + 1):
            # 1回目だけ長く待てる（締め出しは少なくとも35分続くと分かっているため）
            wait = app_settings.block_probe_first_wait_min if i == 1 else app_settings.block_probe_wait_min
            logger.warning(f'[試験] 429後 {wait:g}分待って1件だけ試します'
                           f'（{i}/{app_settings.block_probes}回目・開始から{_elapsed_min():.1f}分）: {url}')
            sleep(wait * 60)
            waited += wait
            result = send_request(url, judgment_word)
            if result != "blocked":
                logger.warning(f'[試験] 回復: 429から{waited:g}分で通りました'
                               f'（開始から{_elapsed_min():.1f}分）→ 巡回を再開します')
                _last_recover = _work_started = time.monotonic()
                return result
            logger.warning(f'[試験] まだ429: 429から{waited:g}分経過')
        return "blocked"


def csv_to_dict(file_path: str) -> List[Dict[str, str]]:
    """
    CSVファイルを読み込んで辞書のリストに変換する

    Args:
        file_path: 読み込むCSVファイルのパス

    Returns:
        CSVファイルの各行を辞書に変換したリスト

    Raises:
        FileNotFoundError: ファイルが存在しない場合
        csv.Error: CSV形式が不正な場合
    """
    result_dict = []
    try:
        with open(file_path, 'r', encoding='utf-8') as csv_file:
            csv_reader = csv.DictReader(csv_file)
            for row in csv_reader:
                result_dict.append(dict(row))
        return result_dict
    except FileNotFoundError:
        logger.error(f'CSVファイルが見つかりません: {file_path}')
        raise
    except csv.Error as e:
        logger.error(f'CSVファイルの読み込みエラー: {file_path}, エラー: {e}')
        raise
    except Exception as e:
        logger.error(f'CSVファイルの読み込み中に予期しないエラーが発生しました: {file_path}, エラー: {e}')
        raise


def write_list(item_data: List[Union[str, int]], file_path: str) -> None:
    """
    リストの各要素を1行ずつファイルに書き込む

    Args:
        item_data: 書き込むデータのリスト
        file_path: 出力先ファイルパス

    Raises:
        IOError: ファイルの書き込みに失敗した場合
    """
    try:
        with open(file_path, 'w', encoding='utf-8') as f:
            for item in item_data:
                f.write(f"{item}\n")
    except IOError as e:
        logger.error(f'ファイルの書き込みに失敗しました: {file_path}, エラー: {e}')
        raise

def get_access_token(refresh_token: str) -> str:
    """
    eBay APIのアクセストークンを取得する

    Args:
        refresh_token: リフレッシュトークン

    Returns:
        アクセストークン文字列

    Raises:
        requests.RequestException: APIリクエストが失敗した場合
        KeyError: レスポンスにaccess_tokenが含まれていない場合
    """
    url = 'https://api.ebay.com/identity/v1/oauth2/token'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': ebay_basic_auth
    }
    data = (
        f'grant_type=refresh_token&'
        f'refresh_token={refresh_token}&'
        f'scope=https%3A%2F%2Fapi.ebay.com%2Foauth%2Fapi_scope'
    )

    try:
        response = requests.post(url, headers=headers, data=data)
        response.raise_for_status()  # HTTPエラー時に例外を出す
        token_json = response.json()
        if 'access_token' not in token_json:
            raise KeyError('レスポンスにaccess_tokenが含まれていません')
        return token_json["access_token"]
    except requests.RequestException as e:
        logger.error(f'eBay APIトークン取得エラー: {e}')
        raise

def line_notify(message: str) -> None:
    """
    LINE Messaging APIを使用してメッセージを送信

    Args:
        message: 送信するメッセージ内容
    """
    access_token = line_access_token
    headers = {
        'Content-Type': 'application/json',
        'Authorization': f'Bearer {line_access_token}',
    }
    data = {
        'to': line_user_id,
        'messages': [
            {
                'type': 'text',
                'text': message,
            },
        ]
    }
    response = requests.post('https://api.line.me/v2/bot/message/push', headers=headers, json=data)
    # 結果出力
    logger.info(f'line通知結果：{response.status_code}')

# 仕入元に繋がらず在庫を判定できなかった結果（send_request の戻り値）→ 表示名。
# これらは在庫0にしない（DNS障害等で大量の偽の在庫0を出さない・2026-10-02の事故対策）。
COMM_ERRORS = {
    "timeout": "タイムアウト",
    "connection_error": "接続エラー",
    "server_error": "サーバーエラー",
    "short_response": "レスポンス異常",
}
# 巡回の最後に繋がらなかった分を再チェックする前の待ち時間（一過性の瞬断が明けるのを待つ）。
RETRY_WAIT_SEC = 60


def send_request(
    request_url: str,
    judgment_word: Optional[str] = None,
    timeout: Optional[int] = None,
    max_retries: Optional[int] = None
) -> Union[bool, str]:
    """
    指定URLにリクエストを送信し、指定文字列が含まれているか判定

    Args:
        request_url: リクエスト先URL
        judgment_word: 判定文字列（Noneの場合は設定ファイルの値を使用）
        timeout: タイムアウト秒数（Noneの場合は設定ファイルの値を使用）
        max_retries: 最大リトライ回数（Noneの場合は設定ファイルの値を使用）

    Returns:
        bool or str: 文字列が含まれている場合True、在庫なしの場合False、
                     タイムアウトの場合"timeout"、接続エラーの場合"connection_error"、
                     サーバーエラーでリトライ上限の場合"server_error"、
                     レスポンスが短くてリトライ上限の場合"short_response"、
                     403/429エラー（ブロック検出）の場合"blocked"
    """
    # デフォルト値の設定（設定ファイルから読み込んだ値を使用）
    if judgment_word is None:
        judgment_word = app_settings.judgment_word
    if timeout is None:
        timeout = app_settings.request_timeout
    if max_retries is None:
        max_retries = app_settings.max_retries

    # URL検証
    if not request_url or not isinstance(request_url, str):
        logger.error(f"無効なURL: {request_url}")
        return False

    if not request_url.startswith(('http://', 'https://')):
        logger.error(f"無効なURL形式: {request_url}")
        return False

    # リトライループ
    for attempt in range(max_retries):
        try:
            # User-Agentをランダムに選択
            selected_user_agent = random.choice(app_settings.user_agents_list)
            headers = {'User-Agent': selected_user_agent}

            res = _http_get(request_url, headers, timeout)

            # エンコーディング処理
            # requestsが自動推測したエンコーディングを使用し、エラー時はUTF-8でフォールバック
            if res.encoding is None or res.encoding.lower() in ('iso-8859-1', 'ascii'):
                # エンコーディングが推測できない、またはデフォルト値の場合はUTF-8を試す
                res.encoding = 'utf-8'

            try:
                response_text = res.text
            except UnicodeDecodeError:
                # デコードエラー時はUTF-8で強制的にデコード（エラーを置換）
                response_text = res.content.decode('utf-8', errors='replace')
                logger.warning(f"エンコーディングエラーを検出、UTF-8で強制デコード: {request_url}")

            # ステータスコードチェック（200番台を成功とみなす）
            if 200 <= res.status_code < 300:
                # レスポンスが空または短すぎる場合のチェック
                response_length = len(response_text)
                min_length = app_settings.min_response_length
                if response_length < min_length:
                    logger.warning(
                        f"レスポンスが異常に短い: {request_url}\n"
                        f"  レスポンス長: {response_length}文字\n"
                        f"  ステータスコード: {res.status_code}\n"
                        f"  レスポンス内容: {response_text[:200]}"
                    )
                    # 短すぎる場合はリトライを試みる
                    if attempt < max_retries - 1:
                        wait_time = 2 ** attempt
                        logger.info(f"リトライ待機中 ({wait_time}秒): {request_url}")
                        sleep(wait_time)
                        continue
                    else:
                        logger.error(f"レスポンスが異常に短い: リトライ上限に達しました: {request_url}")
                        return "short_response"

                # 判定文字列の検索
                if judgment_word in response_text:
                    return True
                else:
                    # 在庫なしと判定した場合、詳細情報をログに記録
                    response_preview = response_text[:500] if response_length > 0 else "(空のレスポンス)"
                    # 判定文字列の類似文字列を探す（デバッグ用）
                    similar_words = []
                    if 'カート' in response_text:
                        similar_words.append("'カート'という文字列は存在します")
                    if '入れる' in response_text:
                        similar_words.append("'入れる'という文字列は存在します")

                    logger.warning(
                        f"在庫なしと判定: {request_url}\n"
                        f"  レスポンス長: {response_length}文字\n"
                        f"  エンコーディング: {res.encoding}\n"
                        f"  ステータスコード: {res.status_code}\n"
                        f"  レスポンス先頭500文字: {response_preview}\n"
                        f"  判定文字列 '{judgment_word}' が見つかりませんでした"
                        + (f"\n  補足: {', '.join(similar_words)}" if similar_words else "")
                    )
                    return False
            else:
                # ステータスコードが200番台以外の場合
                status_msg = {
                    400: "Bad Request",
                    401: "Unauthorized",
                    403: "Forbidden",
                    404: "Not Found",
                    429: "Too Many Requests",
                    500: "Internal Server Error",
                    502: "Bad Gateway",
                    503: "Service Unavailable",
                    504: "Gateway Timeout"
                }.get(res.status_code, "Unknown Error")

                logger.warning(f"ステータスコード {res.status_code} ({status_msg}): {request_url}")

                # 403（Forbidden）や429（Too Many Requests）エラーは即座に停止
                if res.status_code in (403, 429):
                    logger.error(
                        f"ブロックの兆候を検出: ステータスコード {res.status_code} ({status_msg})"
                        f"（開始から{_elapsed_min():.1f}分・出口IP {getattr(_thread_ip, 'ip', None) or '既定'}）: {request_url}"
                    )
                    # 制限の仕組みを切り分けるため、応答ヘッダーと本文の先頭を残す（2026-10-26）
                    logger.error(f"  429応答ヘッダー: {dict(res.headers)}")
                    logger.error(f"  429応答本文(先頭500字): {' '.join(response_text[:500].split())}")
                    return "blocked"

                # 4xx系のクライアントエラーはリトライしない
                if 400 <= res.status_code < 500:
                    return False

                # 5xx系のサーバーエラーはリトライする
                if attempt < max_retries - 1:
                    wait_time = 2 ** attempt  # 指数バックオフ
                    logger.info(f"リトライ待機中 ({wait_time}秒): {request_url}")
                    sleep(wait_time)
                    continue
                else:
                    logger.error(f"サーバーエラー: リトライ上限に達しました: {request_url} (ステータスコード: {res.status_code})")
                    return "server_error"

        except requests.Timeout:
            logger.warning(f"タイムアウト (試行 {attempt + 1}/{max_retries}): {request_url}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt  # 指数バックオフ
                logger.info(f"リトライ待機中 ({wait_time}秒): {request_url}")
                sleep(wait_time)
            else:
                logger.error(f"タイムアウト: リトライ上限に達しました: {request_url}")
                return "timeout"

        except requests.ConnectionError as e:
            logger.warning(f"接続エラー (試行 {attempt + 1}/{max_retries}): {request_url}, エラー: {e}")
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logger.info(f"リトライ待機中 ({wait_time}秒): {request_url}")
                sleep(wait_time)
            else:
                logger.error(f"接続エラー: リトライ上限に達しました: {request_url}")
                return "connection_error"

        except requests.RequestException as e:
            logger.error(f"リクエストエラー: {request_url}, エラー: {e}")
            return False

        except Exception as e:
            logger.error(f"予期しないエラー: {request_url}, エラー: {type(e).__name__}: {e}")
            return False

    return False


def retry_failed_requests(
    failed_items: List[Dict[str, str]],
    judgment_word: Optional[str] = None,
    timeout: Optional[int] = None
) -> Dict[str, Union[int, List[str]]]:
    """
    タイムアウトや接続エラーで失敗したアイテムを再チェックし、在庫があれば在庫を1に戻す
    （eBay反映は司令塔。ここでは report_stock(item_num, True) を司令塔へ送るだけ）

    Args:
        failed_items: 失敗したアイテムのリスト [{"url": str, "item_num": str}, ...]
        judgment_word: 判定文字列（Noneの場合は設定ファイルの値を使用）
        timeout: タイムアウト秒数（Noneの場合は設定ファイルの値を使用）

    Returns:
        dict: {"restored": int, "still_failed": int, "failed_items": list, "restored_items": list} 復元した数、まだ失敗している数、失敗したアイテム番号のリスト、復元したアイテム番号のリスト
    """
    # デフォルト値の設定（設定ファイルから読み込んだ値を使用）
    if judgment_word is None:
        judgment_word = app_settings.judgment_word
    if timeout is None:
        timeout = app_settings.request_timeout

    logger.info(f"=====失敗したアイテムの再チェック開始: {len(failed_items)}件"
                f"（{RETRY_WAIT_SEC}秒待ってから）=====")
    sleep(RETRY_WAIT_SEC)  # 一過性の障害(DNS/回線の瞬断)が明けるのを少し待つ
    restored_count = 0
    still_failed_count = 0
    restored_items = []
    failed_items_list = []  # 再チェックでも繋がらず「判定不能」を報告したアイテム
    no_stock_items = []     # 再チェックで正常に「在庫なし」と判定できたアイテム

    for idx, item in enumerate(failed_items, 1):
        url = item["url"]
        item_num = item["item_num"]
        logger.info(f"再チェック {idx}/{len(failed_items)}: {url}")

        is_stock = send_request(url, judgment_word, timeout=timeout, max_retries=2)

        if is_stock == "blocked":
            # ブロックの兆候＝これ以上 trefac へアクセスしない。残りは報告せず次回の巡回へ持ち越し。
            stop_event.set()
            logger.error(f"再チェック中にブロック検出: 残り{len(failed_items) - idx + 1}件は次回へ持ち越し")
            break
        elif is_stock is True:
            # 繋がって在庫あり＝在庫は変えない。保留中(recheck)だった商品だけ解除/復旧を報告する
            # （保留していない商品に「在庫あり」を送ると無駄なRevise(在庫1→1)が積まれるため）。
            logger.info(f"再チェックで在庫ありを確認: {url} - {item_num}")
            if item.get("recheck"):
                if report_stock(item_num, True):
                    restored_count += 1
                    restored_items.append(item_num)
                else:
                    logger.warning(f"在庫ありの報告に失敗しました: {item_num}")
        elif is_stock in COMM_ERRORS:
            # それでも繋がらない＝「判定不能」を報告。司令塔が保留し、猶予を超えて続けば安全のため0にする。
            still_failed_count += 1
            failed_items_list.append(item_num)
            ok = report_stock(item_num, False, reason="comm_error")
            logger.warning(f"再チェックでも失敗 ({COMM_ERRORS[is_stock]}): {url} - 判定不能を司令塔へ"
                           f"{'報告(保留)' if ok else '報告できず'}")
        else:
            # 正常に取得できて在庫なし＝本当の売り切れ → 在庫0を報告
            if report_stock(item_num, False):
                no_stock_items.append(item_num)
                logger.info(f"再チェックで在庫なし 在庫0を司令塔へ報告: {url}\nhttps://www.ebay.com/itm/{item_num}")
            else:
                no_stock_items.append(f" Error 在庫なし 司令塔への報告に失敗しました。\n{url}\nhttps://www.ebay.com/itm/{item_num}")
                logger.error(f"再チェックで在庫なし: 司令塔への報告に失敗: {item_num}")

        # サーバー負荷を考慮して少し待機
        sleep(app_settings.retry_check_interval)

    logger.info(f"=====再チェック完了: 保留解除={restored_count}件, 在庫なし={len(no_stock_items)}件, "
                f"判定不能(保留)={still_failed_count}件=====")
    if restored_items:
        logger.info(f"保留解除したアイテム番号: {restored_items}")

    return {"restored": restored_count, "still_failed": still_failed_count, "failed_items": failed_items_list,
            "restored_items": restored_items, "no_stock_items": no_stock_items}


def check_single_item(
    item: Dict[str, str],
    index: int,
    total: int,
    judgment_word: str,
    result_lock: Lock
) -> Dict[str, Union[str, bool, int]]:
    """
    1つの商品の在庫状況をチェックする（並列処理用）

    eBayは司令塔(ctrl-01)が一括反映するため、ここではトークン管理もeBay直叩きもしない。
    在庫変化があれば report_stock(item_num, in_stock) で司令塔へ報告するだけ。

    Args:
        item: 商品情報の辞書
        index: 商品のインデックス（1から始まる）
        total: 全商品数
        judgment_word: 判定文字列
        result_lock: 結果リスト/ログへのアクセスを保護するロック

    Returns:
        チェック結果の辞書
    """
    url = item["仕入れURL"]
    item_num = item["eBay Item Number"]

    # 既にブロックを検知済みの場合は、リクエストを送らず即座にスキップする
    # （他のワーカーが403/429を撃ち続けてブロックを悪化させないため）
    if stop_event.is_set():
        return {
            "index": index,
            "total": total,
            "url": url,
            "item_num": item_num,
            "is_stock": "skipped",
            "no_stock_item": None,
            "failed_request": None,
            "is_blocked": False,
        }

    # （トークン更新・eBay直叩きは司令塔へ移管したため、ここでは不要）

    # 連続稼働が work_minutes を超えたら休む（試験用・既定は無効）
    _maybe_rest()

    # サーバー負荷を考慮してリクエスト送信前にランダムな待機時間
    # （設定ファイルの request_interval_min〜max の範囲。頻度を下げてブロック回避）
    wait_time = random.uniform(
        app_settings.request_interval_min, app_settings.request_interval_max
    )
    sleep(wait_time)

    # 待機中に他スレッドがブロックを検知していたら、ここでも送信せず打ち切る
    if stop_event.is_set():
        return {
            "index": index,
            "total": total,
            "url": url,
            "item_num": item_num,
            "is_stock": "skipped",
            "no_stock_item": None,
            "failed_request": None,
            "is_blocked": False,
        }

    # 在庫チェック
    is_stock = send_request(url, judgment_word)

    # 429後に待って再試行する（試験用・既定は無効＝従来どおり即停止）
    # 429なら、まだ使っていない出口IPへ切り替えて同じ商品をやり直す（source_ips 設定時のみ）
    if is_stock == "blocked" and _switch_ip_on_block(getattr(_thread_ip, 'ip', None)):
        is_stock = send_request(url, judgment_word)

    if is_stock == "blocked" and app_settings.block_probes > 0:
        is_stock = _probe_after_block(url, judgment_word)

    # 403/429エラー（ブロック検出）の場合、特別なフラグを設定
    is_blocked = (is_stock == "blocked")
    if is_blocked:
        # 全ワーカーに停止を通知（以降のアイテムはリクエストせずスキップされる）
        stop_event.set()

    result = {
        "index": index,
        "total": total,
        "url": url,
        "item_num": item_num,
        "is_stock": is_stock,
        "no_stock_item": None,
        "failed_request": None,
        "is_blocked": is_blocked
    }

    # ブロック検出の場合は即座に返す（main関数で処理を停止）
    if is_blocked:
        return result

    if is_stock == "blocked":
        # ブロック検出の場合は即座に返す（main関数で処理を停止）
        with result_lock:
            logger.error(f"{index}/{total} ブロック検出: {url}")
        return result
    elif is_stock is True:
        # 在庫ありの場合（判定文字列がページに含まれている）
        # 通常は在庫1のままなので更新不要。通信エラー保留中(recheck)の商品だけは
        # 「在庫あり」を報告して保留を解除する（猶予切れで0にしていた場合はここで1に戻る）。
        if item.get("recheck"):
            ok = report_stock(item_num, True)
            with result_lock:
                logger.info(f"{index}/{total} OK(保留解除{'を報告' if ok else 'の報告に失敗'}): {url}")
        else:
            with result_lock:
                logger.info(f"{index}/{total} OK: {url}")
    elif is_stock in COMM_ERRORS:
        # 仕入元に繋がらず判定不能（DNS障害/接続/タイムアウト/5xx/応答異常）。
        # ここでは在庫0にしない（障害時に大量の偽の在庫0を出さない）。巡回の最後に再チェックし、
        # それでも繋がらなければ司令塔へ「判定不能」を報告→司令塔が保留・猶予超えで安全のため0。
        with result_lock:
            logger.warning(f"{index}/{total} リクエスト失敗 ({COMM_ERRORS[is_stock]}): {url} - 在庫は変えず最後に再チェックします")
        result["failed_request"] = {"url": url, "item_num": item_num, "recheck": bool(item.get("recheck"))}
    else:
        # 在庫なしの場合（正常にページ取得できたが在庫なし）→ 司令塔へ報告（→eBayで0）
        ok = report_stock(item_num, False)
        if ok:
            message = f"{index}/{total} NOT INV 在庫0を司令塔へ報告。\n{url}\nhttps://www.ebay.com/itm/{item_num}"
            with result_lock:
                logger.info(message)
            result["no_stock_item"] = item_num
        else:
            message = f" Error 在庫なし 司令塔への報告に失敗しました。\n{url}\nhttps://www.ebay.com/itm/{item_num}"
            with result_lock:
                logger.error(message)
                line_notify(message)
            result["no_stock_item"] = message

    return result


def filter_items_for_shard(
    items: List[Dict[str, str]],
    shard_index: int,
    shard_count: int
) -> List[Dict[str, str]]:
    """
    複数サーバーで担当を分割するため、このサーバーが担当する商品だけを抽出する

    各商品の eBay Item Number（無ければ仕入れURL、それも無ければ行番号）を
    md5ハッシュ化し、shard_count で割った余りが shard_index と一致する商品だけを返す。
    計算はサーバー間で同一の結果になるため、通信なしで重複・抜けなく分割できる。

    Args:
        items: 全商品のリスト
        shard_index: このサーバーの番号（0始まり）
        shard_count: 全サーバー台数

    Returns:
        このサーバーが担当する商品のリスト
    """
    if shard_count <= 1:
        return items

    selected = []
    for idx, item in enumerate(items):
        # 担当を決めるキー（商品番号を最優先。空ならURL、それも空なら行番号）
        key = str(item.get("eBay Item Number") or item.get("仕入れURL") or idx)
        digest = hashlib.md5(key.encode("utf-8")).hexdigest()
        if int(digest, 16) % shard_count == shard_index:
            selected.append(item)
    return selected


def main() -> None:
    """
    メイン処理: CSVファイルから商品リストを読み込み、在庫状況をチェックしてeBayの在庫を更新する
    """
    logger.info('=====main_program is start=====')
    no_stock_list = []
    failed_requests_list = []  # タイムアウトや接続エラーで失敗したアイテムを保存
    blocked = False  # ブロック検出で中断したか（中断時もここまでの部分結果を保存する）
    stopped_at = 0  # ブロック中断した時点の完了件数（司令塔への完走報告に載せる）

    # 巡回対象は司令塔(ctrl-01のitems DB)から取得（CSV廃止・DB主導巡回）。
    # 司令塔が active かつ在庫品/Revise済を除いた対象だけを返す＝売り切れたら次から巡回しない。
    try:
        if app_settings.crawl_list_file:
            with open(app_settings.crawl_list_file, encoding='utf-8') as f:
                search_items = [{"仕入れURL": it["url"], "eBay Item Number": it["item_num"], "recheck": False}
                                for it in json.load(f)]
            logger.info(f'臨時リスト {app_settings.crawl_list_file} から巡回対象 {len(search_items)} 件を読みました')
        else:
            search_items = fetch_crawl("trefac")
            logger.info(f'司令塔から巡回対象 {len(search_items)} 件を取得しました')
    except Exception as e:
        logger.error(f'司令塔からの巡回対象取得に失敗しました（今回はスキップ）: {e}')
        return
    if not search_items:
        logger.warning('巡回対象が0件のため終了します')
        return
    # シャーディング: 複数サーバー構成のとき、このサーバーの担当分だけに絞る
    if app_settings.shard_count > 1:
        total_before = len(search_items)
        search_items = filter_items_for_shard(
            search_items, app_settings.shard_index, app_settings.shard_count
        )
        logger.info(
            f'シャーディング有効: 全{total_before}件中 {len(search_items)}件を担当します '
            f'（このサーバー = shard {app_settings.shard_index} / 全{app_settings.shard_count}台）'
        )

    all_list = len(search_items)
    logger.info(f'{all_list}件の在庫を調べます（並列数: {max_workers}）')

    # eBay反映・トークン管理は司令塔(ctrl-01)へ移管。ワーカーはログ/結果保護のロックのみ。
    result_lock = Lock()

    # 並列処理で各商品の在庫状況をチェック
    if max_workers == 1:
        # 順次処理（並列処理を使用しない場合）
        logger.info('順次処理モードで実行します')
        for i, item in enumerate(search_items, 1):
            result = check_single_item(
                item, i, all_list, judgment_word, result_lock
            )

            # ブロック検出の場合は即座に処理を停止
            if result.get("is_blocked", False):
                stop_event.set()
                blocked = True
                logger.error(f"{i}/{all_list} ブロック検出: 処理を停止します（残りはスキップ、ここまでの結果は保存します）")
                break

            # 結果を集約
            if result["no_stock_item"] is not None:
                no_stock_list.append(result["no_stock_item"])
            if result["failed_request"] is not None:
                failed_requests_list.append(result["failed_request"])
    else:
        # 並列処理モード
        logger.info(f'並列処理モードで実行します（並列数: {max_workers}）')
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # すべてのタスクを送信（メインループでのsleepは削除）
            futures = {}
            for i, item in enumerate(search_items, 1):
                future = executor.submit(
                    check_single_item,
                    item,
                    i,
                    all_list,
                    judgment_word,
                    result_lock
                )
                futures[future] = i

            # 結果を取得（完了した順に処理）
            completed_count = 0
            for future in as_completed(futures):
                completed_count += 1
                try:
                    result = future.result()

                    # ブロック検出の場合は即座に処理を停止
                    if result.get("is_blocked", False):
                        # 全ワーカーに停止を通知（実行中のスレッドも次のアイテムをスキップする）
                        stop_event.set()
                        blocked = True
                        stopped_at = completed_count
                        logger.error(f"{result['index']}/{all_list} ブロック検出: 全ワーカーを停止します（ここまでの結果は保存します）")
                        # まだ開始していないタスクをキャンセル
                        for f in futures:
                            f.cancel()
                        break

                    # 結果を集約
                    if result["no_stock_item"] is not None:
                        no_stock_list.append(result["no_stock_item"])
                    if result["failed_request"] is not None:
                        failed_requests_list.append(result["failed_request"])
                except Exception as e:
                    index = futures[future]
                    logger.error(f'{index}/{all_list} 商品チェック中にエラーが発生しました: {e}')

                # 進捗表示（10件ごと）
                if completed_count % 10 == 0 or completed_count == all_list:
                    logger.info(f'進捗: {completed_count}/{all_list}件完了')

    # 失敗したリクエストを再チェック（タイムアウトや接続エラーで失敗したアイテム）
    # ※ブロック中断時は trefac へ再アクセスしないよう再チェックをスキップする
    retry_result = None
    if blocked:
        logger.warning("ブロック中断のため、失敗リクエストの再チェックはスキップします")
    elif failed_requests_list:
        logger.info(f"=====失敗したリクエスト {len(failed_requests_list)}件を再チェックします=====")
        retry_result = retry_failed_requests(failed_requests_list, judgment_word)
        logger.info(f"再チェック結果: 保留解除={retry_result['restored']}件, "
                    f"在庫なし={len(retry_result['no_stock_items'])}件, 判定不能(保留)={retry_result['still_failed']}件")
        # 再チェックで正常に在庫なしと判定できた分だけ在庫変更に数える（判定不能は在庫を変えていない）
        no_stock_list.extend(retry_result['no_stock_items'])
    else:
        logger.info("再チェックが必要なリクエストはありませんでした")

    # 結果をファイルに保存してメール送信
    requests_file_path = output_file_name
    try:
        write_list(no_stock_list, requests_file_path)
    except IOError as e:
        logger.error(f'結果ファイルの書き込みに失敗しました: {e}')
        return

    run_subject = mail_subject
    mail_text = f"{len(no_stock_list)}件の在庫を変更しました。"
    if blocked:
        run_subject = f"[ブロック中断] {mail_subject}"
        mail_text += (
            "\n⚠ trefacのブロック検出により途中で停止したため、今回は一部の商品のみチェックしています"
            "（未チェック分は次回以降に持ち越し）。"
        )
    if retry_result:
        mail_text += (
            f"\n繋がらなかった {len(failed_requests_list)}件を再チェック: "
            f"在庫なし {len(retry_result['no_stock_items'])}件 / "
            f"判定不能 {retry_result['still_failed']}件（在庫は変えず司令塔で保留。"
            f"続けば司令塔が安全のため0にする）"
        )

    try:
        mail_sender.send_mail_with_file_to_fujiken36(run_subject, mail_text, requests_file_path)
    except Exception as e:
        logger.error(f'メール送信に失敗しました: {e}')

    # 司令塔へ完走報告（/opsの心拍＋ブロック中断の通知。メールが死んでも気づけるように）
    beat_note = f"在庫変更{len(no_stock_list)}件 / 担当{all_list}件"
    if blocked:
        beat_note += f" / ブロック検出で{stopped_at}件目に中断（残り{all_list - stopped_at}件は未チェック）"
    report_cron_beat("trefac", "blocked" if blocked else "ok", beat_note)

    logger.info('=====main_program is end=====')


def test() -> None:
    """
    テスト用関数: 指定したアイテムの在庫を0に設定する
    """
    item_num = 236541780350
    # eBayインスタンス作成
    try:
        ritz_auth_token = get_access_token(ritz_refresh_token)
        print(ritz_auth_token)
        ritz_ebay_api = EbayApi(ritz_auth_token)
        is_success = ritz_ebay_api.revise_inventory(item_num, 0)
        print(is_success)
    except Exception as e:
        logger.error(f'テスト実行中にエラーが発生しました: {e}')


if __name__ == "__main__":
    main()

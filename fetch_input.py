#!/usr/bin/env python3
"""DO Spaces(S3互換)からマスター入力CSVをダウンロードする。

trefac本体(aki_requests.py)の実行前に run_trefac.sh から呼ばれる前処理。
- 認証情報は gitignore 済みの setting/config.ini の [spaces] セクションから読む
  （= 秘密ファイルを増やさない）。
- 一時ファイルにDLしてからアトミックに置換するので、途中失敗で壊れたCSVが残らない。
- DL失敗時、既存のローカルCSV(input_file_pattern)があればそれで継続する
  （Spaces障害でも在庫監視を止めない）。無ければ非ゼロで終了。

[spaces] に必要なキー:
  access_key  = DO...                         （Spaces Access Key）
  secret_key  = ...                            （Spaces Secret。発行時のみ表示）
  bucket      = sushi-onsen-storage
  region      = sgp1
  object_key  = trefac/input.csv              （バケット内のオブジェクトパス）
  local_file  = aki - sushionsen_up_04_master.csv  （input_file_pattern に一致する名前）
"""
import configparser
import glob
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "setting", "config.ini")


def main() -> int:
    parser = configparser.ConfigParser()
    parser.optionxform = str  # キーの大文字小文字を保持
    if not parser.read(CONFIG_PATH, encoding="utf-8"):
        print(f"[fetch_input] config.ini が読めません: {CONFIG_PATH}", file=sys.stderr)
        return 1
    if not parser.has_section("spaces"):
        print("[fetch_input] config.ini に [spaces] セクションがありません", file=sys.stderr)
        return 1

    sp = parser["spaces"]
    try:
        access_key = sp["access_key"]
        secret_key = sp["secret_key"]
        bucket = sp["bucket"]
        region = sp["region"]
        object_key = sp["object_key"]
        local_file = sp["local_file"]
    except KeyError as e:
        print(f"[fetch_input] [spaces] に必須キーがありません: {e}", file=sys.stderr)
        return 1

    local_path = os.path.join(HERE, local_file)
    endpoint = f"https://{region}.digitaloceanspaces.com"

    tmp = None
    try:
        import boto3
        from botocore.config import Config

        client = boto3.client(
            "s3",
            region_name=region,
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            config=Config(retries={"max_attempts": 3, "mode": "standard"}),
        )
        fd, tmp = tempfile.mkstemp(dir=HERE, prefix=".csv_dl_")
        os.close(fd)
        client.download_file(bucket, object_key, tmp)
        os.replace(tmp, local_path)  # アトミック置換
        tmp = None
        size = os.path.getsize(local_path)
        print(f"[fetch_input] DL成功: s3://{bucket}/{object_key} -> {local_file} ({size} bytes)")
        return 0
    except Exception as e:
        print(f"[fetch_input] DL失敗: {e}", file=sys.stderr)
        # フォールバック: 既存のローカルCSVがあればキャッシュで継続
        pattern = parser.get("Settings", "input_file_pattern", fallback="*.csv")
        existing = glob.glob(os.path.join(HERE, pattern))
        if existing:
            print(f"[fetch_input] 既存のローカルCSVで継続します: {os.path.basename(existing[0])}", file=sys.stderr)
            return 0
        print("[fetch_input] ローカルCSVも無いため中止します", file=sys.stderr)
        return 1
    finally:
        if tmp and os.path.exists(tmp):
            try:
                os.remove(tmp)
            except OSError:
                pass


if __name__ == "__main__":
    sys.exit(main())

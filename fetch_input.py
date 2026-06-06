#!/usr/bin/env python3
"""DO Spaces(S3互換)からマスター入力CSVをダウンロードする。

trefac本体(aki_requests.py)の実行前に run_trefac.sh から呼ばれる前処理。
- 認証情報は gitignore 済みの setting/config.ini の [spaces] セクションから読む
  （= 秘密ファイルを増やさない）。
- 取得対象は「prefix(フォルダ)配下で最新更新の .csv」。
  → アップロードのファイル名は自由（版番号付き等）でよい。運用上そのフォルダに
    CSVは常に1個だけ置く想定だが、複数あっても最新を選ぶので安全。
- 一時ファイルにDLしてからアトミックに置換するので、途中失敗で壊れたCSVが残らない。
- 取得したCSVは local_file という固定名で保存する。aki_requests.py 側は
  input_file_pattern で探すので、local_file はそのパターンに一致する名前にしておく。
- DL失敗時、既存のローカルCSV(input_file_pattern)があればそれで継続する
  （Spaces障害でも在庫監視を止めない）。無ければ非ゼロで終了。

[spaces] に必要なキー:
  access_key  = DO...                              （Spaces Access Key）
  secret_key  = ...                                 （Spaces Secret。発行時のみ表示）
  bucket      = sushi-onsen-storage
  region      = sgp1
  prefix      = trefac/                            （走査するフォルダ。配下の最新.csvを取得）
  local_file  = aki - sushionsen_up_04_master.csv  （input_file_pattern に一致する保存名）
  # 後方互換: prefix の代わりに object_key=trefac/input.csv で固定名取得も可
"""
import configparser
import glob
import os
import sys
import tempfile

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "setting", "config.ini")


def _pick_object_key(client, bucket: str, prefix: str) -> str:
    """prefix 配下で最新更新の .csv オブジェクトのキーを返す。"""
    paginator = client.get_paginator("list_objects_v2")
    csvs = []
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            if obj["Key"].lower().endswith(".csv"):
                csvs.append(obj)
    if not csvs:
        raise FileNotFoundError(f"'{prefix}' 配下に .csv がありません")
    newest = max(csvs, key=lambda o: o["LastModified"])
    if len(csvs) > 1:
        print(f"[fetch_input] 警告: '{prefix}' に .csv が {len(csvs)} 個あります。最新を採用します。",
              file=sys.stderr)
    print(f"[fetch_input] 対象: s3://{bucket}/{newest['Key']} "
          f"(更新={newest['LastModified']})")
    return newest["Key"]


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
        local_file = sp["local_file"]
    except KeyError as e:
        print(f"[fetch_input] [spaces] に必須キーがありません: {e}", file=sys.stderr)
        return 1
    prefix = sp.get("prefix", "")
    object_key = sp.get("object_key", "")
    if not prefix and not object_key:
        print("[fetch_input] [spaces] に prefix か object_key のどちらかが必要です", file=sys.stderr)
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
        # prefix方式(フォルダ内の最新.csv) を優先。無ければ object_key の固定名取得。
        key_to_get = _pick_object_key(client, bucket, prefix) if prefix else object_key

        fd, tmp = tempfile.mkstemp(dir=HERE, prefix=".csv_dl_")
        os.close(fd)
        client.download_file(bucket, key_to_get, tmp)
        os.replace(tmp, local_path)  # アトミック置換
        tmp = None
        size = os.path.getsize(local_path)
        print(f"[fetch_input] DL成功: {key_to_get} -> {local_file} ({size} bytes)")
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

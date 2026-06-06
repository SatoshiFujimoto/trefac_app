#!/usr/bin/env python3
"""Spaces の trefac/fleet.json（全台の台数=shard_count を一元管理）を更新する。

使い方:
    venv/bin/python fleet_put.py 9      # shard_count を 9 にする

- 認証情報・bucket・region は setting/config.ini の [spaces] から読む
  （fetch_input.py と同じ流儀。秘密ファイルを増やさない）。
- 置き場所キーは [spaces] fleet_key（無ければ prefix 配下の fleet.json）。
"""
import configparser
import json
import os
import sys

HERE = os.path.dirname(os.path.abspath(__file__))
CONFIG_PATH = os.path.join(HERE, "setting", "config.ini")


def main() -> int:
    if len(sys.argv) != 2:
        print("usage: python fleet_put.py <shard_count>", file=sys.stderr)
        return 2
    try:
        count = int(sys.argv[1])
    except ValueError:
        print("shard_count は整数で指定してください", file=sys.stderr)
        return 2
    if count < 1:
        print("shard_count は1以上にしてください", file=sys.stderr)
        return 2

    parser = configparser.ConfigParser()
    parser.optionxform = str
    if not parser.read(CONFIG_PATH, encoding="utf-8"):
        print(f"config.ini が読めません: {CONFIG_PATH}", file=sys.stderr)
        return 1
    sp = parser["spaces"]

    fleet_key = sp.get("fleet_key") or (
        (sp.get("prefix", "").rstrip("/") + "/fleet.json") if sp.get("prefix") else "trefac/fleet.json"
    )

    import boto3

    s3 = boto3.client(
        "s3",
        region_name=sp["region"],
        endpoint_url=f"https://{sp['region']}.digitaloceanspaces.com",
        aws_access_key_id=sp["access_key"],
        aws_secret_access_key=sp["secret_key"],
    )
    body = json.dumps({"shard_count": count}).encode("utf-8")
    s3.put_object(
        Bucket=sp["bucket"], Key=fleet_key, Body=body, ContentType="application/json"
    )
    print(f'uploaded s3://{sp["bucket"]}/{fleet_key} = {{"shard_count": {count}}}')
    return 0


if __name__ == "__main__":
    sys.exit(main())

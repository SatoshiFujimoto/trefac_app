# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

A single-purpose automation script (`aki_requests.py`) that monitors Japanese
e-commerce product pages for stock and mirrors that availability onto eBay
listings. For each row in an input CSV it fetches the supplier URL, decides
in-stock vs. out-of-stock by substring match, and calls the eBay Trading API to
zero out (or restore) inventory. Results are written to a CSV and emailed.

## Running

```bash
source venv/bin/activate
python aki_requests.py          # runs main(); processes the input CSV end-to-end
```

There is no test suite, linter, or build step. The module-level `test()`
function (revises a hardcoded item to 0) is a manual probe and is NOT wired to
`__main__` — call it manually if needed.

## Architecture

Execution is top-to-bottom in `aki_requests.py`; import-time side effects matter:

1. **Logging setup (import time):** reads `log/aki_requests_log_config.json`,
   then rewrites the file handler's filename to `log/MMDDHH.log` (current date)
   so each run gets its own log. A missing/invalid config calls `sys.exit(1)`.
2. **Config + secrets (import time):** `setting/config.ini` is parsed into the
   `AppSettings` class. Credentials (eBay OAuth, LINE, Gmail app password) and
   tunables (`judgment_word`, timeouts, `max_workers`, file patterns) all live
   there. `AppSettings` fields are also mirrored into module-level globals for
   backward compatibility — keep both in sync when adding settings.
3. **`main()` flow:** glob the input CSV (`input_file_pattern`) → `csv_to_dict`
   → obtain eBay OAuth access token via `get_access_token` → check every item
   (sequential if `max_workers==1`, else `ThreadPoolExecutor`) → retry failures
   → write `output_file_name` → email it via `MailSender`.

### Stock-check logic (`send_request`)
Returns a union of `True` / `False` / sentinel strings
(`"timeout"`, `"connection_error"`, `"server_error"`, `"short_response"`,
`"blocked"`). Callers branch on these — preserve the exact string values.
- In-stock = `judgment_word` ("カートに入れる") present in page HTML.
- Retries 5xx / timeouts / connection errors with exponential backoff; treats
  responses shorter than `min_response_length` as suspect and retries.
- **403/429 → `"blocked"`**, which makes `main()` sleep 600s and stop the whole
  run (anti-ban guard). Both the sequential and parallel branches handle this.

### eBay API
`EbayApi` comes from an **external package outside this repo**, imported via
`sys.path.insert(0, "/home/fujiken/ebay-pkg")` at the top of `aki_requests.py`.
Source: `/home/fujiken/ebay-pkg/ebay_pkg/trading.py` (XML Trading API,
`ReviseInventoryStatus`). Key methods: `revise_inventory(item_id, num)`,
`revise_inventory_zero`, `end_item`. They return `"Success"` / `"Warning"` /
`"Failure"` strings — branch on those, not booleans.

### Concurrency & token refresh
In parallel mode, `check_single_item` runs per item. Two locks are threaded
through: `result_lock` guards logging/aggregation, `token_lock` guards eBay
token refresh. The shared `EbayApi` instance and the token timestamp are passed
as single-element lists (`ritz_ebay_api_ref`, `token_start_time_ref`) so worker
threads can swap in a refreshed token (every `token_refresh_interval` seconds).

### Notifications
- `line_notify` → LINE Messaging API push (used for hard eBay failures).
- `utils/MailSender.py` → Gmail SMTP (smtp.gmail.com:587, app password) sending
  the result CSV as an attachment to fujiken36@gmail.com.

## Input / output contract
- **Input CSV** columns (exact Japanese headers): `仕入れURL`, `eBay Item Number`.
  Selected by glob `input_file_pattern` — the first match is used.
- **Output:** `requests_reviced_item_list.csv`, one changed item per line.

## Important notes
- `setting/config.ini` holds **live plaintext secrets** (eBay tokens, Gmail app
  password, LINE token). Do not print, echo, or paste its contents into output,
  commits, or external services.
- `requirements.txt` is a full-system `pip freeze`, not a curated dependency
  list; the only real third-party runtime dep is `requests` (plus the external
  `ebay-pkg`). Use the project `venv/`.

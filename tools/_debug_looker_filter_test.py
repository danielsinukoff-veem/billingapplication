from __future__ import annotations

import argparse
import calendar
import json
from pathlib import Path
from urllib import error, parse, request


def login(base_url: str, api_version: str, client_id: str, client_secret: str) -> str:
    body = parse.urlencode({"client_id": client_id, "client_secret": client_secret}).encode()
    req = request.Request(
        f"{base_url.rstrip('/')}/api/{api_version}/login",
        data=body,
        headers={"Content-Type": "application/x-www-form-urlencoded", "Accept": "application/json"},
        method="POST",
    )
    with request.urlopen(req, timeout=60) as response:
        return json.loads(response.read().decode())["access_token"]


def get_json(base_url: str, api_version: str, token: str, path: str) -> dict:
    req = request.Request(
        f"{base_url.rstrip('/')}/api/{api_version}/{path.lstrip('/')}",
        headers={"Authorization": f"token {token}", "Accept": "application/json"},
    )
    with request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode())


def post_json(base_url: str, api_version: str, token: str, path: str, payload: dict) -> dict:
    req = request.Request(
        f"{base_url.rstrip('/')}/api/{api_version}/{path.lstrip('/')}",
        data=json.dumps(payload).encode(),
        headers={
            "Authorization": f"token {token}",
            "Accept": "application/json",
            "Content-Type": "application/json",
        },
        method="POST",
    )
    with request.urlopen(req, timeout=120) as response:
        return json.loads(response.read().decode())


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--config-file", required=True)
    parser.add_argument("--query-id", required=True)
    parser.add_argument("--filter-key", required=True)
    parser.add_argument("--year", type=int, default=2026)
    parser.add_argument("--month", type=int, default=3)
    args = parser.parse_args()

    config = json.loads(Path(args.config_file).read_text(encoding="utf-8"))
    token = login(config["baseUrl"], config.get("apiVersion", "4.0"), config["clientID"], config["clientSecret"])
    query = get_json(config["baseUrl"], config.get("apiVersion", "4.0"), token, f"queries/{parse.quote(args.query_id, safe='')}")

    month_name = calendar.month_name[args.month]
    values = [
        f"{args.year}/{args.month:02d}",
        f"{args.year}-{args.month:02d}",
        f"{month_name} {args.year}",
        f"{args.year}-{args.month:02d}-01 to {args.year}-{args.month:02d}-{calendar.monthrange(args.year, args.month)[1]}",
        "last month",
        "this month",
    ]

    for value in values:
        payload = {
            "model": query["model"],
            "view": query["view"],
            "fields": query["fields"],
            "filters": {**(query.get("filters") or {}), args.filter_key: value},
            "filter_expression": query.get("filter_expression"),
            "sorts": query.get("sorts"),
            "limit": "1",
            "column_limit": query.get("column_limit"),
            "total": query.get("total"),
            "row_total": query.get("row_total"),
            "subtotals": query.get("subtotals"),
            "vis_config": query.get("vis_config"),
            "filter_config": None,
            "query_timezone": query.get("query_timezone"),
            "dynamic_fields": query.get("dynamic_fields"),
        }
        try:
            created = post_json(config["baseUrl"], config.get("apiVersion", "4.0"), token, "queries", payload)
            print(json.dumps({"value": value, "status": "ok", "queryId": created["id"]}, indent=2), flush=True)
        except error.HTTPError as exc:
            detail = exc.read().decode(errors="replace")
            print(json.dumps({"value": value, "status": "http_error", "code": exc.code, "detail": detail[:500]}, indent=2), flush=True)
        except Exception as exc:  # noqa: BLE001
            print(json.dumps({"value": value, "status": "error", "detail": str(exc)}, indent=2), flush=True)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

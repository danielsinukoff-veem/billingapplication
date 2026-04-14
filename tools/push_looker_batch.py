from __future__ import annotations

import argparse
import base64
import json
import re
import sys
from dataclasses import dataclass
from datetime import date
from pathlib import Path
from typing import Any
from urllib import error, request


SUPPORTED_SUFFIXES = {".csv", ".xlsx"}
IMPORT_ORDER = [
    "partner_offline_billing",
    "partner_offline_billing_reversals",
    "all_registered_accounts",
    "partner_rev_share_v2",
    "partner_revenue_share",
    "partner_revenue_reversal",
    "partner_revenue_summary",
    "all_stampli_credit_complete",
    "stampli_fx_revenue_share",
    "stampli_fx_revenue_reversal",
]

FILE_RULES: list[tuple[str, re.Pattern[str]]] = [
    ("partner_offline_billing_reversals", re.compile(r"partner[_\s-]*offline[_\s-]*billing.*revers", re.I)),
    ("partner_offline_billing", re.compile(r"partner[_\s-]*offline[_\s-]*billing", re.I)),
    ("all_registered_accounts", re.compile(r"all[_\s-]*registered[_\s-]*accounts", re.I)),
    ("partner_rev_share_v2", re.compile(r"partner[_\s-]*rev[_\s-]*share[_\s-]*v2", re.I)),
    ("partner_revenue_reversal", re.compile(r"partner[_\s-]*revenue[_\s-]*reversal", re.I)),
    ("partner_revenue_summary", re.compile(r"partner[_\s-]*revenue[_\s-]*summary", re.I)),
    ("partner_revenue_share", re.compile(r"partner[_\s-]*revenue[_\s-]*share", re.I)),
    ("all_stampli_credit_complete", re.compile(r"all[_\s-]*stampli[_\s-]*credit[_\s-]*complete", re.I)),
    ("stampli_fx_revenue_reversal", re.compile(r"stampli[_\s-]*fx[_\s-]*revenue[_\s-]*reversal", re.I)),
    ("stampli_fx_revenue_share", re.compile(r"stampli[_\s-]*fx[_\s-]*revenue[_\s-]*share", re.I)),
]


@dataclass
class SelectedFile:
    file_type: str
    path: Path
    duplicates: list[Path]


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Push a batch of Looker exports into the billing workbook API.")
    parser.add_argument("--source-dir", required=True, help="Directory containing the Looker export files.")
    parser.add_argument("--period", default=date.today().strftime("%Y-%m"), help="Billing period in YYYY-MM.")
    parser.add_argument("--api-base-url", required=True, help="Billing API base URL, for example http://127.0.0.1:4174")
    parser.add_argument("--api-token", default="", help="Optional bearer token for the billing API.")
    parser.add_argument("--dry-run", action="store_true", help="Print the files that would be imported without posting them.")
    parser.add_argument("--fail-on-warnings", action="store_true", help="Exit non-zero if the API returns warnings.")
    return parser


def classify_file(path: Path) -> str | None:
    name = path.name
    for file_type, pattern in FILE_RULES:
        if pattern.search(name):
            return file_type
    return None


def choose_files(source_dir: Path) -> list[SelectedFile]:
    matches: dict[str, list[Path]] = {}
    for path in source_dir.rglob("*"):
        if not path.is_file() or path.suffix.lower() not in SUPPORTED_SUFFIXES:
            continue
        file_type = classify_file(path)
        if not file_type:
            continue
        matches.setdefault(file_type, []).append(path)

    selected: list[SelectedFile] = []
    for file_type in IMPORT_ORDER:
        paths = matches.get(file_type, [])
        if not paths:
            continue
        ordered = sorted(paths, key=lambda candidate: (candidate.stat().st_mtime, candidate.name.lower()), reverse=True)
        selected.append(SelectedFile(file_type=file_type, path=ordered[0], duplicates=ordered[1:]))
    return selected


def encode_file(path: Path) -> str:
    return base64.b64encode(path.read_bytes()).decode("ascii")


def post_import(api_base_url: str, api_token: str, payload: dict[str, Any]) -> dict[str, Any]:
    body = json.dumps(payload).encode("utf-8")
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json",
    }
    if api_token:
        headers["Authorization"] = f"Bearer {api_token}"
    req = request.Request(
        api_base_url.rstrip("/") + "/api/looker/import-and-save",
        data=body,
        headers=headers,
        method="POST",
    )
    try:
        with request.urlopen(req, timeout=120) as response:
            raw = response.read().decode("utf-8")
    except error.HTTPError as exc:
        detail = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"HTTP {exc.code}: {detail}") from exc
    except error.URLError as exc:
        raise RuntimeError(f"Could not reach billing API: {exc}") from exc
    try:
        return json.loads(raw)
    except json.JSONDecodeError as exc:
        raise RuntimeError(f"Billing API returned non-JSON output: {raw}") from exc


def main() -> int:
    args = build_parser().parse_args()
    source_dir = Path(args.source_dir).expanduser().resolve()
    if not source_dir.exists() or not source_dir.is_dir():
        raise SystemExit(f"Source directory does not exist: {source_dir}")

    selections = choose_files(source_dir)
    if not selections:
        raise SystemExit(f"No supported Looker exports were found under {source_dir}")

    summary: dict[str, Any] = {
        "sourceDir": str(source_dir),
        "period": args.period,
        "apiBaseUrl": args.api_base_url,
        "dryRun": bool(args.dry_run),
        "files": [],
    }

    warnings_found = False
    for selection in selections:
        entry: dict[str, Any] = {
            "fileType": selection.file_type,
            "fileName": selection.path.name,
            "path": str(selection.path),
            "duplicatesIgnored": [str(path) for path in selection.duplicates],
        }
        if args.dry_run:
            summary["files"].append(entry)
            continue

        payload = {
            "fileType": selection.file_type,
            "period": args.period,
            "fileName": selection.path.name,
            "fileBase64": encode_file(selection.path),
        }
        response = post_import(args.api_base_url, args.api_token, payload)
        warnings = response.get("warnings") or []
        if warnings:
            warnings_found = True
        entry.update(
            {
                "savedAt": response.get("savedAt"),
                "warnings": warnings,
                "stats": response.get("stats") or {},
                "sectionKeys": sorted((response.get("sections") or {}).keys()),
            }
        )
        summary["files"].append(entry)

    print(json.dumps(summary, indent=2))
    if warnings_found and args.fail_on_warnings:
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())

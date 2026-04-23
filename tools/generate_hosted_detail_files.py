#!/usr/bin/env python3
from __future__ import annotations

import re
import shutil
from pathlib import Path


REPO_ROOT = Path("/Users/danielsinukoff/Documents/billing-workbook")
CONTRACTS_ROOT = REPO_ROOT / "Partner Contracts"

TRANSACTION_EXPORT_PATTERN = re.compile(r"^(?P<partner>.+?) (?P<period>\d{4}-\d{2}) Transactions$")

PARTNER_ALIASES = {
    "LightNet": "Lightnet",
    "MultiGate": "Multigate",
}


def slugify(value: str) -> str:
    lowered = value.strip().lower()
    return re.sub(r"[^a-z0-9]+", "-", lowered).strip("-") or "partner"


def iter_transaction_exports() -> list[tuple[str, str, Path]]:
    exports: list[tuple[str, str, Path]] = []
    for csv_path in sorted(CONTRACTS_ROOT.glob("*/Transactions/*.csv")):
        match = TRANSACTION_EXPORT_PATTERN.match(csv_path.stem)
        if not match:
            continue
        partner = PARTNER_ALIASES.get(match.group("partner").strip(), match.group("partner").strip())
        period = match.group("period").strip()
        exports.append((partner, period, csv_path))
    return exports


def write_csv_copy(output_path: Path, source_path: Path) -> None:
    output_path.parent.mkdir(parents=True, exist_ok=True)
    shutil.copyfile(source_path, output_path)


def main() -> int:
    generated: list[tuple[str, str, Path, Path]] = []

    for partner, period, csv_path in iter_transaction_exports():
        output_path = REPO_ROOT / "hosted-detail-files-v1" / f"{slugify(partner)}-{period}-details.csv"
        write_csv_copy(output_path, csv_path)
        generated.append((partner, period, csv_path, output_path))

    print(f"Generated {len(generated)} hosted detail file(s).")
    for partner, period, csv_path, output_path in generated:
        print(f"  OK  {partner} {period} <- {csv_path} -> {output_path.relative_to(REPO_ROOT)}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

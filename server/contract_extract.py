"""Archival reference contract extraction helpers.

Production contract parsing is moving to an S3 + n8n workflow.
"""

from __future__ import annotations

import base64
import io
import json
import subprocess
import tempfile
from pathlib import Path
from typing import Any


ROOT_DIR = Path(__file__).resolve().parents[1]
SWIFT_EXTRACTOR = ROOT_DIR / "tools" / "extract_pdf_text.swift"
MODULE_CACHE = ROOT_DIR / ".swift-module-cache"


def extract_contract_text(payload: dict[str, Any]) -> dict[str, Any]:
    file_name = str(payload.get("fileName") or "").strip() or "contract.pdf"
    file_b64 = str(payload.get("fileBase64") or "").strip()
    if not file_b64:
        raise ValueError("Upload a contract PDF first.")

    suffix = Path(file_name).suffix.lower() or ".pdf"
    raw_bytes = base64.b64decode(file_b64)
    if suffix != ".pdf":
        text = raw_bytes.decode("utf-8", errors="replace")
        return {
            "fileName": file_name,
            "text": text,
            "charCount": len(text),
            "pageCount": 0,
        }

    try:
        return _extract_contract_text_with_swift(raw_bytes, file_name)
    except Exception:
        return _extract_contract_text_with_pypdf(raw_bytes, file_name)


def _extract_contract_text_with_swift(raw_bytes: bytes, file_name: str) -> dict[str, Any]:
    swift_binary = Path("/usr/bin/swift")
    if not swift_binary.exists() or not SWIFT_EXTRACTOR.exists():
        raise RuntimeError("Swift PDF extractor is unavailable.")

    MODULE_CACHE.mkdir(parents=True, exist_ok=True)
    with tempfile.TemporaryDirectory(prefix="billing-contract-") as temp_dir:
        pdf_path = Path(temp_dir) / (Path(file_name).name or "contract.pdf")
        pdf_path.write_bytes(raw_bytes)
        result = subprocess.run(
            [
                str(swift_binary),
                "-module-cache-path",
                str(MODULE_CACHE),
                str(SWIFT_EXTRACTOR),
                temp_dir,
            ],
            capture_output=True,
            text=True,
            check=True,
        )
        docs = json.loads(result.stdout or "[]")
        doc = docs[0] if docs else {"text": "", "pageCount": 0, "name": file_name}
        text = str(doc.get("text") or "")
        return {
            "fileName": str(doc.get("name") or file_name),
            "text": text,
            "charCount": len(text),
            "pageCount": int(doc.get("pageCount") or 0),
        }


def _extract_contract_text_with_pypdf(raw_bytes: bytes, file_name: str) -> dict[str, Any]:
    try:
        from pypdf import PdfReader
    except ImportError as error:
        raise RuntimeError("pypdf is required for PDF extraction when Swift is unavailable.") from error

    reader = PdfReader(io.BytesIO(raw_bytes))
    text_chunks = []
    for page in reader.pages:
        text_chunks.append(page.extract_text() or "")
    text = "\n".join(chunk for chunk in text_chunks if chunk)
    return {
        "fileName": file_name,
        "text": text,
        "charCount": len(text),
        "pageCount": len(reader.pages),
    }

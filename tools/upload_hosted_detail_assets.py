#!/usr/bin/env python3
from __future__ import annotations

import argparse
import mimetypes
import os
from pathlib import Path

import boto3


def build_s3_client():
    session = boto3.session.Session(
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name=os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION") or "us-east-1",
    )
    return session.client("s3")


def iter_files(paths: list[Path]) -> list[Path]:
    resolved: list[Path] = []
    for path in paths:
      if path.is_dir():
            resolved.extend(sorted(item for item in path.rglob("*") if item.is_file()))
      elif path.is_file():
            resolved.append(path)
    return resolved


def content_type_for(path: Path) -> str:
    content_type, _ = mimetypes.guess_type(str(path))
    return content_type or "application/octet-stream"


def upload_file(s3_client, bucket: str, key: str, source_path: Path, cache_control: str) -> None:
    extra_args = {
        "ContentType": content_type_for(source_path),
        "CacheControl": cache_control,
    }
    s3_client.upload_file(str(source_path), bucket, key, ExtraArgs=extra_args)


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload selected hosted billing assets to S3.")
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", default="partner-billing-form")
    parser.add_argument("--cache-control", default="no-cache")
    parser.add_argument("paths", nargs="+")
    args = parser.parse_args()

    prefix = str(args.prefix or "").strip("/ ")
    source_paths = [Path(path) for path in args.paths]
    files = iter_files(source_paths)
    if not files:
        raise SystemExit("No files found to upload.")

    repo_root = Path("/Users/danielsinukoff/Documents/billing-workbook")
    s3_client = build_s3_client()

    for source_path in files:
        relative_path = source_path.relative_to(repo_root)
        object_key = f"{prefix}/{relative_path.as_posix()}" if prefix else relative_path.as_posix()
        upload_file(s3_client, args.bucket, object_key, source_path, args.cache_control)
        print(f"uploaded s3://{args.bucket}/{object_key}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())

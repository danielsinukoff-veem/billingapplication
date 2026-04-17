"""Archival reference shared-workspace storage helpers.

This local SQLite-backed store exists only for reference and local
experimentation. Production state should live in the AWS-hosted stack.
"""

from __future__ import annotations

import json
import sqlite3
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any


@dataclass(frozen=True)
class WorkspaceIdentity:
    organization_slug: str = "veem-billing"
    organization_name: str = "Veem Billing Workspace"
    user_email: str = "billing.ops@veem.local"
    user_role: str = "billing_ops"


class SharedWorkspaceStore:
    def __init__(self, db_path: Path, identity: WorkspaceIdentity | None = None) -> None:
        self.db_path = db_path
        self.identity = identity or WorkspaceIdentity()
        self.db_path.parent.mkdir(parents=True, exist_ok=True)
        self._init_db()

    @contextmanager
    def connection(self):
        conn = sqlite3.connect(self.db_path, timeout=30)
        conn.row_factory = sqlite3.Row
        try:
            conn.execute("pragma busy_timeout = 30000")
            yield conn
        finally:
            conn.close()

    def _init_db(self) -> None:
        with self.connection() as conn:
            conn.execute("pragma journal_mode = wal")
            conn.execute("pragma synchronous = normal")
            conn.executescript(
                """
                create table if not exists organizations (
                  id integer primary key autoincrement,
                  slug text not null unique,
                  name text not null,
                  created_at text not null default current_timestamp
                );

                create table if not exists app_users (
                  id integer primary key autoincrement,
                  organization_id integer not null references organizations(id) on delete cascade,
                  email text not null,
                  role text not null,
                  created_at text not null default current_timestamp,
                  unique (organization_id, email)
                );

                create table if not exists workbook_snapshots (
                  id integer primary key autoincrement,
                  organization_id integer not null unique references organizations(id) on delete cascade,
                  snapshot_json text not null,
                  source_version integer not null default 1,
                  updated_by integer references app_users(id),
                  updated_at text not null default current_timestamp
                );

                create table if not exists audit_log (
                  id integer primary key autoincrement,
                  organization_id integer not null references organizations(id) on delete cascade,
                  actor_user_id integer references app_users(id),
                  entity_type text not null,
                  action text not null,
                  before_json text,
                  after_json text,
                  created_at text not null default current_timestamp
                );
                """
            )
            organization_id = self._ensure_organization(conn)
            self._ensure_user(conn, organization_id)
            conn.commit()

    def _ensure_organization(self, conn: sqlite3.Connection) -> int:
        row = conn.execute(
            "select id from organizations where slug = ?",
            (self.identity.organization_slug,),
        ).fetchone()
        if row:
            return int(row["id"])
        cursor = conn.execute(
            "insert into organizations (slug, name) values (?, ?)",
            (self.identity.organization_slug, self.identity.organization_name),
        )
        return int(cursor.lastrowid)

    def _ensure_user(self, conn: sqlite3.Connection, organization_id: int) -> int:
        row = conn.execute(
            "select id from app_users where organization_id = ? and email = ?",
            (organization_id, self.identity.user_email),
        ).fetchone()
        if row:
            return int(row["id"])
        cursor = conn.execute(
            "insert into app_users (organization_id, email, role) values (?, ?, ?)",
            (organization_id, self.identity.user_email, self.identity.user_role),
        )
        return int(cursor.lastrowid)

    def get_workspace(self) -> dict[str, Any]:
        with self.connection() as conn:
            organization_id = self._ensure_organization(conn)
            user_id = self._ensure_user(conn, organization_id)
            org = conn.execute(
                "select slug, name from organizations where id = ?",
                (organization_id,),
            ).fetchone()
            user = conn.execute(
                "select email, role from app_users where id = ?",
                (user_id,),
            ).fetchone()
            snapshot_row = conn.execute(
                "select snapshot_json, updated_at from workbook_snapshots where organization_id = ?",
                (organization_id,),
            ).fetchone()
        return {
            "workspace": {
                "label": org["name"],
                "mode": "shared",
            },
            "user": {
                "email": user["email"],
                "role": user["role"],
            },
            "snapshot": json.loads(snapshot_row["snapshot_json"]) if snapshot_row else None,
            "savedAt": snapshot_row["updated_at"] if snapshot_row else None,
        }

    def save_snapshot(self, snapshot: dict[str, Any]) -> str:
        encoded = json.dumps(snapshot)
        with self.connection() as conn:
            organization_id = self._ensure_organization(conn)
            user_id = self._ensure_user(conn, organization_id)
            before = conn.execute(
                "select snapshot_json from workbook_snapshots where organization_id = ?",
                (organization_id,),
            ).fetchone()
            conn.execute(
                """
                insert into workbook_snapshots (organization_id, snapshot_json, updated_by)
                values (?, ?, ?)
                on conflict (organization_id) do update set
                  snapshot_json = excluded.snapshot_json,
                  updated_by = excluded.updated_by,
                  updated_at = current_timestamp
                """,
                (organization_id, encoded, user_id),
            )
            after = conn.execute(
                "select updated_at from workbook_snapshots where organization_id = ?",
                (organization_id,),
            ).fetchone()
            conn.execute(
                """
                insert into audit_log (organization_id, actor_user_id, entity_type, action, before_json, after_json)
                values (?, ?, 'workbook_snapshot', 'save', ?, ?)
                """,
                (organization_id, user_id, before["snapshot_json"] if before else None, encoded),
            )
            conn.commit()
            return str(after["updated_at"])

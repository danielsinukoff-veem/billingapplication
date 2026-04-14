from __future__ import annotations

import json
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any
from uuid import uuid5, NAMESPACE_URL

from .storage import WorkspaceIdentity


@dataclass(frozen=True)
class SupabaseConfig:
    url: str
    service_role_key: str
    timeout_seconds: float = 30.0


class SupabaseStoreError(RuntimeError):
    pass


class SupabaseWorkspaceStore:
    def __init__(self, config: SupabaseConfig, identity: WorkspaceIdentity | None = None) -> None:
        self.config = config
        self.identity = identity or WorkspaceIdentity()
        self.base_url = self.config.url.rstrip("/")

    def _headers(self, extra: dict[str, str] | None = None) -> dict[str, str]:
        headers = {
            "Accept": "application/json",
            "apikey": self.config.service_role_key,
            "Authorization": f"Bearer {self.config.service_role_key}",
        }
        if extra:
            headers.update(extra)
        return headers

    def _request(
        self,
        method: str,
        path: str,
        *,
        params: dict[str, Any] | None = None,
        payload: Any = None,
        headers: dict[str, str] | None = None,
    ) -> Any:
        url = f"{self.base_url}{path}"
        if params:
            query = urllib.parse.urlencode(
                {key: value for key, value in params.items() if value is not None and value != ""},
                doseq=True,
            )
            if query:
                url = f"{url}?{query}"
        body = None
        request_headers = self._headers(headers)
        if payload is not None:
            body = json.dumps(payload).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request = urllib.request.Request(url, data=body, headers=request_headers, method=method)
        try:
            with urllib.request.urlopen(request, timeout=self.config.timeout_seconds) as response:
                raw = response.read().decode("utf-8")
        except urllib.error.HTTPError as error:
            raw = error.read().decode("utf-8")
            message = raw
            try:
                parsed = json.loads(raw) if raw else {}
                if isinstance(parsed, dict):
                    message = parsed.get("message") or parsed.get("error_description") or parsed.get("hint") or raw
            except json.JSONDecodeError:
                pass
            raise SupabaseStoreError(f"Supabase request failed ({method} {path}): {message}") from error
        except urllib.error.URLError as error:
            raise SupabaseStoreError(f"Could not reach Supabase for {method} {path}: {error}") from error
        if not raw:
            return None
        try:
            return json.loads(raw)
        except json.JSONDecodeError:
            return raw

    def _select_first(self, table: str, *, filters: dict[str, Any], select: str) -> dict[str, Any] | None:
        payload = self._request(
            "GET",
            f"/rest/v1/{table}",
            params={**filters, "select": select, "limit": 1},
        )
        if isinstance(payload, list) and payload:
            return payload[0]
        return None

    def _insert_row(self, table: str, payload: dict[str, Any], *, prefer: str = "return=representation") -> dict[str, Any] | None:
        rows = self._request(
            "POST",
            f"/rest/v1/{table}",
            payload=payload,
            headers={"Prefer": prefer},
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def _upsert_row(
        self,
        table: str,
        payload: dict[str, Any],
        *,
        conflict_target: str,
        prefer: str = "resolution=merge-duplicates,return=representation",
    ) -> dict[str, Any] | None:
        rows = self._request(
            "POST",
            f"/rest/v1/{table}",
            params={"on_conflict": conflict_target},
            payload=payload,
            headers={"Prefer": prefer},
        )
        if isinstance(rows, list):
            return rows[0] if rows else None
        return rows if isinstance(rows, dict) else None

    def _ensure_organization(self) -> dict[str, Any]:
        row = self._select_first(
            "organizations",
            filters={"slug": f"eq.{self.identity.organization_slug}"},
            select="id,name,slug",
        )
        if row:
            return row
        created = self._insert_row(
            "organizations",
            {
                "slug": self.identity.organization_slug,
                "name": self.identity.organization_name,
            },
        )
        if not created:
            raise SupabaseStoreError("Could not create organization in Supabase.")
        return created

    def _ensure_user(self, organization_id: str) -> dict[str, Any]:
        row = self._select_first(
            "app_users",
            filters={
                "organization_id": f"eq.{organization_id}",
                "email": f"eq.{self.identity.user_email}",
            },
            select="id,email",
        )
        if row:
            self._ensure_user_role(organization_id, row["id"])
            return {"id": row["id"], "email": row["email"], "role": self.identity.user_role}
        user_id = str(uuid5(NAMESPACE_URL, f"{self.identity.organization_slug}:{self.identity.user_email}"))
        created = self._insert_row(
            "app_users",
            {
                "id": user_id,
                "organization_id": organization_id,
                "email": self.identity.user_email,
                "is_active": True,
            },
        )
        if not created:
            raise SupabaseStoreError("Could not create app user in Supabase.")
        self._ensure_user_role(organization_id, user_id)
        return {"id": user_id, "email": self.identity.user_email, "role": self.identity.user_role}

    def _ensure_user_role(self, organization_id: str, user_id: str) -> None:
        existing = self._select_first(
            "user_roles",
            filters={
                "organization_id": f"eq.{organization_id}",
                "user_id": f"eq.{user_id}",
                "role": f"eq.{self.identity.user_role}",
            },
            select="id,role",
        )
        if existing:
            return
        self._insert_row(
            "user_roles",
            {
                "organization_id": organization_id,
                "user_id": user_id,
                "role": self.identity.user_role,
            },
            prefer="return=minimal",
        )

    def _normalize_snapshot_payload(self, snapshot_value: Any) -> dict[str, Any] | None:
        if snapshot_value is None:
            return None
        if isinstance(snapshot_value, dict):
            return snapshot_value
        if isinstance(snapshot_value, str):
            return json.loads(snapshot_value)
        raise SupabaseStoreError("Unexpected snapshot payload returned from Supabase.")

    def get_workspace(self) -> dict[str, Any]:
        organization = self._ensure_organization()
        user = self._ensure_user(organization["id"])
        snapshot_row = self._select_first(
            "workbook_snapshots",
            filters={"organization_id": f"eq.{organization['id']}"},
            select="snapshot_json,updated_at",
        )
        return {
            "workspace": {
                "label": organization["name"],
                "mode": "shared",
            },
            "user": {
                "email": user["email"],
                "role": user["role"],
            },
            "snapshot": self._normalize_snapshot_payload(snapshot_row["snapshot_json"]) if snapshot_row else None,
            "savedAt": snapshot_row["updated_at"] if snapshot_row else None,
        }

    def save_snapshot(self, snapshot: dict[str, Any]) -> str:
        organization = self._ensure_organization()
        user = self._ensure_user(organization["id"])
        before = self._select_first(
            "workbook_snapshots",
            filters={"organization_id": f"eq.{organization['id']}"},
            select="id,snapshot_json,updated_at",
        )
        upserted = self._upsert_row(
            "workbook_snapshots",
            {
                "organization_id": organization["id"],
                "snapshot_json": snapshot,
                "source_version": int(snapshot.get("_version") or 1),
                "updated_by": user["id"],
                "updated_at": snapshot.get("_saved"),
            },
            conflict_target="organization_id",
        )
        if not upserted:
            raise SupabaseStoreError("Could not save workbook snapshot in Supabase.")
        self._insert_row(
            "audit_log",
            {
                "organization_id": organization["id"],
                "actor_user_id": user["id"],
                "entity_type": "workbook_snapshot",
                "entity_id": upserted.get("id"),
                "action": "save",
                "before_json": before["snapshot_json"] if before else None,
                "after_json": snapshot,
            },
            prefer="return=minimal",
        )
        return str(upserted.get("updated_at") or snapshot.get("_saved") or "")

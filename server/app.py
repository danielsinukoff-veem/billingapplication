from __future__ import annotations

import argparse
import json
import os
import sys
from datetime import datetime, timezone
from http import HTTPStatus
from http.server import SimpleHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

if __package__ in (None, ""):
    sys.path.append(str(Path(__file__).resolve().parents[1]))
    from server.automation_engine import build_automation_outbox  # type: ignore
    from server.contract_extract import extract_contract_text  # type: ignore
    from server.contract_parse import parse_contract_text  # type: ignore
    from server.invoice_engine import calculate_invoice  # type: ignore
    from server.looker_direct_sync import run_direct_looker_sync  # type: ignore
    from server.looker_update import apply_looker_import_result, build_looker_import_change_summary, parse_looker_file, update_looker_import_audit  # type: ignore
    from server.storage import SharedWorkspaceStore, WorkspaceIdentity  # type: ignore
    from server.supabase_store import SupabaseConfig, SupabaseWorkspaceStore  # type: ignore
else:
    from .automation_engine import build_automation_outbox
    from .contract_extract import extract_contract_text
    from .contract_parse import parse_contract_text
    from .invoice_engine import calculate_invoice
    from .looker_direct_sync import run_direct_looker_sync
    from .looker_update import apply_looker_import_result, build_looker_import_change_summary, parse_looker_file, update_looker_import_audit
    from .storage import SharedWorkspaceStore, WorkspaceIdentity
    from .supabase_store import SupabaseConfig, SupabaseWorkspaceStore


ROOT_DIR = Path(__file__).resolve().parents[1]
DB_PATH = ROOT_DIR / "server" / "data" / "shared_workspace.db"
DEFAULT_PORT = 4174
DEFAULT_BACKEND = "sqlite"


def default_host() -> str:
    configured = os.getenv("BILLING_SERVER_HOST", "").strip()
    if configured:
        return configured
    if os.getenv("PORT"):
        return "0.0.0.0"
    return "127.0.0.1"


def default_port() -> int:
    raw = os.getenv("PORT", "").strip()
    if raw:
        try:
            return int(raw)
        except ValueError:
            pass
    return DEFAULT_PORT


class BillingRequestHandler(SimpleHTTPRequestHandler):
    server_version = "BillingSharedServer/0.1"

    def __init__(
        self,
        *args,
        directory: str | None = None,
        store=None,
        workspace_label: str = "Veem Billing Shared Workspace",
        backend_name: str = DEFAULT_BACKEND,
        api_token: str = "",
        **kwargs,
    ):
        self.store = store or SharedWorkspaceStore(DB_PATH)
        self.workspace_label = workspace_label
        self.backend_name = backend_name
        self.api_token = api_token
        super().__init__(*args, directory=directory or str(ROOT_DIR), **kwargs)

    def end_headers(self) -> None:
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Headers", "Content-Type, Authorization")
        self.send_header("Access-Control-Allow-Methods", "GET, PUT, POST, OPTIONS")
        super().end_headers()

    def do_OPTIONS(self) -> None:
        self.send_response(HTTPStatus.NO_CONTENT)
        self.end_headers()

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path == "/api/health":
            self.respond_json({
                "status": "ok",
                "time": datetime.now(timezone.utc).isoformat(),
                "backend": self.backend_name,
                "workspaceLabel": self.workspace_label,
            })
            return
        if parsed.path.startswith("/api/") and not self.authorize_request():
            return
        if parsed.path == "/api/bootstrap":
            self.handle_bootstrap()
            return
        if parsed.path == "/api/invoices/draft":
            self.handle_invoice_draft(parsed.query)
            return
        if parsed.path == "/api/automation/outbox":
            self.handle_automation_outbox(parsed.query)
            return
        if parsed.path in ("/", "/index.html"):
            self.handle_index()
            return
        super().do_GET()

    def do_PUT(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/") and not self.authorize_request():
            return
        if parsed.path == "/api/workbook":
            self.handle_save_workbook()
            return
        self.respond_error(HTTPStatus.NOT_FOUND, "Route not found.")

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        if parsed.path.startswith("/api/") and not self.authorize_request():
            return
        if parsed.path == "/api/looker/import-and-save":
            self.handle_looker_import_and_save()
            return
        if parsed.path == "/api/looker/direct-sync":
            self.handle_looker_direct_sync()
            return
        if parsed.path == "/api/looker/import":
            self.handle_looker_import()
            return
        if parsed.path == "/api/contracts/extract":
            self.handle_contract_extract()
            return
        if parsed.path == "/api/contracts/parse":
            self.handle_contract_parse()
            return
        self.respond_error(HTTPStatus.NOT_FOUND, "Route not found.")

    def authorize_request(self) -> bool:
        if not self.api_token:
            return True
        header = self.headers.get("Authorization", "").strip()
        if header == f"Bearer {self.api_token}":
            return True
        self.respond_error(HTTPStatus.UNAUTHORIZED, "Unauthorized")
        return False

    def handle_index(self) -> None:
        index_path = ROOT_DIR / "index.html"
        html = index_path.read_text(encoding="utf-8")
        config_script = (
            "<script>"
            "window.BILLING_APP_CONFIG = {"
            'mode: "shared",'
            f'workspaceLabel: {json.dumps(self.workspace_label)},'
            "apiBaseUrl: window.location.origin,"
            "enableSharedWorkbook: true,"
            "enableRemoteInvoiceReads: true"
            "};"
            "</script>\n"
        )
        injected = html.replace("</body>", f"{config_script}</body>")
        body = injected.encode("utf-8")
        self.send_response(HTTPStatus.OK)
        self.send_header("Content-Type", "text/html; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def handle_bootstrap(self) -> None:
        payload = self.store.get_workspace()
        self.respond_json(payload)

    def handle_save_workbook(self) -> None:
        payload = self.read_json_body()
        snapshot = payload.get("snapshot")
        if not isinstance(snapshot, dict):
            self.respond_error(HTTPStatus.BAD_REQUEST, "Request must include a snapshot object.")
            return
        saved_at = self.store.save_snapshot(snapshot)
        self.respond_json({"savedAt": saved_at})

    def handle_invoice_draft(self, query: str) -> None:
        params = parse_qs(query)
        partner = (params.get("partner") or [""])[0]
        start_period = (params.get("startPeriod") or params.get("period") or [""])[0]
        end_period = (params.get("endPeriod") or [start_period])[0]
        if not partner or not start_period:
            self.respond_error(HTTPStatus.BAD_REQUEST, "partner and startPeriod are required.")
            return
        workspace = self.store.get_workspace()
        snapshot = workspace.get("snapshot")
        if not isinstance(snapshot, dict):
            self.respond_error(HTTPStatus.NOT_FOUND, "No shared workbook snapshot has been saved yet.")
            return
        try:
            invoice = calculate_invoice(snapshot, partner, start_period, end_period)
        except Exception as error:  # pragma: no cover - defensive guard for local service
            self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not calculate invoice: {error}")
            return
        self.respond_json({
            "invoice": invoice,
            "generatedAt": datetime.now(timezone.utc).isoformat(),
            "source": "server",
        })

    def handle_automation_outbox(self, query: str) -> None:
        params = parse_qs(query)
        as_of = (params.get("asOf") or [""])[0] or ""
        lookahead_days = (params.get("lookaheadDays") or [""])[0] or ""
        try:
            lookahead_value = int(lookahead_days or 45)
        except ValueError:
            self.respond_error(HTTPStatus.BAD_REQUEST, "lookaheadDays must be an integer.")
            return
        workspace = self.store.get_workspace()
        snapshot = workspace.get("snapshot")
        if not isinstance(snapshot, dict):
            self.respond_error(HTTPStatus.NOT_FOUND, "No shared workbook snapshot has been saved yet.")
            return
        try:
            forwarded_proto = (self.headers.get("X-Forwarded-Proto") or "").strip()
            host = (self.headers.get("X-Forwarded-Host") or self.headers.get("Host") or "").strip()
            if forwarded_proto and host:
                base_url = f"{forwarded_proto}://{host}"
            elif host:
                base_url = f"http://{host}"
            else:
                base_url = f"http://127.0.0.1:{self.server.server_port}" if getattr(self.server, "server_port", None) else ""
            outbox = build_automation_outbox(
                snapshot,
                as_of=as_of or None,
                lookahead_days=lookahead_value,
                operator_email=str(workspace.get("user", {}).get("email") or self.store.identity.user_email),
                base_url=base_url,
            )
        except Exception as error:
            self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not build billing automation outbox: {error}")
            return
        self.respond_json(outbox)

    def handle_looker_import(self) -> None:
        try:
            payload = self.read_json_body()
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        try:
            result = parse_looker_file(payload)
        except Exception as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, f"Could not parse Looker input: {error}")
            return
        self.respond_json(result)

    def handle_looker_import_and_save(self) -> None:
        workspace = self.store.get_workspace()
        snapshot = workspace.get("snapshot")
        if not isinstance(snapshot, dict):
            self.respond_error(HTTPStatus.NOT_FOUND, "No shared workbook snapshot has been saved yet.")
            return
        try:
            payload = self.read_json_body()
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        effective_payload = dict(payload)
        saved_context = snapshot.get("lookerImportContext") or {}
        request_context = payload.get("context") or {}
        effective_payload["context"] = {
            **saved_context,
            **request_context,
        }
        try:
            result = parse_looker_file(effective_payload)
        except Exception as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, f"Could not parse Looker input: {error}")
            return
        try:
            run_id = str(payload.get("runId") or f"manual-{datetime.now(timezone.utc).strftime('%Y%m%dT%H%M%S')}")
            saved_stamp = datetime.now(timezone.utc).isoformat()
            next_snapshot = apply_looker_import_result(snapshot, result)
            result["changeSummary"] = build_looker_import_change_summary(snapshot, next_snapshot, result)
            if isinstance(payload.get("sourceMetadata"), dict):
                result["sourceMetadata"] = dict(payload.get("sourceMetadata") or {})
            next_snapshot["_saved"] = saved_stamp
            update_looker_import_audit(next_snapshot, result, run_id, saved_stamp, "server")
            saved_at = self.store.save_snapshot(next_snapshot)
        except Exception as error:
            self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not save Looker import: {error}")
            return
        self.respond_json({
            **result,
            "runId": run_id,
            "savedAt": saved_at,
            "source": "server",
        })

    def handle_looker_direct_sync(self) -> None:
        workspace = self.store.get_workspace()
        snapshot = workspace.get("snapshot")
        if not isinstance(snapshot, dict):
            self.respond_error(HTTPStatus.NOT_FOUND, "No shared workbook snapshot has been saved yet.")
            return
        try:
            payload = self.read_json_body()
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        try:
            next_snapshot, summary, imported_any = run_direct_looker_sync(snapshot, payload)
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        except Exception as error:
            self.respond_error(HTTPStatus.INTERNAL_SERVER_ERROR, f"Could not complete direct Looker sync: {error}")
            return

        saved_at = None
        if not bool(payload.get("dryRun")) and imported_any:
            saved_at = self.store.save_snapshot(next_snapshot)
        summary["savedAt"] = saved_at
        self.respond_json(summary)

    def handle_contract_extract(self) -> None:
        try:
            payload = self.read_json_body()
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        try:
            result = extract_contract_text(payload)
        except Exception as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, f"Could not extract contract text: {error}")
            return
        self.respond_json(result)

    def handle_contract_parse(self) -> None:
        try:
            payload = self.read_json_body()
        except ValueError as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, str(error))
            return
        try:
            result = parse_contract_text(payload)
        except Exception as error:
            self.respond_error(HTTPStatus.BAD_REQUEST, f"Could not parse the contract text: {error}")
            return
        self.respond_json(result)

    def read_json_body(self) -> dict:
        content_length = int(self.headers.get("Content-Length", "0"))
        raw = self.rfile.read(content_length) if content_length > 0 else b"{}"
        try:
            return json.loads(raw.decode("utf-8") or "{}")
        except json.JSONDecodeError as error:
            raise ValueError(f"Invalid JSON body: {error}") from error

    def respond_json(self, payload: dict, status: HTTPStatus = HTTPStatus.OK) -> None:
        body = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def respond_error(self, status: HTTPStatus, message: str) -> None:
        self.respond_json({"error": message}, status=status)


def build_handler(store: SharedWorkspaceStore, api_token: str):
    workspace_label = getattr(getattr(store, "identity", None), "organization_name", "Veem Billing Shared Workspace")
    backend_name = "supabase" if isinstance(store, SupabaseWorkspaceStore) else "sqlite"

    def handler(*args, **kwargs):
        BillingRequestHandler(
            *args,
            directory=str(ROOT_DIR),
            store=store,
            workspace_label=workspace_label,
            backend_name=backend_name,
            api_token=api_token,
            **kwargs,
        )
    return handler


def build_identity(args: argparse.Namespace) -> WorkspaceIdentity:
    return WorkspaceIdentity(
        organization_slug=args.org_slug,
        organization_name=args.org_name,
        user_email=args.user_email,
        user_role=args.user_role,
    )


def build_store(args: argparse.Namespace):
    identity = build_identity(args)
    if args.backend == "supabase":
        if not args.supabase_url or not args.supabase_service_role_key:
            raise SystemExit("Supabase backend requires SUPABASE_URL and SUPABASE_SERVICE_ROLE_KEY.")
        return SupabaseWorkspaceStore(
            SupabaseConfig(
                url=args.supabase_url,
                service_role_key=args.supabase_service_role_key,
            ),
            identity=identity,
        )
    return SharedWorkspaceStore(args.db, identity=identity)


def main() -> None:
    parser = argparse.ArgumentParser(description="Serve the shared billing workbook app and API.")
    parser.add_argument("--host", default=default_host())
    parser.add_argument("--port", type=int, default=default_port())
    parser.add_argument("--backend", choices=["sqlite", "supabase"], default=os.getenv("BILLING_BACKEND", DEFAULT_BACKEND))
    parser.add_argument("--db", type=Path, default=DB_PATH)
    parser.add_argument("--supabase-url", default=os.getenv("SUPABASE_URL", ""))
    parser.add_argument("--supabase-service-role-key", default=os.getenv("SUPABASE_SERVICE_ROLE_KEY", ""))
    parser.add_argument("--org-slug", default=os.getenv("BILLING_ORG_SLUG", "veem-billing"))
    parser.add_argument("--org-name", default=os.getenv("BILLING_ORG_NAME", "Veem Billing Workspace"))
    parser.add_argument("--user-email", default=os.getenv("BILLING_DEFAULT_USER_EMAIL", "billing.ops@veem.local"))
    parser.add_argument("--user-role", default=os.getenv("BILLING_DEFAULT_USER_ROLE", "billing_ops"))
    parser.add_argument("--api-token", default=os.getenv("BILLING_API_TOKEN", ""))
    args = parser.parse_args()

    store = build_store(args)
    server = ThreadingHTTPServer((args.host, args.port), build_handler(store, args.api_token))
    print(f"Shared billing server running at http://{args.host}:{args.port} ({args.backend})")
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nStopping server.")
    finally:
        server.server_close()


if __name__ == "__main__":
    main()

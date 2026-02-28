"""Phase 4D: Shell Sovereignty Over Harbor Charles — Tests.

Test groups per contract:
    TEST-4D-1: Missing WEC fails
    TEST-4D-2: Activation gate
    TEST-4D-3: Token mediation
    TEST-4D-4: Route audit
    TEST-4D-5: No standalone runtime
"""

import os
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

# ---------------------------------------------------------------------------
# Resolve repo root so imports work regardless of cwd
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parents[3]  # apps/charles/tests -> repo root
CHARLES_ROOT = REPO_ROOT / "apps" / "charles"
SHELL_ROOT = REPO_ROOT / "shell"

# Ensure import paths:
#   apps/     → for "charles.*" imports
#   <root>    → for "shell.*" imports
#   shared/   → for "harbor_common.*" imports
for p in [str(REPO_ROOT / "apps"), str(REPO_ROOT), str(REPO_ROOT / "shared")]:
    if p not in sys.path:
        sys.path.insert(0, p)

from harbor_common.errors import HarborError
from shell.core.wec import WorkspaceExecutionContext, build_wec
from charles.security.wec_guard import require_wec


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_wec(**overrides) -> WorkspaceExecutionContext:
    """Build a valid WEC for testing."""
    defaults = dict(
        workspace_id="ws-test-001",
        app_id="charles",
        qbo_realm_id="realm-123",
        qbo_access_token="tok-abc",
        issued_at=datetime.now(timezone.utc),
        request_id="req-xyz",
    )
    defaults.update(overrides)
    return build_wec(**defaults)


# =========================================================================
# TEST-4D-1: Missing WEC Fails
# =========================================================================

class TestMissingWECFails:
    """INV-4D-2 — WEC is mandatory for any Charles execution."""

    def test_require_wec_none_raises_value_error(self):
        with pytest.raises(ValueError, match="WorkspaceExecutionContext is required"):
            require_wec(None)

    def test_require_wec_wrong_type_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected WorkspaceExecutionContext"):
            require_wec("not a wec")

    def test_require_wec_wrong_type_dict_raises_type_error(self):
        with pytest.raises(TypeError, match="Expected WorkspaceExecutionContext"):
            require_wec({"workspace_id": "ws-1"})

    def test_require_wec_empty_workspace_id_raises_value_error(self):
        wec = build_wec(
            workspace_id="",
            app_id="charles",
            qbo_realm_id="r",
            qbo_access_token="t",
            issued_at=datetime.now(timezone.utc),
        )
        with pytest.raises(ValueError, match="WEC.workspace_id is required"):
            require_wec(wec)

    def test_require_wec_valid_returns_wec(self):
        wec = _make_wec()
        result = require_wec(wec)
        assert result is wec

    def test_charles_module_run_sync_none_wec_raises(self):
        """CharlesModule.run_sync(None) must raise ValueError."""
        # Mock heavy dependencies before importing module_entry
        sys.modules.setdefault("charles.database", MagicMock())
        sys.modules.setdefault("charles.database.module2_database", MagicMock())
        sys.modules.setdefault("charles.domain", MagicMock())
        sys.modules.setdefault("charles.domain.sync_manager", MagicMock())
        sys.modules.setdefault("charles.domain.match_payout", MagicMock())

        import importlib
        import charles.module_entry as me
        importlib.reload(me)  # reload with mocked deps

        module = me.CharlesModule("/tmp/test.db")
        with pytest.raises(ValueError, match="WorkspaceExecutionContext is required"):
            module.run_sync(None)

    def test_charles_module_build_deposit_none_wec_raises(self):
        """CharlesModule.build_deposit(None, payout_id) must raise ValueError."""
        sys.modules.setdefault("charles.database", MagicMock())
        sys.modules.setdefault("charles.database.module2_database", MagicMock())
        sys.modules.setdefault("charles.domain", MagicMock())
        sys.modules.setdefault("charles.domain.sync_manager", MagicMock())

        import importlib
        import charles.module_entry as me
        importlib.reload(me)

        module = me.CharlesModule("/tmp/test.db")
        with pytest.raises(ValueError, match="WorkspaceExecutionContext is required"):
            module.build_deposit(None, "payout-123")


# =========================================================================
# TEST-4D-2: Activation Gate
# =========================================================================

class TestActivationGate:
    """INV-4D-3 — activation gate happens before Charles logic."""

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_workspace_not_found_raises_not_found(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        mock_db.return_value.cursor.return_value = cursor

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "NOT_FOUND"
        assert exc_info.value.status_code == 404

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_no_entitlement_raises_forbidden(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": False,
            "activation_completed": False,
            "qbo_status": None,
        }

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "FORBIDDEN"
        assert exc_info.value.status_code == 403

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_activation_not_completed_raises_forbidden(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": True,
            "activation_completed": False,
            "qbo_status": None,
        }

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "FORBIDDEN"
        assert exc_info.value.status_code == 403


# =========================================================================
# TEST-4D-3: Token Mediation
# =========================================================================

class TestTokenMediation:
    """INV-4D-5 — OAuth token sovereignty (shell-mediated)."""

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_qbo_not_connected_raises_oauth_required(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": True,
            "activation_completed": True,
            "qbo_status": "OAUTH_PENDING",
        }

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "OAUTH_REQUIRED"
        assert exc_info.value.status_code == 403

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_missing_access_token_raises_oauth_required(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": True,
            "activation_completed": True,
            "qbo_status": "CONNECTED",
        }
        mock_tokens.return_value = {
            "access_token": "",
            "refresh_token": "rt",
            "qbo_realm_id": "r",
        }

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "OAUTH_REQUIRED"
        assert exc_info.value.status_code == 403

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_token_retrieval_failure_raises_oauth_required(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": True,
            "activation_completed": True,
            "qbo_status": "CONNECTED",
        }
        mock_tokens.side_effect = HarborError(
            code="TOKEN_ERROR", message="decrypt failed"
        )

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        with pytest.raises(HarborError) as exc_info:
            gate_charles_execution(UUID("00000000-0000-0000-0000-000000000001"))

        assert exc_info.value.code == "OAUTH_REQUIRED"

    @patch("shell.app.charles_gate.get_qbo_tokens")
    @patch("shell.app.charles_gate.get_activation_status")
    @patch("shell.app.charles_gate.get_db")
    def test_successful_gate_returns_wec(
        self, mock_db, mock_status, mock_tokens
    ):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("ws-1",)
        mock_db.return_value.cursor.return_value = cursor
        mock_status.return_value = {
            "entitlement_valid": True,
            "activation_completed": True,
            "qbo_status": "CONNECTED",
        }
        mock_tokens.return_value = {
            "access_token": "at-secret",
            "refresh_token": "rt",
            "qbo_realm_id": "realm-99",
        }

        from shell.app.charles_gate import gate_charles_execution
        from uuid import UUID

        wec = gate_charles_execution(
            UUID("00000000-0000-0000-0000-000000000001"),
            request_id="req-42",
        )

        assert isinstance(wec, WorkspaceExecutionContext)
        assert wec.workspace_id == "00000000-0000-0000-0000-000000000001"
        assert wec.app_id == "charles"
        assert wec.qbo_realm_id == "realm-99"
        assert wec.qbo_access_token == "at-secret"
        assert wec.request_id == "req-42"

    def test_charles_never_calls_token_refresh(self):
        """Static assertion: Charles code must not call refresh_token."""
        result = subprocess.run(
            ["grep", "-rn", "refresh_token", str(CHARLES_ROOT),
             "--include=*.py", "--exclude-dir=tests"],
            capture_output=True, text=True,
        )
        # Filter out comments and string literals that merely mention the concept
        violations = []
        for line in result.stdout.strip().splitlines():
            # Skip comments and docstrings
            stripped = line.split(":", 2)[-1].strip()
            if stripped.startswith("#") or stripped.startswith('"""'):
                continue
            # Skip imports of this test module
            if "test_phase4d" in line:
                continue
            # Skip raw_data_persistence which handles Stripe data passthrough
            if "raw_data_persistence" in line:
                continue
            # Skip database schema definitions (column names)
            if "CREATE TABLE" in line or "INSERT INTO" in line or "SELECT" in line:
                continue
            # Actual calls to refresh_token function are violations
            if "refresh_token(" in stripped:
                violations.append(line)

        assert not violations, (
            "Charles must not directly call token refresh functions:\n"
            + "\n".join(violations)
        )


# =========================================================================
# TEST-4D-4: Route Audit
# =========================================================================

class TestRouteAudit:
    """INV-4D-1 — Shell is the only entry authority."""

    def test_charles_routes_file_has_no_route_content(self):
        """apps/charles/app/routes.py must contain no route registrations."""
        routes_file = CHARLES_ROOT / "app" / "routes.py"
        content = routes_file.read_text()
        assert "@" not in content, (
            "Charles routes.py must not contain route decorators"
        )
        assert "def " not in content, (
            "Charles routes.py must not define route handler functions"
        )

    def test_charles_create_app_has_no_flask_app(self):
        """apps/charles/app/create_app.py must not define a functional Flask app."""
        create_app_file = CHARLES_ROOT / "app" / "create_app.py"
        content = create_app_file.read_text()
        assert "Flask(" not in content, (
            "Charles create_app.py must not instantiate Flask"
        )
        assert "def create_app" not in content, (
            "Charles create_app.py must not define create_app()"
        )

    def test_shell_charles_routes_file_exists(self):
        """shell/app/charles_routes.py must exist with shell-owned routes."""
        routes_file = SHELL_ROOT / "app" / "charles_routes.py"
        assert routes_file.exists(), "Shell charles_routes.py must exist"
        content = routes_file.read_text()
        assert "charles_bp" in content, "Must define charles_bp Blueprint"
        assert "/charles/sync" in content, "Must define sync route"
        assert "/charles/deposit" in content, "Must define deposit route"
        assert "/charles/status" in content, "Must define status route"

    def test_shell_create_app_registers_charles_blueprint(self):
        """shell/app/create_app.py must register charles_bp."""
        create_app_file = SHELL_ROOT / "app" / "create_app.py"
        content = create_app_file.read_text()
        assert "charles_bp" in content, (
            "Shell create_app.py must import and register charles_bp"
        )
        assert "register_blueprint(charles_bp)" in content, (
            "Shell must register charles_bp via register_blueprint()"
        )


# =========================================================================
# TEST-4D-5: No Standalone Runtime
# =========================================================================

class TestNoStandaloneRuntime:
    """INV-4D-4 — no standalone runtime."""

    def test_no_app_run_in_charles(self):
        """Charles production code must not contain app.run() or .serve()."""
        result = subprocess.run(
            ["grep", "-rn", r"app\.run\|\.serve(", str(CHARLES_ROOT),
             "--include=*.py", "--exclude-dir=tests"],
            capture_output=True, text=True,
        )
        assert not result.stdout.strip(), (
            "Charles must not contain standalone server start:\n"
            + result.stdout
        )

    def test_charles_gunicorn_conf_has_no_config(self):
        """Charles gunicorn.conf.py must not contain active configuration."""
        gunicorn_file = CHARLES_ROOT / "gunicorn.conf.py"
        content = gunicorn_file.read_text()
        assert "bind" not in content, (
            "Charles gunicorn.conf.py must not contain bind config"
        )
        assert "workers" not in content.lower().replace("sync_worker", ""), (
            "Charles gunicorn.conf.py must not contain workers config"
        )

    def test_systemd_service_is_deprecated(self):
        """harbor-charles.service must be marked deprecated."""
        service_file = REPO_ROOT / "deploy" / "systemd" / "harbor-charles.service"
        content = service_file.read_text()
        assert "DEPRECATED" in content, (
            "harbor-charles.service must be marked DEPRECATED"
        )

    def test_sync_worker_run_is_non_production(self):
        """SyncWorker.run() must be documented as non-production."""
        worker_file = CHARLES_ROOT / "shared" / "sync_worker.py"
        content = worker_file.read_text()
        # Find the run() method docstring
        run_idx = content.find("def run(self):")
        assert run_idx != -1, "SyncWorker.run() must exist"
        # Check the docstring immediately after
        run_section = content[run_idx:run_idx + 300]
        assert "NON-PRODUCTION" in run_section, (
            "SyncWorker.run() docstring must contain NON-PRODUCTION marker"
        )

    def test_build_wec_never_imported_by_charles(self):
        """Charles must never import build_wec."""
        result = subprocess.run(
            ["grep", "-rn", "build_wec", str(CHARLES_ROOT),
             "--include=*.py", "--exclude-dir=tests"],
            capture_output=True, text=True,
        )
        violations = []
        for line in result.stdout.strip().splitlines():
            content = line.split(":", 2)[-1].strip()
            # Skip comments (including docstring-style comments)
            if content.startswith("#") or content.startswith("It never"):
                continue
            violations.append(line)
        assert not violations, (
            "Charles must never import or call build_wec:\n"
            + "\n".join(violations)
        )

    def test_wec_never_instantiated_by_charles(self):
        """Charles must never directly instantiate WorkspaceExecutionContext."""
        result = subprocess.run(
            ["grep", "-rn", "WorkspaceExecutionContext(", str(CHARLES_ROOT),
             "--include=*.py", "--exclude-dir=tests"],
            capture_output=True, text=True,
        )
        assert not result.stdout.strip(), (
            "Charles must never instantiate WorkspaceExecutionContext:\n"
            + result.stdout
        )

    def test_wec_module_is_boundary_clean(self):
        """shell/core/wec.py must have no web-layer dependencies."""
        wec_file = SHELL_ROOT / "core" / "wec.py"
        content = wec_file.read_text()
        assert "from shell.app" not in content, (
            "WEC module must not import from shell.app"
        )
        assert "import flask" not in content.lower(), (
            "WEC module must not import Flask"
        )
        assert "from flask" not in content, (
            "WEC module must not import from Flask"
        )

    def test_sync_worker_execute_job_requires_wec(self):
        """SyncWorker._execute_job must require explicit wec parameter."""
        worker_file = CHARLES_ROOT / "shared" / "sync_worker.py"
        content = worker_file.read_text()
        # Verify the method signature includes wec
        assert "def _execute_job(self, job" in content
        # Find the signature line and check for wec
        import re
        match = re.search(r"def _execute_job\(self,\s*job.*?,\s*wec\)", content)
        assert match, (
            "SyncWorker._execute_job must have explicit wec parameter"
        )

    def test_no_production_entry_invokes_sync_worker_run(self):
        """No production entry point should invoke SyncWorker.run()."""
        # Check module_entry.py — the production entry surface
        module_entry = CHARLES_ROOT / "module_entry.py"
        content = module_entry.read_text()
        assert "SyncWorker" not in content or ".run()" not in content, (
            "CharlesModule must not invoke SyncWorker.run()"
        )
        # Check shell routes
        routes = SHELL_ROOT / "app" / "charles_routes.py"
        content = routes.read_text()
        assert "SyncWorker" not in content, (
            "Shell routes must not invoke SyncWorker directly"
        )


# =========================================================================
# TEST: WEC Frozen Immutability
# =========================================================================

class TestWECProperties:
    """Supplementary WEC property tests."""

    def test_wec_is_frozen(self):
        """WEC instances must be immutable (frozen dataclass)."""
        wec = _make_wec()
        with pytest.raises(AttributeError):
            wec.workspace_id = "mutated"

    def test_build_wec_produces_correct_fields(self):
        """build_wec populates all fields correctly."""
        now = datetime.now(timezone.utc)
        wec = build_wec(
            workspace_id="ws-1",
            app_id="charles",
            qbo_realm_id="realm-1",
            qbo_access_token="tok-1",
            issued_at=now,
            request_id="req-1",
        )
        assert wec.workspace_id == "ws-1"
        assert wec.app_id == "charles"
        assert wec.qbo_realm_id == "realm-1"
        assert wec.qbo_access_token == "tok-1"
        assert wec.issued_at == now
        assert wec.request_id == "req-1"

    def test_build_wec_request_id_defaults_none(self):
        """request_id defaults to None when not provided."""
        wec = build_wec(
            workspace_id="ws-1",
            app_id="charles",
            qbo_realm_id="realm-1",
            qbo_access_token="tok-1",
            issued_at=datetime.now(timezone.utc),
        )
        assert wec.request_id is None


# =========================================================================
# TEST: Logging — request_id Propagation
# =========================================================================

class TestLoggingRequestId:
    """INV-4D-7 — logging must preserve tenant traceability."""

    def test_structured_logger_includes_request_id(self):
        """StructuredLogger must include request_id in log entries."""
        import io
        from charles.shared.structured_logging import StructuredLogger, EventType
        import json

        output = io.StringIO()
        logger = StructuredLogger(
            service="test",
            workspace_id="ws-1",
            request_id="req-42",
            output=output,
        )
        logger.info(EventType.SYNC_STARTED, "test message")
        entry = json.loads(output.getvalue())
        assert entry["request_id"] == "req-42"
        assert entry["workspace_id"] == "ws-1"

    def test_structured_logger_omits_request_id_when_none(self):
        """request_id should not appear in log entry when not set."""
        import io
        from charles.shared.structured_logging import StructuredLogger, EventType
        import json

        output = io.StringIO()
        logger = StructuredLogger(
            service="test",
            workspace_id="ws-1",
            output=output,
        )
        logger.info(EventType.SYNC_STARTED, "test message")
        entry = json.loads(output.getvalue())
        assert "request_id" not in entry

    def test_get_logger_accepts_request_id(self):
        """get_logger factory must accept and pass request_id."""
        from charles.shared.structured_logging import get_logger
        logger = get_logger(
            service="test",
            workspace_id="ws-1",
            request_id="req-99",
        )
        assert logger.request_id == "req-99"

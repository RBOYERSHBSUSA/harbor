"""
Fail-Fast Configuration Loader

CONFIGURATION_MANAGEMENT_CONTRACT Compliance:
- Missing or malformed configuration MUST halt execution
- No implicit defaults in production paths
- Environment configuration must be explicit and immutable

ENVIRONMENT_ISOLATION_CONTRACT (Phase A Compliance - A2):
- Production credentials MUST NOT run in non-production environments
- Environment mismatch causes immediate termination (fail-fast)
- No silent fallback, no warnings-only - hard failure is REQUIRED

This module provides the ONLY authorized way to load configuration.
All modules MUST use this loader instead of os.getenv() with defaults.
"""

import os
import re
import sys
from typing import Optional, Dict, Any, List
from dataclasses import dataclass


class ConfigurationError(Exception):
    """Raised when required configuration is missing or invalid."""
    pass


class EnvironmentIsolationError(ConfigurationError):
    """
    Raised when production credentials are detected in non-production environment.

    This is a FATAL error per Phase A compliance requirements.
    The application MUST NOT start when this condition is detected.
    """
    pass


@dataclass(frozen=True)
class CharlesConfig:
    """
    Immutable configuration for Charles application.

    All fields are required unless explicitly marked optional.
    Configuration is loaded once at startup and cannot be modified.
    """
    # Application Environment (REQUIRED for isolation enforcement)
    app_environment: str  # Must be 'development', 'staging', or 'production'

    # Database Configuration (REQUIRED)
    master_db_path: str
    company_db_base_path: str

    # QuickBooks OAuth Configuration (REQUIRED)
    qbo_client_id: str
    qbo_client_secret: str
    qbo_redirect_uri: str
    qbo_environment: str  # Must be 'sandbox' or 'production'

    # Encryption (REQUIRED)
    encryption_key: str

    def __post_init__(self):
        """
        Validate configuration values after initialization.

        Per CONFIGURATION_MANAGEMENT_CONTRACT:
        - Invalid values must cause immediate termination
        - No silent coercion or "best effort" interpretation

        Per ENVIRONMENT_ISOLATION_CONTRACT (Phase A):
        - Production credentials MUST NOT run in non-production environments
        - Mismatch causes immediate termination (fail-fast)
        """
        # Validate application environment
        if self.app_environment not in ('development', 'staging', 'production'):
            raise ConfigurationError(
                f"CHARLES_ENV must be 'development', 'staging', or 'production', got: {self.app_environment}"
            )

        # Validate QBO environment
        if self.qbo_environment not in ('sandbox', 'production'):
            raise ConfigurationError(
                f"QBO_ENVIRONMENT must be 'sandbox' or 'production', got: {self.qbo_environment}"
            )

        # Validate paths are not empty
        if not self.master_db_path or not self.master_db_path.strip():
            raise ConfigurationError("MASTER_DB_PATH cannot be empty")

        if not self.company_db_base_path or not self.company_db_base_path.strip():
            raise ConfigurationError("COMPANY_DB_BASE_PATH cannot be empty")

        # Validate OAuth credentials are not empty
        if not self.qbo_client_id or not self.qbo_client_id.strip():
            raise ConfigurationError("QBO_CLIENT_ID cannot be empty")

        if not self.qbo_client_secret or not self.qbo_client_secret.strip():
            raise ConfigurationError("QBO_CLIENT_SECRET cannot be empty")

        if not self.qbo_redirect_uri or not self.qbo_redirect_uri.strip():
            raise ConfigurationError("QBO_REDIRECT_URI cannot be empty")

        # Validate encryption key
        if not self.encryption_key or not self.encryption_key.strip():
            raise ConfigurationError("ENCRYPTION_KEY cannot be empty")

        # =========================================================================
        # ENVIRONMENT ISOLATION ENFORCEMENT (Phase A Compliance - A2)
        # =========================================================================
        # Production credentials MUST NOT be used in non-production environments.
        # This is a HARD REQUIREMENT - no warnings, no fallbacks, immediate exit.
        # =========================================================================
        self._validate_environment_isolation()

    def _validate_environment_isolation(self) -> None:
        """
        Validate environment isolation rules.

        Per ENVIRONMENT_ISOLATION_CONTRACT (Phase A Compliance - A2):

        RULE 1: QBO environment must match application environment
                - production app MUST use production QBO
                - non-production app (development/staging) MUST use sandbox QBO

        RULE 2: Production redirect URIs cannot be used in non-production
                - Redirect URI containing production domain in dev/staging = FATAL

        RULE 3: Database paths must match environment
                - Production paths (e.g., /var/lib/charles/prod) in dev = FATAL

        Raises:
            EnvironmentIsolationError: If any isolation rule is violated
        """
        violations: List[str] = []
        is_production_app = self.app_environment == 'production'
        is_production_qbo = self.qbo_environment == 'production'

        # RULE 1: QBO environment must match application environment
        if is_production_app and not is_production_qbo:
            violations.append(
                "ISOLATION VIOLATION: CHARLES_ENV=production but QBO_ENVIRONMENT=sandbox."
                "Production application MUST use production QBO credentials."
            )

        if not is_production_app and is_production_qbo:
            violations.append(
                f"ISOLATION VIOLATION: CHARLES_ENV={self.app_environment} but QBO_ENVIRONMENT=production. "
                "Non-production environment MUST NOT use production QBO credentials."
            )

        # RULE 2: Production redirect URIs cannot be used in non-production
        # Production URIs typically contain the production domain
        production_uri_patterns = [
            r'charlestheapp\.com',
            r'charles-app\.com',
            r'api\.charles',
        ]

        if not is_production_app:
            for pattern in production_uri_patterns:
                if re.search(pattern, self.qbo_redirect_uri, re.IGNORECASE):
                    violations.append(
                        f"ISOLATION VIOLATION: CHARLES_ENV={self.app_environment} but QBO_REDIRECT_URI "
                        f"contains production domain pattern. "
                        "Non-production environment MUST NOT use production redirect URIs."
                    )
                    break

        # RULE 3: Database paths must match environment
        # Production databases are typically in /var/lib or /data paths
        production_db_patterns = [
            r'/var/lib/charles/prod',
            r'/data/charles/prod',
            r'/opt/charles/prod',
        ]

        if not is_production_app:
            for pattern in production_db_patterns:
                if re.search(pattern, self.master_db_path, re.IGNORECASE):
                    violations.append(
                        f"ISOLATION VIOLATION: CHARLES_ENV={self.app_environment} but MASTER_DB_PATH "
                        f"appears to be a production database path. "
                        "Non-production environment MUST NOT use production database."
                    )
                    break

                if re.search(pattern, self.company_db_base_path, re.IGNORECASE):
                    violations.append(
                        f"ISOLATION VIOLATION: CHARLES_ENV={self.app_environment} but COMPANY_DB_BASE_PATH "
                        f"appears to be a production database path. "
                        "Non-production environment MUST NOT use production database."
                    )
                    break

        # If any violations, raise EnvironmentIsolationError
        if violations:
            raise EnvironmentIsolationError(
                "FATAL: Environment isolation check FAILED.\n\n" +
                "\n\n".join(violations) +
                "\n\nThis is a PRODUCTION-BLOCKING error. "
                "The application CANNOT start with mismatched environment configuration."
            )


def _get_required_env(key: str) -> str:
    """
    Get a required environment variable.

    Per CONFIGURATION_MANAGEMENT_CONTRACT:
    - Missing values cause immediate termination
    - No defaults, no fallbacks, no guessing

    Args:
        key: Environment variable name

    Returns:
        Environment variable value

    Raises:
        ConfigurationError: If environment variable is not set
    """
    value = os.getenv(key)
    if value is None:
        raise ConfigurationError(
            f"Required environment variable {key} is not set. "
            "Charles cannot start without explicit configuration. "
            "Set this variable and try again."
        )
    return value


def _get_optional_env(key: str, default: str) -> str:
    """
    Get an optional environment variable with a default.

    Args:
        key: Environment variable name
        default: Default value if not set

    Returns:
        Environment variable value or default
    """
    return os.getenv(key, default)


def load_configuration() -> CharlesConfig:
    """
    Load and validate configuration from environment variables.

    Per CONFIGURATION_MANAGEMENT_CONTRACT:
    - This function MUST be called exactly once at application startup
    - Missing or invalid configuration causes immediate termination
    - No execution proceeds without complete, valid configuration

    Per ENVIRONMENT_ISOLATION_CONTRACT (Phase A):
    - Environment isolation is validated BEFORE any other operations
    - Mismatched credentials cause immediate termination

    Returns:
        Validated, immutable configuration object

    Raises:
        ConfigurationError: If any required configuration is missing or invalid
        EnvironmentIsolationError: If production credentials in non-production env
    """
    try:
        config = CharlesConfig(
            app_environment=_get_required_env('CHARLES_ENV'),
            master_db_path=_get_required_env('MASTER_DB_PATH'),
            company_db_base_path=_get_required_env('COMPANY_DB_BASE_PATH'),
            qbo_client_id=_get_required_env('QBO_CLIENT_ID'),
            qbo_client_secret=_get_required_env('QBO_CLIENT_SECRET'),
            qbo_redirect_uri=_get_required_env('QBO_REDIRECT_URI'),
            qbo_environment=_get_required_env('QBO_ENVIRONMENT'),
            encryption_key=_get_required_env('ENCRYPTION_KEY'),
        )

        return config

    except EnvironmentIsolationError as e:
        # Per ENVIRONMENT_ISOLATION_CONTRACT: Hard failure on isolation violation
        print("", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print("FATAL: ENVIRONMENT ISOLATION VIOLATION", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print("", file=sys.stderr)
        print(str(e), file=sys.stderr)
        print("", file=sys.stderr)
        print("This is a PRODUCTION-BLOCKING error.", file=sys.stderr)
        print("The application CANNOT start with mismatched environment configuration.", file=sys.stderr)
        print("", file=sys.stderr)
        print("Check the following environment variables:", file=sys.stderr)
        print("  - CHARLES_ENV (must match QBO_ENVIRONMENT)", file=sys.stderr)
        print("  - QBO_ENVIRONMENT (sandbox for dev/staging, production for prod)", file=sys.stderr)
        print("  - QBO_REDIRECT_URI (must not use production domain in dev/staging)", file=sys.stderr)
        print("  - MASTER_DB_PATH (must not use production path in dev/staging)", file=sys.stderr)
        print("=" * 70, file=sys.stderr)
        print("", file=sys.stderr)

        # Hard exit - no recovery, no retry
        sys.exit(1)

    except ConfigurationError as e:
        # Per CONFIGURATION_MANAGEMENT_CONTRACT: Hard failure on config error
        print(f"FATAL CONFIGURATION ERROR: {e}", file=sys.stderr)
        print("", file=sys.stderr)
        print("Charles requires explicit configuration via environment variables.", file=sys.stderr)
        print("No defaults are provided in production paths.", file=sys.stderr)
        print("", file=sys.stderr)
        print("Required environment variables:", file=sys.stderr)
        print("  - CHARLES_ENV (must be 'development', 'staging', or 'production')", file=sys.stderr)
        print("  - MASTER_DB_PATH", file=sys.stderr)
        print("  - COMPANY_DB_BASE_PATH", file=sys.stderr)
        print("  - QBO_CLIENT_ID", file=sys.stderr)
        print("  - QBO_CLIENT_SECRET", file=sys.stderr)
        print("  - QBO_REDIRECT_URI", file=sys.stderr)
        print("  - QBO_ENVIRONMENT (must be 'sandbox' or 'production')", file=sys.stderr)
        print("  - ENCRYPTION_KEY", file=sys.stderr)
        print("", file=sys.stderr)
        print("Application terminated.", file=sys.stderr)

        # Hard exit - no recovery, no retry
        sys.exit(1)


# Singleton instance - loaded once at module import
_config: Optional[CharlesConfig] = None


def get_config() -> CharlesConfig:
    """
    Get the application configuration.

    Configuration is loaded exactly once on first access.
    Subsequent calls return the same immutable instance.

    Returns:
        Immutable configuration object

    Raises:
        SystemExit: If configuration is invalid (via load_configuration)
    """
    global _config
    if _config is None:
        _config = load_configuration()
    return _config

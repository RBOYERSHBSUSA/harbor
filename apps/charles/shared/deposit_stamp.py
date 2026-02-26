"""
Deposit Stamp Generator

Generates idempotent CHARLES stamps for QBO Deposits per specification:
  Format: "By Charles-{last 8 chars of payout_id}"

The stamp is designed to be searchable in QBO via Advanced Search on the Memo field.
The "Charles-" prefix (with dash) differentiates from words like "Charleston".

IDEMPOTENCY GUARANTEES (LOCKED):
- The stamp MUST be generated once per logical Deposit
- The full rendered stamp string MUST be persisted internally
- Retry, replay, or idempotent execution MUST reuse the exact same stamp
- Under no circumstances may the stamp change after creation
"""

import os
import requests
from datetime import datetime, timezone
from typing import Optional, Tuple
from zoneinfo import ZoneInfo

from shared.config import get_config


# QBO timezone mapping: QBO timezone name -> IANA timezone identifier
# Based on QBO CompanyInfo.Country and QBO's timezone values
QBO_TIMEZONE_MAP = {
    # US timezones (most common for QBO users)
    "America/Los_Angeles": "America/Los_Angeles",
    "America/Denver": "America/Denver",
    "America/Chicago": "America/Chicago",
    "America/New_York": "America/New_York",
    "America/Phoenix": "America/Phoenix",
    "America/Anchorage": "America/Anchorage",
    "Pacific/Honolulu": "Pacific/Honolulu",

    # QBO-specific timezone names
    "PST": "America/Los_Angeles",
    "MST": "America/Denver",
    "CST": "America/Chicago",
    "EST": "America/New_York",
    "AST": "America/Anchorage",
    "HST": "Pacific/Honolulu",

    # Canada
    "America/Toronto": "America/Toronto",
    "America/Vancouver": "America/Vancouver",
    "America/Edmonton": "America/Edmonton",
    "America/Winnipeg": "America/Winnipeg",
    "America/Halifax": "America/Halifax",
    "America/St_Johns": "America/St_Johns",

    # UK/Europe
    "Europe/London": "Europe/London",
    "GMT": "Europe/London",

    # Australia
    "Australia/Sydney": "Australia/Sydney",
    "Australia/Melbourne": "Australia/Melbourne",
    "Australia/Brisbane": "Australia/Brisbane",
    "Australia/Perth": "Australia/Perth",
    "Australia/Adelaide": "Australia/Adelaide",

    # Fallback
    "UTC": "UTC",
}


def _get_timezone_abbreviation(tz_name: str, dt: datetime) -> str:
    """
    Get the timezone abbreviation for a given timezone at a specific datetime.

    Correctly handles DST vs standard time:
    - America/New_York in summer: EDT
    - America/New_York in winter: EST

    Args:
        tz_name: IANA timezone identifier (e.g., 'America/New_York')
        dt: The datetime to check (used for DST determination)

    Returns:
        Timezone abbreviation (e.g., 'EST', 'EDT', 'PST', 'PDT')
    """
    try:
        tz = ZoneInfo(tz_name)
        localized_dt = dt.astimezone(tz)
        return localized_dt.strftime('%Z')
    except Exception:
        # Fallback to UTC if timezone is invalid
        return 'UTC'


def fetch_qbo_company_timezone(
    access_token: str,
    realm_id: str,
    environment: str = None
) -> str:
    """
    Fetch the timezone from QBO CompanyInfo.

    Per spec: Time zone MUST be derived from QBO CompanyInfo configuration.

    Args:
        access_token: QBO OAuth access token
        realm_id: QBO company realm ID
        environment: 'sandbox' or 'production' (defaults to config)

    Returns:
        IANA timezone identifier (e.g., 'America/New_York')

    Raises:
        RuntimeError: If QBO API call fails or timezone cannot be determined
    """
    if environment is None:
        environment = get_config().qbo_environment

    if environment == 'sandbox':
        base_url = 'https://sandbox-quickbooks.api.intuit.com'
    else:
        base_url = 'https://quickbooks.api.intuit.com'

    url = f'{base_url}/v3/company/{realm_id}/companyinfo/{realm_id}?minorversion=65'

    headers = {
        'Authorization': f'Bearer {access_token}',
        'Accept': 'application/json'
    }

    response = requests.get(url, headers=headers)

    if response.status_code != 200:
        raise RuntimeError(
            f"Failed to fetch QBO CompanyInfo: HTTP {response.status_code} - {response.text}"
        )

    data = response.json()
    company_info = data.get('CompanyInfo', {})

    # QBO stores timezone in CompanyInfo.Country paired with state/locale
    # The CompanyInfo may have a direct timezone field or we derive it from address
    qbo_timezone = None

    # Try direct timezone field first (newer QBO API versions)
    if 'LegalAddr' in company_info:
        addr = company_info['LegalAddr']
        country = addr.get('Country', 'US')
        state = addr.get('CountrySubDivisionCode', '')

        # For US companies, derive timezone from state
        if country == 'US':
            qbo_timezone = _derive_us_timezone_from_state(state)
        elif country == 'CA':
            qbo_timezone = _derive_canada_timezone_from_province(state)
        elif country == 'GB':
            qbo_timezone = 'Europe/London'
        elif country == 'AU':
            qbo_timezone = _derive_australia_timezone_from_state(state)

    # Fallback to America/New_York if we can't determine
    if not qbo_timezone:
        qbo_timezone = 'America/New_York'

    # Map to IANA timezone if needed
    return QBO_TIMEZONE_MAP.get(qbo_timezone, qbo_timezone)


def _derive_us_timezone_from_state(state: str) -> str:
    """Derive US timezone from state abbreviation."""
    # Eastern Time states
    eastern = {'CT', 'DE', 'FL', 'GA', 'IN', 'KY', 'ME', 'MD', 'MA', 'MI',
               'NH', 'NJ', 'NY', 'NC', 'OH', 'PA', 'RI', 'SC', 'TN', 'VT',
               'VA', 'WV', 'DC'}
    # Central Time states
    central = {'AL', 'AR', 'IL', 'IA', 'KS', 'LA', 'MN', 'MS', 'MO', 'NE',
               'ND', 'OK', 'SD', 'TX', 'WI'}
    # Mountain Time states
    mountain = {'AZ', 'CO', 'ID', 'MT', 'NM', 'UT', 'WY'}
    # Pacific Time states
    pacific = {'CA', 'NV', 'OR', 'WA'}
    # Alaska
    alaska = {'AK'}
    # Hawaii
    hawaii = {'HI'}

    state_upper = state.upper().strip()

    if state_upper in eastern:
        return 'America/New_York'
    elif state_upper in central:
        return 'America/Chicago'
    elif state_upper in mountain:
        if state_upper == 'AZ':
            return 'America/Phoenix'  # Arizona doesn't observe DST
        return 'America/Denver'
    elif state_upper in pacific:
        return 'America/Los_Angeles'
    elif state_upper in alaska:
        return 'America/Anchorage'
    elif state_upper in hawaii:
        return 'Pacific/Honolulu'
    else:
        return 'America/New_York'  # Default to Eastern


def _derive_canada_timezone_from_province(province: str) -> str:
    """Derive Canadian timezone from province abbreviation."""
    province_upper = province.upper().strip()

    mapping = {
        'BC': 'America/Vancouver',
        'AB': 'America/Edmonton',
        'SK': 'America/Regina',
        'MB': 'America/Winnipeg',
        'ON': 'America/Toronto',
        'QC': 'America/Toronto',
        'NB': 'America/Halifax',
        'NS': 'America/Halifax',
        'PE': 'America/Halifax',
        'NL': 'America/St_Johns',
        'YT': 'America/Whitehorse',
        'NT': 'America/Yellowknife',
        'NU': 'America/Iqaluit',
    }

    return mapping.get(province_upper, 'America/Toronto')


def _derive_australia_timezone_from_state(state: str) -> str:
    """Derive Australian timezone from state abbreviation."""
    state_upper = state.upper().strip()

    mapping = {
        'NSW': 'Australia/Sydney',
        'VIC': 'Australia/Melbourne',
        'QLD': 'Australia/Brisbane',
        'WA': 'Australia/Perth',
        'SA': 'Australia/Adelaide',
        'TAS': 'Australia/Hobart',
        'ACT': 'Australia/Sydney',
        'NT': 'Australia/Darwin',
    }

    return mapping.get(state_upper, 'Australia/Sydney')


def generate_deposit_stamp(
    processor_payout_id: str
) -> str:
    """
    Generate a CHARLES deposit stamp.

    Format: "By Charles-{last 8 chars of payout_id}"

    The "Charles-" prefix (with dash) is used to differentiate from words
    like "Charleston" when searching in QBO.

    Args:
        processor_payout_id: Payout ID from processor (e.g., Stripe payout ID)

    Returns:
        Formatted stamp string (e.g., "By Charles-TFayEc12")
    """
    # Extract last 8 characters of the payout ID
    payout_suffix = processor_payout_id[-8:] if len(processor_payout_id) >= 8 else processor_payout_id

    return f"By Charles-{payout_suffix}"


def generate_deposit_memo(
    processor_payout_id: str,
    stamp: str
) -> str:
    """
    Generate the complete deposit memo combining payout ID and CHARLES stamp.

    The stamp appears on its own line for visibility in QBO.

    Args:
        processor_payout_id: Payout ID from processor (e.g., Stripe)
        stamp: Pre-generated CHARLES stamp

    Returns:
        Complete memo string for QBO Deposit PrivateNote
    """
    return f"{stamp}\nPayout: {processor_payout_id}"


class DepositStampManager:
    """
    Manages deposit stamp generation with idempotency guarantees.

    Per spec:
    - The stamp MUST be generated once per logical Deposit
    - The full rendered stamp string MUST be persisted internally
    - Retry, replay, or idempotent execution MUST reuse the exact same stamp
    """

    def __init__(self, db_connection, workspace_id: str):
        """
        Initialize the stamp manager.

        Args:
            db_connection: Database connection
            workspace_id: Workspace identifier
        """
        self._conn = db_connection
        self._workspace_id = workspace_id

    def get_or_create_stamp(
        self,
        payout_batch_id: str,
        processor_payout_id: str
    ) -> str:
        """
        Get existing stamp or create new one for a payout.

        This method implements idempotency:
        - If a stamp already exists for this payout, return it unchanged
        - If no stamp exists, generate one and persist it

        Args:
            payout_batch_id: Internal payout batch UUID
            processor_payout_id: Processor's payout ID

        Returns:
            The deposit stamp string
        """
        # Check for existing stamp
        cursor = self._conn.cursor()
        cursor.execute(
            "SELECT deposit_stamp FROM payout_batches WHERE id = ? AND workspace_id = ?",
            (payout_batch_id, self._workspace_id)
        )
        row = cursor.fetchone()

        if row and row['deposit_stamp']:
            # Return existing stamp (idempotent)
            return row['deposit_stamp']

        # Generate new stamp using the payout ID
        stamp = generate_deposit_stamp(processor_payout_id)

        # Persist stamp atomically
        cursor.execute(
            """UPDATE payout_batches
               SET deposit_stamp = ?, updated_at = CURRENT_TIMESTAMP
               WHERE id = ? AND workspace_id = ? AND deposit_stamp IS NULL""",
            (stamp, payout_batch_id, self._workspace_id)
        )
        self._conn.commit()

        # Verify persistence (concurrent write protection)
        cursor.execute(
            "SELECT deposit_stamp FROM payout_batches WHERE id = ? AND workspace_id = ?",
            (payout_batch_id, self._workspace_id)
        )
        row = cursor.fetchone()

        return row['deposit_stamp'] if row else stamp

"""
request.py

Handles requests using the WorldCat Metadata API.
Provides functions to fetch and save XML records, retrieve holdings, and apply retry logic.
"""

from bookops_worldcat import WorldcatAccessToken
from bookops_worldcat import MetadataSession
import os
import json
import configparser
import threading
import time
import re
from worldcat_quota import WorldCatDailyQuotaError, reserve_requests


class UserStopRequestedError(RuntimeError):
    """Raised when the user asks the current fetch run to stop."""


def _friendly_error_message(exc) -> str:
    """Return a concise, user-friendly reason for a failed fetch.

    Attempts to surface HTTP status codes, authentication/rate-limit issues,
    timeouts, and common network failures in plain language.
    """
    status = _extract_status_code(exc)

    msg = str(exc) if exc else ""
    low = msg.lower()

    if isinstance(exc, WorldCatDailyQuotaError):
        return msg
    if isinstance(exc, UserStopRequestedError):
        return msg

    if status is not None:
        if status == 401:
            return "Authentication failed (401). Check API key/secret/scope in config.ini."
        if status == 403:
            return "Access forbidden (403). Scope/permissions may be insufficient."
        if status == 404:
            return "Record not found (404). The OCN may be invalid or not available."
        if status == 429:
            return "Rate limited (429). Too many requests — please try again later."
        if 500 <= status <= 599:
            return f"WorldCat service error ({status}). Try again later."
        return f"HTTP error ({status})."

    # No explicit status — try to infer from message
    if "timed out" in low or "timeout" in low:
        return "Network timeout contacting WorldCat."
    if "ssl" in low:
        return "SSL/TLS connection problem."
    if "connection" in low and ("refused" in low or "reset" in low or "aborted" in low):
        return "Network connection error."
    if "oclc number" in low and "invalid" in low:
        return "Invalid OCN format."

    # Fallback to raw exception text
    return msg or "Unknown error"


# Read the configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Retrieve key, secret and scopes from the configuration file
worldcat_config = config['WorldCat']
key = worldcat_config.get('key')
secret = worldcat_config.get('secret')
scope = worldcat_config.get('scope')
# Optional: custom User-Agent to identify the application to OCLC
agent = worldcat_config.get('agent')

# Build token using credentials from config.ini.
# Include agent so token requests also identify the application.
token = WorldcatAccessToken(
    key=key,
    secret=secret,
    scopes=scope,
    agent=agent,
)

# Configure the MetadataSession without transport-level retries.
# Retries are handled explicitly below so every retry attempt still passes
# through the global rate limiter and daily quota tracker.
SESSION_CONFIG = {
    "authorization": token,
    "timeout": 30,
    "agent": agent,  # identify the app on all Metadata API calls
}

# Pre-create output directories once to avoid repeating os.makedirs in hot paths
REQUESTED_DIR = os.path.join('OCNrecords', 'requested')
HOLDINGS_DIR = os.path.join('OCNrecords', 'requested_holdings')
os.makedirs(REQUESTED_DIR, exist_ok=True)
os.makedirs(HOLDINGS_DIR, exist_ok=True)

# Thread-local storage for reusing a MetadataSession per worker thread
_thread_local = threading.local()
_token_refresh_lock = threading.Lock()

class SimpleRateLimiter:
    """A simple thread-safe rate limiter that ensures a minimum interval between calls."""
    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.lock = threading.Lock()
        self.next_allowed_time = 0.0

    def wait(self, stop_event=None):
        if self.interval <= 0:
            return
        with self.lock:
            now = time.monotonic()
            wait_time = self.next_allowed_time - now
            while wait_time > 0:
                if stop_event is not None and stop_event.is_set():
                    raise UserStopRequestedError("Fetching stopped by user.")
                sleep_chunk = min(wait_time, 0.1)
                time.sleep(sleep_chunk)
                now = time.monotonic()
                wait_time = self.next_allowed_time - now
            self.next_allowed_time = now + self.interval

# API RATE LIMITER: match the published per-key limit.
API_RATE_LIMITER = SimpleRateLimiter(2.0)
MAX_REQUEST_ATTEMPTS = 4


def _extract_status_code(exc) -> int | None:
    """Best-effort extraction of an HTTP status code from wrapped request errors."""
    try:
        status = getattr(exc, 'status_code', None) or getattr(getattr(exc, 'response', None), 'status_code', None)
        if status is not None:
            return int(status)
    except Exception:
        pass

    match = re.search(r"\b([1-5]\d{2})\b", str(exc))
    if match:
        try:
            return int(match.group(1))
        except ValueError:
            return None
    return None


def _is_auth_error(exc) -> bool:
    status = _extract_status_code(exc)
    if status == 401:
        return True

    message = str(exc).lower()
    return "401" in message or "unauthorized" in message


def _sync_session_authorization(session):
    """Keep a worker session's Authorization header aligned with the shared token."""
    try:
        session.headers.update({"Authorization": f"Bearer {session.authorization.token_str}"})
    except Exception:
        pass


def _refresh_session_authorization(session, force: bool = False):
    """Refresh the shared token once and update the current session header."""
    with _token_refresh_lock:
        expected_header = f"Bearer {session.authorization.token_str}"
        current_header = session.headers.get("Authorization")
        should_request_new_token = session.authorization.is_expired()

        if force and current_header == expected_header:
            should_request_new_token = True

        if should_request_new_token:
            session._get_new_access_token()
        _sync_session_authorization(session)


def _sleep_with_stop(delay_seconds: float, stop_event=None):
    remaining = max(0.0, float(delay_seconds))
    while remaining > 0:
        _check_stop_requested(stop_event)
        sleep_chunk = min(remaining, 0.1)
        time.sleep(sleep_chunk)
        remaining -= sleep_chunk


def _is_retryable_request_error(exc) -> bool:
    """Return True for transient failures worth retrying under our own limiter."""
    status = _extract_status_code(exc)
    if status in {429, 500, 502, 503, 504}:
        return True

    message = str(exc).lower()
    return any(fragment in message for fragment in (
        "rate limit",
        "timed out",
        "timeout",
        "connection error",
        "connection aborted",
        "connection reset",
        "connection refused",
        "temporarily unavailable",
    ))


def _retry_delay_seconds(exc, attempt_number: int) -> float:
    """Back off more aggressively after rate limits than after generic transient errors."""
    status = _extract_status_code(exc)
    if status == 429 or "rate limit" in str(exc).lower():
        return min(30.0, 2.0 * (2 ** (attempt_number - 1)))
    return min(10.0, 0.75 * (2 ** (attempt_number - 1)))


def _check_stop_requested(stop_event=None):
    if stop_event is not None and stop_event.is_set():
        raise UserStopRequestedError("Fetching stopped by user.")


def _prepare_api_call(stop_event=None):
    """Apply both short-term pacing and the persisted daily quota gate."""
    _check_stop_requested(stop_event)
    API_RATE_LIMITER.wait(stop_event=stop_event)
    _check_stop_requested(stop_event)
    reserve_requests(1)


def _perform_api_call(api_call, session=None, stop_event=None):
    """Run an API call with quota-aware retries under the global rate limiter."""
    last_error = None
    auth_retry_used = False
    for attempt in range(1, MAX_REQUEST_ATTEMPTS + 1):
        _check_stop_requested(stop_event)
        _prepare_api_call(stop_event=stop_event)
        if session is not None:
            _sync_session_authorization(session)
        try:
            return api_call()
        except Exception as exc:
            if isinstance(exc, (WorldCatDailyQuotaError, UserStopRequestedError)):
                raise
            last_error = exc
            if session is not None and _is_auth_error(exc) and not auth_retry_used:
                auth_retry_used = True
                _refresh_session_authorization(session, force=True)
                continue
            if attempt >= MAX_REQUEST_ATTEMPTS or not _is_retryable_request_error(exc):
                raise
            _sleep_with_stop(_retry_delay_seconds(exc, attempt), stop_event=stop_event)

    if last_error is not None:
        raise last_error


def _get_session():
    """Return a thread-local MetadataSession instance for connection reuse."""
    session = getattr(_thread_local, 'session', None)
    if session is None:
        session = MetadataSession(**SESSION_CONFIG)
        # Ensure User-Agent header is set (belt-and-suspenders)
        try:
            session.headers.update({"User-Agent": agent})
        except Exception:
            try:
                # Some clients expose underlying requests.Session
                session.session.headers["User-Agent"] = agent  # type: ignore[attr-defined]
            except Exception:
                pass
        _thread_local.session = session
    _sync_session_authorization(session)
    return session


def fetch_marcxmldata(ocn, stop_event=None):
    """Fetch XML data for an OCN using the WorldCat Metadata API. Returns bytes."""
    session = _get_session()
    response = _perform_api_call(
        lambda: session.bib_get(ocn),
        session=session,
        stop_event=stop_event,
    )
    return response.content  # bytes


def save_marcxmldata(ocn, ocn_record_bytes):
    """Save XML data to 'OCNrecords/requested' as bytes."""
    filename = f"{ocn}.xml"
    directory = os.path.join('OCNrecords', 'requested')
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    with open(filepath, 'wb') as file:
        file.write(ocn_record_bytes)


def fetch_holdingsdata(ocn, held_by_symbols=None, stop_event=None):
    """Fetch holdings for an OCN for each specific institution symbol.

    The user requires individual holdings data for each symbol.
    """
    if held_by_symbols is None:
        held_by_symbols = ['QGE', 'QGK', 'NLTUD', 'NETUE', 'QGQ', 'L2U', 'NLMAA', 'GRG', 'GRU', 'QHU', 'QGJ', 'VU@', 'WURST']

    combined_holdings = {
        'ocn': ocn,
        'holdings': []
    }

    session = _get_session()

    # Per-symbol requests as required for individual holdings
    for symbol in held_by_symbols:
        try:
            _check_stop_requested(stop_event)
            response = _perform_api_call(
                lambda current_symbol=symbol: session.summary_holdings_get(
                    oclcNumber=ocn,
                    heldBySymbol=current_symbol,
                ),
                session=session,
                stop_event=stop_event,
            )
            holdings = response.json()
            holdings_data = {
                'institutionSymbol': symbol,
                'totalHoldingCount': holdings.get('totalHoldingCount', 0),
                'totalSharedPrintCount': holdings.get('totalSharedPrintCount', 0),
                'totalEditions': holdings.get('totalEditions', 0)
            }
            combined_holdings['holdings'].append(holdings_data)
        except Exception as e:
            # Check for 429 rate limiting in individual requests
            if isinstance(e, WorldCatDailyQuotaError):
                raise e
            if isinstance(e, UserStopRequestedError):
                raise e
            status = _extract_status_code(e)
            if status == 429:
                # If we're hitting 429 even with retries, we might want to re-raise
                # so the caller can decide whether to abort.
                raise e
            # Otherwise log error for this symbol and continue
            holdings_data = {
                'institutionSymbol': symbol,
                'error': str(e)
            }
            combined_holdings['holdings'].append(holdings_data)

    return combined_holdings


def save_holdingsdata(ocn, holdings_data):
    """Save holdings data as JSON in 'OCNrecords/requested_holdings' without pretty printing."""
    filename = f"{ocn}_holdings.json"
    directory = os.path.join('OCNrecords', 'requested_holdings')
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', encoding='utf-8') as file:
        json.dump(holdings_data, file, ensure_ascii=False)


def fetch_and_save_data(
    ocn,
    fetch_holdings,
    reporter=None,
    stop_event=None,
    report_existing_xml=True,
    report_existing_json=True,
):
    """
    Fetch and save the record with the given OCN and optionally fetch holdings.
    Uses the functions above. Reports sub-step completion via `reporter` if provided.
    Existing local files can be excluded from reporter events so the caller can
    pre-seed progress from disk without double counting cached records.
    Returns (ocn, error or None).
    """
    # Precompute target filepaths using constants
    xml_filepath = os.path.join(REQUESTED_DIR, f'{ocn}.xml')
    holdings_filepath = os.path.join(HOLDINGS_DIR, f'{ocn}_holdings.json')
    _check_stop_requested(stop_event)

    # If XML already exists, we can still fetch holdings if requested
    if os.path.exists(xml_filepath):
        try:
            # Report XML already done
            if reporter is not None and report_existing_xml:
                try:
                    reporter.xml_done(ocn)
                except Exception:
                    pass
            # Handle holdings
            if fetch_holdings:
                if os.path.exists(holdings_filepath):
                    if reporter is not None and report_existing_json:
                        try:
                            reporter.json_done(ocn)
                        except Exception:
                            pass
                else:
                    _check_stop_requested(stop_event)
                    holdings = fetch_holdingsdata(ocn, stop_event=stop_event)
                    if holdings:
                        save_holdingsdata(ocn, holdings)
                        if reporter is not None:
                            try:
                                reporter.json_done(ocn)
                            except Exception:
                                pass
            return ocn, None
        except Exception as e:
            return ocn, _friendly_error_message(e)

    # Fetch the record and save it to the requested folder
    try:
        _check_stop_requested(stop_event)
        ocn_record_bytes = fetch_marcxmldata(ocn, stop_event=stop_event)
        save_marcxmldata(ocn, ocn_record_bytes)
        if reporter is not None:
            try:
                reporter.xml_done(ocn)
            except Exception:
                pass

        # Fetch holdings if the checkbox was selected and holdings not already fetched
        if fetch_holdings:
            if os.path.exists(holdings_filepath):
                if reporter is not None and report_existing_json:
                    try:
                        reporter.json_done(ocn)
                    except Exception:
                        pass
            else:
                _check_stop_requested(stop_event)
                holdings = fetch_holdingsdata(ocn, stop_event=stop_event)
                if holdings:
                    save_holdingsdata(ocn, holdings)
                    if reporter is not None:
                        try:
                            reporter.json_done(ocn)
                        except Exception:
                            pass

        return ocn, None

    except Exception as e:
        return ocn, _friendly_error_message(e)


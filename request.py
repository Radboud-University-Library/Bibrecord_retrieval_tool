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


def _friendly_error_message(exc) -> str:
    """Return a concise, user-friendly reason for a failed fetch.

    Attempts to surface HTTP status codes, authentication/rate-limit issues,
    timeouts, and common network failures in plain language.
    """
    # Try to extract HTTP status code if available
    status = None
    try:
        status = getattr(exc, 'status_code', None) or getattr(getattr(exc, 'response', None), 'status_code', None)
    except Exception:
        status = None

    msg = str(exc) if exc else ""
    low = msg.lower()

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

# Configure the MetadataSession with automatic retry functionality
# Include 429 for rate limiting and a slightly higher backoff for stability
SESSION_CONFIG = {
    "authorization": token,
    "timeout": 30,
    "totalRetries": 5,
    "backoffFactor": 0.5,
    "statusForcelist": [429, 500, 502, 503, 504],
    "allowedMethods": ["GET"],
    "agent": agent,  # identify the app on all Metadata API calls
}

# Pre-create output directories once to avoid repeating os.makedirs in hot paths
REQUESTED_DIR = os.path.join('OCNrecords', 'requested')
HOLDINGS_DIR = os.path.join('OCNrecords', 'requested_holdings')
os.makedirs(REQUESTED_DIR, exist_ok=True)
os.makedirs(HOLDINGS_DIR, exist_ok=True)

# Thread-local storage for reusing a MetadataSession per worker thread
_thread_local = threading.local()

class SimpleRateLimiter:
    """A simple thread-safe rate limiter that ensures a minimum interval between calls."""
    def __init__(self, requests_per_second: float):
        self.interval = 1.0 / requests_per_second if requests_per_second > 0 else 0
        self.lock = threading.Lock()
        self.next_allowed_time = 0.0

    def wait(self):
        if self.interval <= 0:
            return
        with self.lock:
            now = time.monotonic()
            wait_time = self.next_allowed_time - now
            if wait_time > 0:
                time.sleep(wait_time)
                now = time.monotonic()
            self.next_allowed_time = now + self.interval

# API RATE LIMITER: Enforces a strict requests-per-second limit.
# WorldCat API keys are often limited to 2 requests per second.
API_RATE_LIMITER = SimpleRateLimiter(2.0)


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
    return session


def fetch_marcxmldata(ocn):
    """Fetch XML data for an OCN using the WorldCat Metadata API. Returns bytes."""
    session = _get_session()
    # Global per-key pacing gate (2 requests/second).
    API_RATE_LIMITER.wait()
    response = session.bib_get(ocn)
    return response.content  # bytes


def save_marcxmldata(ocn, ocn_record_bytes):
    """Save XML data to 'OCNrecords/requested' as bytes."""
    filename = f"{ocn}.xml"
    directory = os.path.join('OCNrecords', 'requested')
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)
    with open(filepath, 'wb') as file:
        file.write(ocn_record_bytes)


def fetch_holdingsdata(ocn, held_by_symbols=None):
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
            # Global per-key pacing gate (2 requests/second).
            API_RATE_LIMITER.wait()
            response = session.summary_holdings_get(
                oclcNumber=ocn,
                heldBySymbol=symbol,
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
            status = getattr(e, 'status_code', None) or getattr(getattr(e, 'response', None), 'status_code', None)
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


def fetch_and_save_data(ocn, fetch_holdings, reporter=None):
    """
    Fetch and save the record with the given OCN and optionally fetch holdings.
    Uses the functions above. Reports sub-step completion via `reporter` if provided.
    Returns (ocn, error or None).
    """
    # Precompute target filepaths using constants
    xml_filepath = os.path.join(REQUESTED_DIR, f'{ocn}.xml')
    holdings_filepath = os.path.join(HOLDINGS_DIR, f'{ocn}_holdings.json')

    # If XML already exists, we can still fetch holdings if requested
    if os.path.exists(xml_filepath):
        try:
            # Report XML already done
            if reporter is not None:
                try:
                    reporter.xml_done(ocn)
                except Exception:
                    pass
            # Handle holdings
            if fetch_holdings:
                if os.path.exists(holdings_filepath):
                    if reporter is not None:
                        try:
                            reporter.json_done(ocn)
                        except Exception:
                            pass
                else:
                    holdings = fetch_holdingsdata(ocn)
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
        ocn_record_bytes = fetch_marcxmldata(ocn)
        save_marcxmldata(ocn, ocn_record_bytes)
        if reporter is not None:
            try:
                reporter.xml_done(ocn)
            except Exception:
                pass

        # Fetch holdings if the checkbox was selected and holdings not already fetched
        if fetch_holdings:
            if os.path.exists(holdings_filepath):
                if reporter is not None:
                    try:
                        reporter.json_done(ocn)
                    except Exception:
                        pass
            else:
                holdings = fetch_holdingsdata(ocn)
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


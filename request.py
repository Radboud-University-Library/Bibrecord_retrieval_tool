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

# Insert key and secret here
# Include agent so token requests also identify the application
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
    # Try with timeout; if client doesn't support, fall back without
    try:
        response = session.bib_get(ocn, timeout=30)
    except TypeError:
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
    """Fetch general holdings for an OCN filtered by specific institution symbols.

    Optimization: If API supports multiple heldBySymbol values at once, try a single batched
    request by passing a list of symbols. If the response does not provide per-institution
    counts, fall back to per-symbol requests to preserve output structure.
    """
    if held_by_symbols is None:
        held_by_symbols = ['QGE', 'QGK', 'NLTUD', 'NETUE', 'QGQ', 'L2U', 'NLMAA', 'GRG', 'GRU', 'QHU', 'QGJ', 'VU@', 'WURST']

    combined_holdings = {
        'ocn': ocn,
        'holdings': []
    }

    session = _get_session()

    # Attempt batched request first (array[string] for heldBySymbol)
    try:
        try:
            batched_resp = session.summary_holdings_get(
                oclcNumber=ocn,
                heldBySymbol=held_by_symbols,
                timeout=30,
            )
        except TypeError:
            batched_resp = session.summary_holdings_get(
                oclcNumber=ocn,
                heldBySymbol=held_by_symbols,
            )
        data = batched_resp.json()
        # Try to detect per-institution breakdown commonly used in APIs
        institutions = None
        if isinstance(data, dict):
            # Possible keys depending on API representation
            for key in ("institutions", "holdingInstitutions", "libraries", "resultsByInstitution"):
                if key in data and isinstance(data[key], list):
                    institutions = data[key]
                    break
        if institutions is not None:
            for inst in institutions:
                symbol = inst.get('institutionSymbol') or inst.get('symbol') or inst.get('oclcSymbol')
                holdings_data = {
                    'institutionSymbol': symbol or 'UNKNOWN',
                    'totalHoldingCount': inst.get('totalHoldingCount', data.get('totalHoldingCount', 0)),
                    'totalSharedPrintCount': inst.get('totalSharedPrintCount', data.get('totalSharedPrintCount', 0)),
                    'totalEditions': inst.get('totalEditions', data.get('totalEditions', 0)),
                }
                combined_holdings['holdings'].append(holdings_data)
            return combined_holdings
        # If no per-institution breakdown available, fall back to per-symbol loop
    except Exception:
        # On any error in batched attempt, fall back to per-symbol requests
        pass

    # Per-symbol fallback
    for symbol in held_by_symbols:
        try:
            response = session.summary_holdings_get(
                oclcNumber=ocn,
                heldBySymbol=symbol,
                timeout=30,
            )
        except TypeError:
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


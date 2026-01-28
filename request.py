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


# Read configuration file
config = configparser.ConfigParser()
config.read('config.ini')

# Retrieve key, secret and scopes from the configuration file
worldcat_config = config['WorldCat']
key = worldcat_config.get('key')
secret = worldcat_config.get('secret')
scope = worldcat_config.get('scope')

# Insert key and secret here
token = WorldcatAccessToken(
    key=key,
    secret=secret,
    scopes=scope,
)

# Configure the MetadataSession with automatic retry functionality
# Include 429 for rate limiting and a slightly higher backoff for stability
SESSION_CONFIG = {
    "authorization": token,
    "totalRetries": 5,
    "backoffFactor": 0.5,
    "statusForcelist": [429, 500, 502, 503, 504],
    "allowedMethods": ["GET"],
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


def fetch_and_save_data(ocn, fetch_holdings):
    """
    Fetch and save the record with the given OCN and optionally fetch holdings.
    Uses the functions above.
    """
    # Precompute target filepaths using constants
    xml_filepath = os.path.join(REQUESTED_DIR, f'{ocn}.xml')
    holdings_filepath = os.path.join(HOLDINGS_DIR, f'{ocn}_holdings.json')

    # If XML already exists, we can still fetch holdings if requested
    if os.path.exists(xml_filepath):
        try:
            if fetch_holdings and not os.path.exists(holdings_filepath):
                holdings = fetch_holdingsdata(ocn)
                if holdings:
                    save_holdingsdata(ocn, holdings)
            return ocn, None
        except Exception as e:
            return ocn, str(e)

    # Fetch the record and save it to the requested folder
    try:
        ocn_record_bytes = fetch_marcxmldata(ocn)
        save_marcxmldata(ocn, ocn_record_bytes)

        # Fetch holdings if the checkbox was selected and holdings not already fetched
        if fetch_holdings and not os.path.exists(holdings_filepath):
            holdings = fetch_holdingsdata(ocn)
            if holdings:
                save_holdingsdata(ocn, holdings)

        return ocn, None

    except Exception as e:
        return ocn, str(e)


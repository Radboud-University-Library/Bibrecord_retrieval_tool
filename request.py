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
SESSION_CONFIG = {
    "authorization": token,
    "totalRetries": 3,
    "backoffFactor": 0.1,
    "statusForcelist": [500, 502, 503, 504],
    "allowedMethods": ["GET"],
}


def fetch_marcxmldata(ocn):
    """Fetch XML data for an OCN using the WorldCat Metadata API."""
    with MetadataSession(**SESSION_CONFIG) as session:
        response = session.bib_get(ocn)
        return response.content.decode('utf-8')


def save_marcxmldata(ocn, ocn_record):
    """Save XML data to 'OCNrecords/requested'."""
    filename = f"{ocn}.xml"
    directory = 'OCNrecords/requested'
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join('OCNrecords', 'requested', filename)
    with open(filepath, 'w', encoding='utf-8') as file:
        file.write(ocn_record)


def fetch_holdingsdata(ocn, held_by_symbols=None):
    """Fetch general holdings for an OCN filtered by specific institution symbols."""
    if held_by_symbols is None:
        held_by_symbols = ['QGE', 'QGK', 'NLTUD', 'NETUE', 'QGQ', 'L2U', 'NLMAA', 'GRG', 'GRU', 'QHU', 'QGJ', 'VU@', 'WURST']

    combined_holdings = {
        'ocn': ocn,
        'holdings': []
    }

    with MetadataSession(**SESSION_CONFIG) as session:
        for symbol in held_by_symbols:
            response = session.summary_holdings_get(
                oclcNumber=ocn,
                heldBySymbol=symbol
            )
            holdings = response.json()

            # Create a dictionary for each institution
            holdings_data = {
                'institutionSymbol': symbol,
                'totalHoldingCount': holdings.get('totalHoldingCount', 0),
                'totalSharedPrintCount': holdings.get('totalSharedPrintCount', 0),
                'totalEditions': holdings.get('totalEditions', 0)
            }

            combined_holdings['holdings'].append(holdings_data)

    return combined_holdings


def save_holdingsdata(ocn, holdings_data):
    """Save holdings data as JSON in 'OCNrecords/requested_holdings'."""
    filename = f"{ocn}_holdings.json"
    directory = 'OCNrecords/requested_holdings'
    os.makedirs(directory, exist_ok=True)
    filepath = os.path.join(directory, filename)

    with open(filepath, 'w', encoding='utf-8') as file:
        json.dump(holdings_data, file, ensure_ascii=False, indent=4)


def fetch_and_save_data(ocn, fetch_holdings):
    """
    Fetch and save the record with the given OCN and optionally fetch holdings.
    Uses the functions above.
    """
    # Check if the record has already been fetched
    xml_filepath = os.path.join('OCNrecords/requested', f'{ocn}.xml')
    holdings_filepath = os.path.join('OCNrecords/requested_holdings', f'{ocn}_holdings.json')

    # Skip fetching if the file already exists
    if os.path.exists(xml_filepath):
        return ocn, None

    # Fetch the record and save it to the requested folder
    try:
        ocn_record = fetch_marcxmldata(ocn)
        save_marcxmldata(ocn, ocn_record)

        # Fetch holdings if the checkbox was selected and holdings not already fetched
        if fetch_holdings and not os.path.exists(holdings_filepath):
            holdings = fetch_holdingsdata(ocn)
            if holdings:
                save_holdingsdata(ocn, holdings)

        return ocn, None

    except Exception as e:
        return ocn, str(e)


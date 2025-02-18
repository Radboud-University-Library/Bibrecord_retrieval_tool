# Bibrecord Retrieval Tool

This project provides a Streamlit application for uploading a CSV file containing OCLC numbers, fetching corresponding records from the WorldCat Metadata API, and exporting the data to Excel.

## Features

- Upload a CSV file with OCLC numbers.
- Fetch MARCXML data for each OCLC number.
- Optionally fetch (UKB) holdings data for each OCLC number.
- Export MARCXML and JSON data to Excel files.
- Merge Excel files containing MARCXML and JSON data.
- Save all XML files to a ZIP archive.

## Requirements

- Python 3.12 or higher
- Streamlit 1.37.0
- Pandas 2.2.2
- Openpyxl 3.2.0b1
- Requests 2.32.3
- BookOps WorldCat 1.0.1

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/Feelingthefoo/bibrecord-retrieval-tool.git
    cd bibrecord-retrieval-tool
    ```

2. Create a virtual environment and activate it:
    ```sh
    python -m venv venv
    source venv/bin/activate  # On Windows use `venv\Scripts\activate`
    ```

3. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

4. Create a `config.ini` file with your WorldCat API credentials:
    ```ini
    [WorldCat]
    key=your_key_here
    secret=your_secret_here
    scope=WorldCatMetadataAPI
    ```

## Usage

1. Run the Streamlit application:
    ```sh
    streamlit run main.py
    ```

2. Upload a CSV file containing an `OCLC Number` column.

3. Optionally, check the "Fetch Holdings Data" checkbox to fetch holdings data.

4. Click the "Fetch Record Data" button to start fetching data. Click again if records fail.

5. Once the data is fetched, use the export buttons to save the data to Excel or ZIP files.

## Project Structure

- `main.py`: The main Streamlit application.
- `request.py`: Handles API requests to the WorldCat Metadata API.
- `export.py`: Handles exporting data to Excel and ZIP files.
- `utils.py`: Utility functions for processing data.
- `config.ini`: Configuration file for API credentials (not included in the repository).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details.
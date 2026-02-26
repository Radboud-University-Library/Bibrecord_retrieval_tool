# Bibrecord Retrieval Tool

This project provides a Streamlit application for uploading a CSV file containing OCLC numbers, fetching corresponding records from the WorldCat Metadata API, and exporting the data to Excel.

## Features

- Upload a CSV file with OCLC numbers (expects a column named `OCLC Number`).
- Fetch MARCXML data for each OCLC number with a debounced progress bar that shows percentage, counts, and ETA.
- Stop long-running retrieval at any time with a Stop button that appears during processing.
- Optionally fetch holdings data for other UKB libraries (you can adjust symbols in the fetch_holdingsdata function in `request.py`).
  - ['QGE', 'QGK', 'NLTUD', 'NETUE', 'QGQ', 'L2U', 'NLMAA', 'GRG', 'GRU', 'QHU', 'QGJ', 'VU@', 'WURST']
- Step-by-step export flow:
  - Step 2: Generate Excel (XML + optional JSON holdings) with a status stepper and progress text per phase.
  - Step 3: Download Final Excel appears only after the export successfully completes and the file exists.
- Optional, clearly separate action: Save raw MARCXML to a ZIP on your Desktop.
- Compact error reporting via an expandable "Summary of Errors" with a count.

## Requirements

- Python 3.12 or higher
- Streamlit 1.37.0
- Pandas 2.2.2
- openpyxl 3.2.0b1
- requests 2.32.3
- bookops-worldcat 1.0.1

## Installation

1. Clone the repository:
    ```sh
    git clone https://github.com/Feelingthefoo/bibrecord-retrieval-tool.git
    ```

2. Install the required dependencies:
    ```sh
    pip install -r requirements.txt
    ```

3. Create a `config.ini` file with your WorldCat API credentials:
    ```ini
    [WorldCat]
    key=your_key_here
    secret=your_secret_here
    scope=WorldCatMetadataAPI
    agent=bibrecord-retrieval-tool
    ```

## Usage

1. Run the Streamlit application:
    ```sh
    streamlit run main.py
    ```

2. Upload a CSV file containing an `OCLC Number` column (CSV is expected to use `;` as delimiter in the sample workflow).

3. Optionally, check the "Fetch Holdings Data" checkbox to also retrieve holdings.

4. Click "Step 1: Fetch Records" to start retrieval. While running:
   - A Stop button appears next to Step 1 so you can cancel.
   - The progress bar shows percentage, items completed/total, and ETA.
   - Any issues encountered will be summarized later in an expandable error section.

5. After all records are retrieved and saved successfully:
   - Click "Step 2: Generate Excel (XML + optional JSON holdings)". The app will export XML, optionally export JSON holdings (if present), and merge them. A status panel describes each step.
   - When the export completes, "Step 3: Download Final Excel" appears. Click it to download the generated file (e.g., `final_data.xlsx`).

6. Optional: Use "Save raw MARCXML to ZIP (Desktop)" to archive all raw XML files to a ZIP on your Desktop. This is separate from the step-by-step export.

## Project Structure

- `main.py`: The main Streamlit application and UI flow (Step 1/2/3, Stop state handling).
- `request.py`: Handles API requests to the WorldCat Metadata API.
- `export.py`: Handles exporting data to Excel and ZIP files and merging XML/JSON outputs.
- `utils.py`: Processing loop with debounced progress + ETA, error summary, and export UI helpers.
- `config.ini`: Configuration file for API credentials (not included in the repository).

## License

This project is licensed under the MIT License - see the [LICENSE](LICENSE.txt) file for details.
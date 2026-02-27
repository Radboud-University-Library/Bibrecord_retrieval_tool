"""
Bibrecord Retrieval Tool

This module provides a Streamlit application for uploading a CSV file containing OCLC numbers,
fetching corresponding records, and exporting the data to Excel.

Functions:
- main(): The main function that sets up the Streamlit interface and handles user interactions.
"""

import streamlit as st
import pandas as pd
from datetime import datetime
from utils import process_data, update_session_state, show_export_buttons, verify_required_files

DEFAULT_WORKERS = 4


def main():
    """Main function to set up the Streamlit interface and handle user interactions."""
    st.title("Bibrecord Retrieval Tool")

    # Upload CSV file
    uploaded_file = st.file_uploader("Upload a CSV file", type="csv")
    data_frame = pd.DataFrame()
    csv_uploaded = False

    # Load the CSV data if a file is uploaded
    if uploaded_file is not None:
        data_frame = pd.read_csv(uploaded_file, delimiter=";")
        if 'OCLC Number' not in data_frame.columns:
            st.error("The uploaded CSV file must contain an 'OCLC Number' column.")
            return
        data_frame['OCLC Number'] = data_frame['OCLC Number'].astype(str)
        st.write("Preview of Uploaded CSV:")
        st.dataframe(data_frame.head())
        csv_uploaded = True

    # Checkbox to indicate whether to fetch holdings
    fetch_holdings = False
    if csv_uploaded:
        fetch_holdings = st.checkbox("Fetch Holdings Data", value=False)

    # Initialize session state variables if they don't exist
    if 'error_list' not in st.session_state:
        st.session_state.error_list = []
    if 'all_fetched' not in st.session_state:
        st.session_state.all_fetched = False
    if 'all_saved' not in st.session_state:
        st.session_state.all_saved = False

    # Main workflow for fetching and saving records
    if csv_uploaded:
        # Initialize Stop flag if not present
        if 'stop' not in st.session_state:
            st.session_state.stop = False

        # Initialize processing flag if not present
        if 'processing' not in st.session_state:
            st.session_state.processing = False

        col1, col2 = st.columns(2)
        with col1:
            start_clicked = st.button("Step 1: Fetch Records", disabled=st.session_state.get('processing', False))
        with col2:
            if st.session_state.get('processing', False):
                if st.button("Stop"):
                    st.session_state.stop = True
                    st.session_state.processing = False
                    try:
                        st.rerun()
                    except Exception:
                        st.experimental_rerun()

        # If the Start button was clicked, set processing state and rerun so the Stop button appears immediately
        if start_clicked and not st.session_state.get('processing', False):
            st.session_state.stop = False
            st.session_state.processing = True
            try:
                st.rerun()
            except Exception:
                st.experimental_rerun()

        # When processing is active, run the long task and show progress + Stop button
        if st.session_state.get('processing', False):
            try:
                xml_progress_bar = st.progress(0.0, text="XML • Starting…")
            except Exception:
                xml_progress_bar = st.progress(0)

            json_progress_bar = None
            if fetch_holdings:
                try:
                    json_progress_bar = st.progress(0.0, text="JSON • Starting…")
                except Exception:
                    json_progress_bar = st.progress(0)

            remaining_time_placeholder = st.empty()
            start_time = datetime.now()
            # Keep worker count small; API throughput is controlled by the global 2 req/s limiter.
            max_workers = DEFAULT_WORKERS
            st.caption(f"Using {max_workers} worker threads with global API pacing at 2 requests/second.")

            all_fetched, all_saved, error_list = process_data(
                data_frame=data_frame,
                max_workers=max_workers,
                fetch_holdings=fetch_holdings,
                start_time=start_time,
                xml_progress_bar=xml_progress_bar,
                remaining_time_placeholder=remaining_time_placeholder,
                json_progress_bar=json_progress_bar,
            )

            st.session_state.all_fetched = all_fetched
            st.session_state.all_saved = all_saved
            st.session_state.error_list = error_list
            # Mark processing ended
            st.session_state.processing = False

            update_session_state(all_fetched, all_saved, error_list)

    # Show export buttons only if all required files are present for all OCNs
    if st.session_state.all_fetched and st.session_state.all_saved and csv_uploaded:
        # Build normalized OCN list from the uploaded CSV
        try:
            ocn_list = (
                data_frame['OCLC Number']
                .astype(str)
                .str.strip()
                .tolist()
            )
        except Exception:
            ocn_list = []
        # Require JSON holdings only if user opted to fetch holdings
        require_json = bool(fetch_holdings)
        all_present, missing_xml, missing_json = verify_required_files(ocn_list, require_json=require_json)

        if all_present:
            show_export_buttons()
        else:
            st.error("Cannot generate Excel yet. Some files are missing.")
            if missing_xml:
                with st.expander(f"Missing XML files for {len(missing_xml)} OCN(s)", expanded=False):
                    st.write(", ".join(missing_xml))
            if require_json and missing_json:
                with st.expander(f"Missing JSON holdings for {len(missing_json)} OCN(s)", expanded=False):
                    st.write(", ".join(missing_json))
            st.info("Please fetch missing records before proceeding to Step 2.")


if __name__ == "__main__":
    main()

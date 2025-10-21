"""
Bibrecord Retrieval Tool

This module provides a Streamlit application for uploading a CSV file containing OCLC numbers,
fetching corresponding records, and exporting the data to Excel.

Functions:
- main(): The main function that sets up the Streamlit interface and handles user interactions.
"""

import streamlit as st
import pandas as pd
import os
from datetime import datetime
from utils import process_data, update_session_state, show_export_buttons


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
                progress_bar = st.progress(0.0, text="Starting…")
            except Exception:
                progress_bar = st.progress(0)
            remaining_time_placeholder = st.empty()
            start_time = datetime.now()
            max_workers = min(10, (os.cpu_count() or 4) * 2)

            all_fetched, all_saved, error_list = process_data(
                data_frame, max_workers, fetch_holdings, start_time, progress_bar, remaining_time_placeholder
            )

            st.session_state.all_fetched = all_fetched
            st.session_state.all_saved = all_saved
            st.session_state.error_list = error_list
            # Mark processing ended
            st.session_state.processing = False

            update_session_state(all_fetched, all_saved, error_list)

    # Show export buttons if all records have been fetched and saved successfully
    if st.session_state.all_fetched and st.session_state.all_saved:
        show_export_buttons()


if __name__ == "__main__":
    main()

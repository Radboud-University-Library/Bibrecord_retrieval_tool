"""
utils.py

This module provides utility functions.
It includes functions for processing records, updating progress bars,
estimating remaining time, updating session state, and displaying export buttons.

Functions:
- process_data(data_frame, max_workers, fetch_holdings, start_time, progress_bar, remaining_time_placeholder)
- update_progress_bar(progress_bar, completed_count, total_records)
- update_remaining_time(start_time, completed_count, total_records, remaining_time_placeholder)
- update_session_state(all_fetched, all_saved, error_list)
- show_export_buttons()
"""

import time
import os
import streamlit as st
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
from request import fetch_and_save_data
from export import export_xml_data_to_excel, export_json_data_to_excel, merge_excel_files, save_all_xml_to_zip


def process_data(data_frame, max_workers, fetch_holdings, start_time, progress_bar, remaining_time_placeholder):
    """
    Process records using ThreadPoolExecutor, fetch data, and update progress bar.

    Args:
        data_frame (DataFrame): The dataframe containing OCNs to process.
        max_workers (int): The maximum number of threads to use.
        fetch_holdings (bool): Whether to fetch holdings as well.
        start_time (datetime): The time the process started.
        progress_bar (streamlit.progress): Progress bar object to update.
        remaining_time_placeholder (streamlit.empty): Placeholder to update remaining time.

    Returns:
        tuple: (all_fetched, all_saved, error_list)
    """
    all_fetched = True
    all_saved = True
    error_list = []

    chunk_size = max(100, len(data_frame) // 10)
    completed_count = 0
    total_records = len(data_frame)

    for chunk_start in range(0, total_records, chunk_size):
        chunk = data_frame.iloc[chunk_start:chunk_start + chunk_size]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(fetch_and_save_data, row['OCLC Number'], fetch_holdings=fetch_holdings): row[
                    'OCLC Number']
                for _, row in chunk.iterrows()
            }

            for future in as_completed(futures):
                ocn = futures[future]
                try:
                    ocn, error = future.result()
                    if error:
                        error_list.append(f"OCN {ocn}: {error}")
                        all_fetched = False
                        all_saved = False
                except Exception as e:
                    error_list.append(f"OCN {ocn}: {e}")
                    all_fetched = False
                    all_saved = False

                completed_count += 1

                # Batch update progress bar to reduce UI latency
                if completed_count % 10 == 0 or completed_count == len(futures):
                    update_progress_bar(progress_bar, completed_count, total_records)
                    update_remaining_time(start_time, completed_count, total_records, remaining_time_placeholder)

    return all_fetched, all_saved, error_list


def update_progress_bar(progress_bar, completed_count, total_records):
    """
    Update the progress bar in Streamlit.

    Args:
        progress_bar (streamlit.progress): Progress bar object to update.
        completed_count (int): Number of completed tasks.
        total_records (int): Total number of records to process.
    """
    progress_bar.progress(completed_count / total_records)


def update_remaining_time(start_time, completed_count, total_records, remaining_time_placeholder):
    """
    Update the estimated remaining time in Streamlit.

    Args:
        start_time (datetime): The time the process started.
        completed_count (int): Number of completed tasks.
        total_records (int): Total number of records to process.
        remaining_time_placeholder (streamlit.empty): Placeholder to update remaining time.
    """
    elapsed_time = (datetime.now() - start_time).total_seconds()
    avg_time_per_record = elapsed_time / completed_count
    remaining_time = avg_time_per_record * (total_records - completed_count)
    remaining_time_hms = time.strftime('%H:%M:%S', time.gmtime(remaining_time))
    remaining_time_placeholder.text(
        f"Progress: {completed_count}/{total_records} - Estimated time to completion: {remaining_time_hms}")


def update_session_state(all_fetched, all_saved, error_list):
    """
    Update the session state and display appropriate messages.

    Args:
        all_fetched (bool): Indicates if all records were fetched successfully.
        all_saved (bool): Indicates if all records were saved successfully.
        error_list (list): List of errors encountered during processing.
    """
    st.session_state.error_list = error_list

    if all_fetched and all_saved:
        st.success("All records have been fetched and saved successfully.")
    else:
        st.error("An error occurred while fetching and saving the records.")
        if error_list:
            st.write("Summary of Errors:")
            for error in error_list:
                st.write(error)


def show_export_buttons():
    # Button to export all XML data to Excel in chunks
    if st.button("Export Data to Excel"):
        xml_directory = 'OCNrecords/requested'
        json_directory = 'OCNrecords/requested_holdings'
        xml_final_filename = 'xml_data.xlsx'
        json_final_filename = 'json_data.xlsx'
        merged_final_filename = 'final_data.xlsx'
        progress_bar = st.progress(0)

        # Step 1: Export XML Data to Excel
        export_xml_data_to_excel(xml_directory, xml_final_filename, progress_bar)

        # Step 2: Check if JSON directory exists and contains files
        if os.path.exists(json_directory) and os.listdir(json_directory):
            st.info("Found JSON files. Exporting JSON data and merging with XML data.")

            # Step 3: Export JSON Data to Excel
            export_json_data_to_excel(json_directory, json_final_filename, progress_bar)

            # Step 4: Merge XML and JSON Excel files
            merge_excel_files(xml_final_filename, json_final_filename, merged_final_filename)

        else:
            st.info("No JSON files found. Skipping merging step.")
            merged_final_filename = xml_final_filename

        st.success(f"Export complete. Final file: {merged_final_filename}")

    # Button to save all requested XML files to a ZIP file
    if st.button("Save Marcxml Data to ZIP"):
        xml_directory = 'OCNrecords/requested'
        zip_file_name = "Export.zip"
        save_all_xml_to_zip(xml_directory, zip_file_name)


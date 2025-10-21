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


def _format_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def process_data(data_frame, max_workers, fetch_holdings, start_time, progress_bar, remaining_time_placeholder):
    """
    Process records using ThreadPoolExecutor, fetch data, and update progress bar with debounced ETA text.

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
    # Handle empty input early
    total_records = int(len(data_frame))
    if total_records == 0:
        try:
            progress_bar.progress(0.0, text="No records to process.")
        except Exception:
            progress_bar.progress(0.0)
        remaining_time_placeholder.empty()
        return True, True, []

    all_fetched = True
    all_saved = True
    error_list = []

    # Chunking
    chunk_size = max(100, total_records // 10)
    completed_count = 0

    # ETA smoothing state (EMA of per-record seconds)
    ema_tau = 0.2
    ema_sec_per_item = None

    # Debounce settings
    last_ui_update = 0.0
    min_update_interval = 0.25  # seconds

    def push_progress(force: bool = False, final: bool = False):
        nonlocal last_ui_update
        now = time.time()
        if not force and (now - last_ui_update) < min_update_interval:
            return
        last_ui_update = now

        frac = completed_count / max(1, total_records)
        if completed_count == 0 and not final:
            text = f"Starting… 0/{total_records}"
        else:
            elapsed = (datetime.now() - start_time).total_seconds()
            if ema_sec_per_item is None or completed_count == 0:
                sec_per_item = elapsed / max(1, completed_count)
            else:
                sec_per_item = ema_sec_per_item
            remaining = sec_per_item * max(0, total_records - completed_count)
            text = f"{int(frac*100)}% • {completed_count}/{total_records} • ETA {_format_eta(remaining)}"

        if final:
            try:
                progress_bar.progress(1.0, text=f"100% • {total_records}/{total_records} • Done")
            except Exception:
                progress_bar.progress(1.0)
            remaining_time_placeholder.empty()
        else:
            try:
                progress_bar.progress(frac, text=text)
            except Exception:
                progress_bar.progress(frac)

    # Initial paint
    push_progress(force=True)

    for chunk_start in range(0, total_records, chunk_size):
        if st.session_state.get('stop'):
            break
        chunk = data_frame.iloc[chunk_start:chunk_start + chunk_size]
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    fetch_and_save_data,
                    row['OCLC Number'],
                    fetch_holdings=fetch_holdings
                ): row['OCLC Number']
                for _, row in chunk.iterrows()
            }

            for future in as_completed(futures):
                if st.session_state.get('stop'):
                    break
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

                # Update EMA of average seconds per item based on elapsed/completed
                curr_sec_per_item = (datetime.now() - start_time).total_seconds() / max(1, completed_count)
                if ema_sec_per_item is None:
                    ema_sec_per_item = curr_sec_per_item
                else:
                    ema_sec_per_item = (1 - ema_tau) * ema_sec_per_item + ema_tau * curr_sec_per_item

                # Debounced progress update
                push_progress()

        # Ensure an update at the end of each chunk
        push_progress(force=True)

    # Final update
    push_progress(force=True, final=True)
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
        st.error("Some records failed to fetch or save.")
        if error_list:
            with st.expander(f"Summary of Errors ({len(error_list)})", expanded=False):
                for error in error_list:
                    st.write(error)


def show_export_buttons():
    # Ensure session flags exist
    if 'export_complete' not in st.session_state:
        st.session_state.export_complete = False
    if 'final_export_filename' not in st.session_state:
        st.session_state.final_export_filename = ''

    # Button to generate the Excel export (XML + optional JSON holdings)
    if st.button("Step 2: Generate Excel (XML + optional JSON holdings)"):
        xml_directory = 'OCNrecords/requested'
        json_directory = 'OCNrecords/requested_holdings'
        xml_final_filename = 'xml_data.xlsx'
        json_final_filename = 'json_data.xlsx'
        merged_final_filename = 'final_data.xlsx'

        with st.status("Exporting…", expanded=True) as status:
            progress_bar = st.progress(0.0, text="Exporting XML…")
            # Step 1: Export XML Data to Excel
            export_xml_data_to_excel(xml_directory, xml_final_filename, progress_bar)
            status.update(label="XML exported", state="running")

            # Step 2: Check if JSON directory exists and contains files
            if os.path.exists(json_directory) and os.listdir(json_directory):
                st.info("Found JSON files. Exporting JSON data and merging with XML data.")

                # Step 3: Export JSON Data to Excel
                progress_bar.progress(0.0, text="Exporting JSON…")
                export_json_data_to_excel(json_directory, json_final_filename, progress_bar)

                # Step 4: Merge XML and JSON Excel files
                st.write("Merging exports…")
                merge_excel_files(xml_final_filename, json_final_filename, merged_final_filename)
                status.update(label="Merged XML + JSON", state="running")
            else:
                st.info("No JSON files found. Skipping merging step.")
                merged_final_filename = xml_final_filename

            status.update(label=f"Export complete: {merged_final_filename}", state="complete")
            st.success(f"Export complete. Final file: {merged_final_filename}")

            # Mark export as complete and store final filename
            st.session_state.export_complete = True
            st.session_state.final_export_filename = merged_final_filename

    # Show the download button right after Step 2 export, if available
    final_name = st.session_state.get('final_export_filename')
    if st.session_state.get('export_complete') and final_name and os.path.exists(final_name):
        with open(final_name, "rb") as file:
            st.download_button(
                label="Step 3: Download Final Excel",
                data=file,
                file_name=final_name,
                mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            )

    # Visual separation for the ZIP action
    st.markdown("---")
    st.subheader("Optional: Archive raw data")

    # Button to save all requested XML files to a ZIP file (separate action)
    if st.button("Save raw MARCXML to ZIP (Desktop)"):
        xml_directory = 'OCNrecords/requested'
        zip_file_name = "Export.zip"
        save_all_xml_to_zip(xml_directory, zip_file_name)


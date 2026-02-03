"""
utils.py

This module provides utility functions.
It includes functions for processing records, updating progress bars,
estimating remaining time, updating session state, and displaying export buttons.

Functions:
- process_data(data_frame, max_workers, fetch_holdings, start_time, progress_bar, remaining_time_placeholder)
- update_session_state(all_fetched, all_saved, error_list)
- verify_required_files(ocn_list, require_json)
- show_export_buttons()
"""

import time
import os
import streamlit as st
from concurrent.futures import as_completed, ThreadPoolExecutor
from datetime import datetime
from request import fetch_and_save_data
from export import export_xml_data_to_excel, export_json_data_to_excel, merge_excel_files, save_all_xml_to_zip
import queue


def _format_eta(seconds: float) -> str:
    seconds = max(0, int(seconds))
    h, rem = divmod(seconds, 3600)
    m, s = divmod(rem, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


class ProgressReporter:
    """Thread-safe reporter used by workers to signal finished sub-steps."""
    def __init__(self):
        self._q = queue.Queue()

    def xml_done(self, ocn: str):
        self._q.put(("xml", ocn))

    def json_done(self, ocn: str):
        self._q.put(("json", ocn))

    def drain(self, max_items: int = 100):
        out = []
        for _ in range(max_items):
            try:
                out.append(self._q.get_nowait())
            except queue.Empty:
                break
        return out


def process_data(data_frame, max_workers, fetch_holdings, start_time, xml_progress_bar, remaining_time_placeholder, json_progress_bar=None):
    """
    Process records concurrently and update two progress bars:
    - XML bar (always shown)
    - JSON bar (only when fetch_holdings is True)

    Returns: (all_fetched, all_saved, error_list)
    """
    # Pre-filter: normalize, drop duplicates, and optionally skip already-downloaded OCNs
    original_df = data_frame
    try:
        df = data_frame.copy()
        df['OCLC Number'] = df['OCLC Number'].astype(str).str.strip()
        df = df[df['OCLC Number'] != ""]
        df = df.drop_duplicates(subset=['OCLC Number'])
        if fetch_holdings:
            # When fetching holdings, process all OCNs (even those with existing XML) so holdings can be fetched-only
            data_frame = df
        else:
            requested_dir = os.path.join('OCNrecords', 'requested')
            try:
                existing = set(os.path.splitext(f)[0] for f in os.listdir(requested_dir))
            except FileNotFoundError:
                existing = set()
            data_frame = df[~df['OCLC Number'].isin(existing)]
    except Exception:
        df = original_df

    # Totals
    xml_total = int(len(data_frame))
    if fetch_holdings:
        try:
            json_total = int(len(df))
        except Exception:
            json_total = xml_total
    else:
        json_total = 0

    # Empty check
    if xml_total == 0:
        try:
            xml_progress_bar.progress(0.0, text="No records to process.")
        except Exception:
            xml_progress_bar.progress(0.0)
        remaining_time_placeholder.empty()
        return True, True, []

    all_fetched = True
    all_saved = True
    error_list = []

    # EMA state for XML ETA
    ema_tau = 0.2
    ema_sec_per_item = None

    # Debounce settings
    last_ui_update = 0.0
    min_update_interval = 0.25

    # Sub-step counters
    xml_completed = 0
    json_completed = 0

    reporter = ProgressReporter()

    def push_xml_progress(force=False, final=False):
        nonlocal last_ui_update
        now = time.time()
        if not force and (now - last_ui_update) < min_update_interval:
            return
        last_ui_update = now

        frac = xml_completed / max(1, xml_total)
        if xml_completed == 0 and not final:
            text = f"XML • Starting… 0/{xml_total}"
        else:
            elapsed = (datetime.now() - start_time).total_seconds()
            if ema_sec_per_item is None or xml_completed == 0:
                sec_per_item = elapsed / max(1, xml_completed)
            else:
                sec_per_item = ema_sec_per_item
            remaining = sec_per_item * max(0, xml_total - xml_completed)
            text = f"XML • {int(frac*100)}% • {xml_completed}/{xml_total} • ETA {_format_eta(remaining)}"

        if final:
            try:
                xml_progress_bar.progress(1.0, text=f"XML • 100% • {xml_total}/{xml_total} • Done")
            except Exception:
                xml_progress_bar.progress(1.0)
        else:
            try:
                xml_progress_bar.progress(frac, text=text)
            except Exception:
                xml_progress_bar.progress(frac)

    def push_json_progress(force=False, final=False):
        if not fetch_holdings or json_progress_bar is None:
            return
        frac = json_completed / max(1, json_total)
        if final:
            try:
                json_progress_bar.progress(1.0, text=f"JSON • 100% • {json_total}/{json_total} • Done")
            except Exception:
                json_progress_bar.progress(1.0)
            return
        try:
            json_progress_bar.progress(frac, text=f"JSON • {int(frac*100)}% • {json_completed}/{json_total}")
        except Exception:
            json_progress_bar.progress(frac)

    # Initial paints
    push_xml_progress(force=True)
    if fetch_holdings and json_progress_bar is not None:
        try:
            json_progress_bar.progress(0.0, text=f"JSON • Starting… 0/{json_total}")
        except Exception:
            json_progress_bar.progress(0.0)

    # Submit tasks
    ocns = data_frame['OCLC Number'].astype(str).tolist()
    executor = ThreadPoolExecutor(max_workers=max_workers)
    executor_shutdown = False
    try:
        futures = {executor.submit(fetch_and_save_data, ocn, fetch_holdings, reporter): ocn for ocn in ocns}
        for future in as_completed(futures):
            if st.session_state.get('stop'):
                try:
                    executor.shutdown(cancel_futures=True)
                    executor_shutdown = True
                except Exception:
                    pass
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

            # Drain events accumulated so far
            for (kind, _ocn_evt) in reporter.drain():
                if kind == "xml":
                    xml_completed += 1
                elif kind == "json":
                    json_completed += 1

            # Update EMA based on XML progress
            if xml_completed > 0:
                curr_sec_per_item = (datetime.now() - start_time).total_seconds() / max(1, xml_completed)
                if ema_sec_per_item is None:
                    ema_sec_per_item = curr_sec_per_item
                else:
                    ema_sec_per_item = (1 - ema_tau) * ema_sec_per_item + ema_tau * curr_sec_per_item

            # Debounced UI updates
            push_xml_progress()
            push_json_progress()
    finally:
        if not executor_shutdown:
            try:
                executor.shutdown(wait=True)
            except Exception:
                pass
        # Drain any remaining events
        for (kind, _ocn_evt) in reporter.drain(100000):
            if kind == "xml":
                xml_completed += 1
            elif kind == "json":
                json_completed += 1
        # Final updates
        push_xml_progress(force=True, final=True)
        if fetch_holdings and json_progress_bar is not None:
            push_json_progress(final=True)
        remaining_time_placeholder.empty()

    return all_fetched, all_saved, error_list




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
            # Detailed list of each failure
            with st.expander(f"Detailed errors ({len(error_list)})", expanded=False):
                for error in error_list:
                    st.write(error)

            # Grouped summary by reason so the user can quickly see why records weren't fetched
            # Expected error_list entries look like: "OCN 12345: <reason>"
            grouped = {}
            for entry in error_list:
                try:
                    # Split on the first colon to isolate the reason part
                    prefix, reason = entry.split(":", 1)
                    reason = reason.strip()
                except ValueError:
                    reason = entry.strip()
                grouped.setdefault(reason, []).append(entry)

            with st.expander("Why records were not fetched (grouped)", expanded=True):
                for reason, entries in grouped.items():
                    # Extract OCNs from the entries for a concise summary line
                    ocns = []
                    for e in entries:
                        # Expect formats like "OCN 12345: ..."
                        parts = e.split(":", 1)[0].strip().split()
                        if len(parts) >= 2 and parts[0].upper() == "OCN":
                            ocns.append(parts[1])
                    count = len(entries)
                    st.markdown(f"- {reason} — {count} record(s)")
                    if ocns:
                        st.caption("OCNs: " + ", ".join(ocns))


def verify_required_files(ocn_list, require_json: bool = True):
    """
    Verify that for every OCN in ocn_list, the expected XML and (optionally) JSON files exist.

    Args:
        ocn_list (list[str]): List of OCN strings to verify.
        require_json (bool): If True, JSON holdings files are also required.

    Returns:
        tuple:
            (all_present: bool, missing_xml: list[str], missing_json: list[str])
    """
    xml_dir = os.path.join('OCNrecords', 'requested')
    json_dir = os.path.join('OCNrecords', 'requested_holdings')

    missing_xml = []
    missing_json = []

    for raw in ocn_list:
        ocn = str(raw).strip()
        if not ocn:
            continue
        xml_path = os.path.join(xml_dir, f"{ocn}.xml")
        if not os.path.exists(xml_path):
            missing_xml.append(ocn)
        if require_json:
            json_path = os.path.join(json_dir, f"{ocn}_holdings.json")
            if not os.path.exists(json_path):
                missing_json.append(ocn)

    all_present = (len(missing_xml) == 0) and ((len(missing_json) == 0) if require_json else True)
    return all_present, missing_xml, missing_json


def show_export_buttons():
    # Ensure session flags exist
    if 'export_complete' not in st.session_state:
        st.session_state.export_complete = False
    if 'final_export_filename' not in st.session_state:
        st.session_state.final_export_filename = ''

    # Button to generate the Excel export
    if st.button("Step 2: Generate Excel"):
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


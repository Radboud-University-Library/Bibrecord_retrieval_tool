"""
export.py

This module handles the export of MARCXML and JSON data to Excel files.
It includes functions for parsing MARCXML data, exporting data to Excel,
merging Excel files, and saving XML files to a ZIP archive.

Functions:
- parse_marcxml(xml_data)
- export_xml_data_to_excel(xml_directory, final_filename, progress_bar)
- export_json_data_to_excel(json_directory, final_filename, progress_bar)
- merge_excel_files(xml_filename, json_filename, merged_filename)
- save_all_xml_to_zip(xml_directory, zip_file_name)
"""

import streamlit as st
import os
import xml.etree.ElementTree as ET
import pandas as pd
import zipfile
import json


def parse_marcxml(xml_data):
    """Parse MARCXML data and return a list of dictionaries."""
    ns = {'marc': 'http://www.loc.gov/MARC21/slim'}
    root = ET.fromstring(xml_data)
    all_records = []

    for record in [root]:
        record_dict = {}
        for cf in record.findall('marc:controlfield', ns):
            tag = cf.get('tag')
            record_dict[tag] = cf.text.strip() if cf.text else ''

        for df in record.findall('marc:datafield', ns):
            tag = df.get('tag')
            for sf in df.findall('marc:subfield', ns):
                code = sf.get('code')
                value = sf.text.strip() if sf.text else ''
                key = f"{tag}_{code}"
                if key in record_dict:
                    if isinstance(record_dict[key], list):
                        record_dict[key].append(value)
                    else:
                        record_dict[key] = [record_dict[key], value]
                else:
                    record_dict[key] = value

        all_records.append(record_dict)

    return all_records


def export_xml_data_to_excel(xml_directory, final_filename, progress_bar):

    if not os.path.exists(xml_directory):
        st.error("No XML files found to export.")
        return

    xml_files = [f for f in os.listdir(xml_directory) if f.endswith('.xml')]
    if not xml_files:
        st.error("No XML files found to export.")
        return

    total_files = len(xml_files)
    chunk_size = 5000

    # First pass: determine the complete set of columns across all files
    all_columns = set()
    for filename in xml_files:
        filepath = os.path.join(xml_directory, filename)
        with open(filepath, 'r', encoding='utf-8') as file:
            xml_data = file.read()
            parsed_data = parse_marcxml(xml_data)
            for record in parsed_data:
                all_columns.update(record.keys())
    # Fix the order of columns (for example, sorted alphabetically)
    all_columns = sorted(all_columns)

    # Second pass: process the XML files in chunks with a consistent column order
    for i in range(0, total_files, chunk_size):
        chunk_data = []
        chunk = xml_files[i:i + chunk_size]
        for filename in chunk:
            filepath = os.path.join(xml_directory, filename)
            with open(filepath, 'r', encoding='utf-8') as file:
                xml_data = file.read()
                parsed_data = parse_marcxml(xml_data)
                chunk_data.extend(parsed_data)

        # Create DataFrame and reindex using the pre-determined column order
        df = pd.DataFrame(chunk_data)
        df = df.reindex(columns=all_columns)  # This ensures consistent alignment

        # Write the data to the final Excel file
        if i == 0:  # For the first chunk, create a new file with header
            df.to_excel(final_filename, index=False, engine='openpyxl')
        else:  # For subsequent chunks, append to the existing sheet
            with pd.ExcelWriter(final_filename, engine='openpyxl', mode='a', if_sheet_exists='overlay') as writer:
                startrow = writer.book['Sheet1'].max_row
                df.to_excel(writer, index=False, header=False, startrow=startrow)

        progress_bar.progress((i + len(chunk)) / total_files, text=f"XML {min(i+len(chunk), total_files)}/{total_files}")

    st.success(f"XML data has been exported to {final_filename}")


def export_json_data_to_excel(json_directory, final_filename, progress_bar):
    if not os.path.exists(json_directory):
        st.error("No JSON files found to export.")
        return

    json_files = [f for f in os.listdir(json_directory) if f.endswith('.json')]
    if not json_files:
        st.error("No JSON files found to export.")
        return

    total_files = len(json_files)
    all_data = []

    # Process each JSON file
    for i, filename in enumerate(json_files):
        filepath = os.path.join(json_directory, filename)
        with open(filepath, 'r', encoding='utf-8') as file:
            json_data = json.load(file)
            if isinstance(json_data, dict) and "holdings" in json_data:
                base_record = {"ocn": json_data.get("ocn")}
                holdings = json_data["holdings"]

                # Flatten ONLY totalHoldingCount per institution
                for holding in holdings:
                    symbol = holding.get("institutionSymbol")
                    if symbol is None:
                        continue
                    total_count = holding.get("totalHoldingCount", 0)
                    column_name = f"totalHoldingCount_{symbol}"
                    base_record[column_name] = total_count

                all_data.append(base_record)

        # Update progress bar
        progress_bar.progress((i + 1) / total_files, text=f"JSON {i+1}/{total_files}")

    # Write the flattened JSON data to the final Excel file
    df = pd.DataFrame(all_data)
    df.to_excel(final_filename, index=False, engine='openpyxl')

    st.success(f"JSON data has been exported to {final_filename}")


def merge_excel_files(xml_filename, json_filename, merged_filename):
    # Read both Excel files into DataFrames
    df_xml = pd.read_excel(xml_filename, engine='openpyxl')
    df_json = pd.read_excel(json_filename, engine='openpyxl')

    # Clean the '001' field in XML DataFrame
    if '001' in df_xml.columns:
        df_xml['001'] = df_xml['001'].astype(str).str.extract(r'(\d+)', expand=False)
        df_xml['001'] = df_xml['001'].astype(int).astype(str)

    # Clean the 'ocn' field in JSON DataFrame
    if 'ocn' in df_json.columns:
        df_json['ocn'] = df_json['ocn'].astype(str).astype(int).astype(str)

    # Keep only totalHoldingCount_* columns from JSON along with 'ocn'
    json_cols = [c for c in df_json.columns if c.startswith('totalHoldingCount_')]
    keep_cols = ['ocn'] + json_cols if 'ocn' in df_json.columns else json_cols
    df_json = df_json[keep_cols]

    # Merge XML and JSON DataFrames on the '001' (from XML) and 'ocn' (from JSON)
    merged_df = pd.merge(df_xml, df_json, left_on='001', right_on='ocn', how='left')

    # Insert a per-row sum column for JSON totals placed before the JSON columns
    json_cols_in_merged = [c for c in merged_df.columns if c.startswith('totalHoldingCount_')]
    sum_col_name = 'totalHoldingCount_SUM'
    if json_cols_in_merged:
        # Create numeric sum as a fallback (also used by filters/pivots)
        merged_df[sum_col_name] = merged_df[json_cols_in_merged].sum(axis=1, skipna=True)
        # Reorder columns: XML columns, then SUM, then JSON columns
        first_json_idx = min(merged_df.columns.get_loc(c) for c in json_cols_in_merged)
        before_json = list(merged_df.columns[:first_json_idx])
        after_json = list(merged_df.columns[first_json_idx:])
        new_order = before_json + [sum_col_name] + [c for c in after_json if c != sum_col_name]
        merged_df = merged_df.reindex(columns=new_order)

    # Write the merged data to a new Excel file
    merged_df.to_excel(merged_filename, index=False, engine='openpyxl')

    # Replace SUM column values with Excel formulas so they are dynamic in Excel
    if json_cols_in_merged:
        try:
            from openpyxl import load_workbook
            from openpyxl.utils import get_column_letter
            wb = load_workbook(merged_filename)
            ws = wb.active
            # Determine column indices for SUM and JSON columns from header row
            headers = [cell.value for cell in next(ws.iter_rows(min_row=1, max_row=1))]
            sum_col_idx = headers.index(sum_col_name) + 1 if sum_col_name in headers else None
            json_col_indices = [headers.index(c) + 1 for c in headers if c in json_cols_in_merged]
            if sum_col_idx and json_col_indices:
                first_data_row = 2
                last_row = ws.max_row
                for row in range(first_data_row, last_row + 1):
                    # Build SUM over all JSON columns for this row
                    refs = [f"{get_column_letter(col)}{row}" for col in json_col_indices]
                    formula = f"=SUM({','.join(refs)})"
                    ws.cell(row=row, column=sum_col_idx, value=formula)
            wb.save(merged_filename)
        except Exception:
            # If openpyxl is unavailable or something goes wrong, keep numeric sums
            pass

    st.success(f"XML and JSON data have been merged and exported to {merged_filename}")


def save_all_xml_to_zip(xml_directory, zip_file_name):
    """Save all XML files from the specified directory to a ZIP file to Desktop."""
    home_directory = os.path.expanduser('~')
    desktop_directory = os.path.join(home_directory, 'Desktop')

    if not os.path.isdir(desktop_directory):
        st.error("Unable to find the desktop directory.")
        return

    save_location = os.path.join(desktop_directory, zip_file_name)
    with zipfile.ZipFile(save_location, 'w') as zipf:
        for root, _, files in os.walk(xml_directory):
            for file in files:
                if file.endswith('.xml'):
                    zipf.write(os.path.join(root, file), arcname=file)

    st.success(f"All XML files have been saved to {save_location} successfully.")

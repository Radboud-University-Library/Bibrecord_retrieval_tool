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
    chunk_size = 10

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

        progress_bar.progress((i + len(chunk)) / total_files)

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

                # Flatten holdings into separate columns for each institution
                for holding in holdings:
                    for key, value in holding.items():
                        if key != "institutionSymbol":
                            column_name = f"{key}_{holding['institutionSymbol']}"
                            base_record[column_name] = value

                all_data.append(base_record)

        # Update progress bar
        progress_bar.progress((i + 1) / total_files)

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

    # Merge XML and JSON DataFrames on the '001' (from XML) and 'ocn' (from JSON)
    merged_df = pd.merge(df_xml, df_json, left_on='001', right_on='ocn', how='left')

    # Write the merged data to a new Excel file
    merged_df.to_excel(merged_filename, index=False, engine='openpyxl')

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

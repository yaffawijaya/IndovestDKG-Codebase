import re
from datetime import datetime
import pandas as pd

def clean_and_convert_date(date_str):
    match = re.search(r'\d{2}/\d{2}/\d{4}, \d{2}:\d{2} WIB', str(date_str))
    if match:
        date_str = match.group(0)
        try:
            return datetime.strptime(date_str, '%d/%m/%Y, %H:%M WIB')
        except ValueError:
            return pd.NaT
    return pd.NaT

def remove_title_from_isi(row):
    if 'judul' not in row or 'isi' not in row:
        return row.get('isi', '')
    judul = row['judul'] if pd.notna(row['judul']) else ""
    isi = row['isi'] if pd.notna(row['isi']) else ""
    judul_normalized = re.sub(r'\s+', ' ', str(judul)).strip().lower()
    isi_normalized = re.sub(r'\s+', ' ', str(isi)).strip().lower()
    isi_cleaned = re.sub(re.escape(judul_normalized), '', isi_normalized, flags=re.IGNORECASE)
    isi_cleaned = re.sub(r'\s+', ' ', isi_cleaned).strip()
    return isi_cleaned

def remove_redundancy(text):
    if len(text) < 100:
        return text
    prefix = text[:100]
    cleaned_text = text
    start_index = 0
    while True:
        start_index = cleaned_text.find(prefix, start_index + len(prefix))
        if start_index == -1:
            break
        end_index = cleaned_text.find(prefix, start_index + 1)
        if end_index == -1:
            end_index = len(cleaned_text)
        cleaned_text = cleaned_text[:start_index] + cleaned_text[end_index:]
        start_index = 0
    return cleaned_text

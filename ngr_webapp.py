import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import io, zipfile, datetime

# -------------------------------------------------------------
# GrainBridge • NGR XML → Airtable CSV Converter
# -------------------------------------------------------------
# Drop an NGR XML export, get four tidy CSVs (plus a ZIP)
# • GRN_unique.csv        – one row per GRN
# • PAYEE_unique.csv      – one row per PAYEE_ID
# • USER_unique.csv       – one row per USER_ID
# • GRN_PAYEE_USER.csv    – mapping table (many‑to‑many links)
# -------------------------------------------------------------
# All processing happens in‑memory inside the Streamlit session.
# No external APIs, no data leaves this app.
# -------------------------------------------------------------

st.set_page_config(
    page_title="GrainBridge • NGR XML → Airtable CSV",
    page_icon="🌾",
    layout="centered",
)

st.title("🌾 GrainBridge")

st.markdown(
    """
**Turn NGR XML exports into clean, Airtable‑ready CSVs — in one drop.**

1. **Upload** your XML file below.  
2. GrainBridge 💪 removes duplicates and builds a link‑table for you.  
3. **Download** four CSVs (or a handy ZIP) and import them straight into Airtable.

*Everything happens right here — nothing is sent to outside servers.*
"""
)

# ---------------- Helper functions ----------------

def parse_xml(upload) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Extract raw GRN, Payee and User DataFrames from an NGR XML file."""
    tree = ET.parse(upload)
    root = tree.getroot()

    def txt(elem, tag):
        t = elem.find(tag)
        return t.text.strip() if t is not None and t.text else None

    grn_rows, payee_rows, user_rows = [], [], []

    for partnership in root.findall('partnership'):
        grn_id = txt(partnership, 'GRN')
        # --- GRN flat row ---
        grn_rows.append({
            child.tag.upper(): (child.text or '').strip()
            for child in partnership
            if child.tag != 'payee' and len(child) == 0
        })

        for payee in partnership.findall('payee'):
            payee_id = txt(payee, 'PAYEE_ID')
            # --- Payee flat row ---
            payee_row = {'GRN': grn_id}
            payee_row.update({
                child.tag.upper(): (child.text or '').strip()
                for child in payee
                if child.tag != 'user' and len(child) == 0
            })
            payee_rows.append(payee_row)

            for user in payee.findall('user'):
                phone_types, phone_nums = [], []
                user_row = {'GRN': grn_id, 'PAYEE_ID': payee_id}
                for child in user:
                    if child.tag == 'PHONE_TYPE':
                        phone_types.append((child.text or '').strip())
                    elif child.tag == 'PHONE_NUMBER':
                        phone_nums.append((child.text or '').strip())
                    elif len(child) == 0:
                        user_row[child.tag.upper()] = (child.text or '').strip()
                user_row['PHONE_TYPES'] = '; '.join(phone_types) if phone_types else None
                user_row['PHONE_NUMBERS'] = '; '.join(phone_nums) if phone_nums else None
                user_rows.append(user_row)

    return pd.DataFrame(grn_rows), pd.DataFrame(payee_rows), pd.DataFrame(user_rows)


def dedup(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return df with first non‑null value kept for each duplicated key."""
    return (
        df.groupby(key, as_index=False)
          .agg(lambda col: col.dropna().iloc[0] if col.notna().any() else None)
    )


def build_outputs(grn_raw, payee_raw, user_raw):
    grn_unique   = grn_raw.drop_duplicates(subset=['GRN'])
    payee_unique = dedup(payee_raw, 'PAYEE_ID')
    user_unique  = dedup(user_raw,  'USER_ID')
    mapping      = user_raw[['GRN', 'PAYEE_ID', 'USER_ID']].drop_duplicates()
    return grn_unique, payee_unique, user_unique, mapping


def df_to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode('utf-8')

# ---------------- Streamlit UI ----------------

xml_file = st.file_uploader("Drop your NGR XML file here", type=["xml"], help="Max 200 MB per file")

if xml_file:
    with st.spinner("Crunching numbers…"):
        try:
            grn_raw, payee_raw, user_raw = parse_xml(xml_file)
        except Exception as e:
            st.error(f"❌ Could not parse XML: {e}")
            st.stop()

        grn_u, payee_u, user_u, mapping = build_outputs(grn_raw, payee_raw, user_raw)

        csv_map = {
            "GRN_unique.csv": grn_u,
            "PAYEE_unique.csv": payee_u,
            "USER_unique.csv": user_u,
            "GRN_PAYEE_USER.csv": mapping,
        }

        st.success("Done! Download your files:")
        for fn, df in csv_map.items():
            st.download_button(
                label=f"📥 {fn}",
                data=df_to_csv_bytes(df),
                file_name=fn,
                mime="text/csv",
            )

        # One‑click ZIP bundle
        zip_buf = io.BytesIO()
        with zipfile.ZipFile(zip_buf, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fn, df in csv_map.items():
                zf.writestr(fn, df_to_csv_bytes(df))
        zip_buf.seek(0)
        stamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="📦 Download all as ZIP",
            data=zip_buf,
            file_name=f"grainbridge_{stamp}.zip",
            mime="application/zip",
        )

        st.info(
            "Import the three **unique** tables first in Airtable, then the "
            "**GRN_PAYEE_USER** table, and turn each column into ‘Link to "
            "another record’."
        )

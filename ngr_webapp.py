import streamlit as st
import pandas as pd
import xml.etree.ElementTree as ET
import io, zipfile, datetime

st.set_page_config(page_title="NGR XML → Airtable CSV", page_icon="📄")

st.title("📄 NGR XML → Airtable CSV Converter")
st.markdown(
    """
Upload an **NGR XML** export below and get back four deduplicated CSV files
plus a ZIP bundle ready for Airtable:
* **GRN_unique.csv** – one row per GRN
* **PAYEE_unique.csv** – one row per PAYEE_ID
* **USER_unique.csv** – one row per USER_ID
* **GRN_PAYEE_USER.csv** – mapping of every GRN ↔ PAYEE ↔ USER combo
"""
)

# ---------------- Helper functions ----------------

def parse_xml(upload) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
    """Return raw GRN, Payee, User dataframes from an NGR XML file-like object."""
    tree = ET.parse(upload)
    root = tree.getroot()

    def text(elem, tag):
        t = elem.find(tag)
        return t.text.strip() if t is not None and t.text else None

    grn_rows, payee_rows, user_rows = [], [], []

    for partnership in root.findall('partnership'):
        grn_id = text(partnership, 'GRN')
        grn_row = {child.tag.upper(): (child.text or '').strip()
                   for child in partnership if len(child) == 0 and child.tag != 'payee'}
        grn_rows.append(grn_row)

        for payee in partnership.findall('payee'):
            payee_id = text(payee, 'PAYEE_ID')
            payee_row = {'GRN': grn_id}
            payee_row.update({child.tag.upper(): (child.text or '').strip()
                              for child in payee if len(child) == 0 and child.tag != 'user'})
            payee_rows.append(payee_row)

            for user in payee.findall('user'):
                user_row = {'GRN': grn_id, 'PAYEE_ID': payee_id}
                phone_types, phone_nums = [], []
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


def deduplicate(df: pd.DataFrame, key: str) -> pd.DataFrame:
    """Return df with first non-null value kept for each duplicated key."""
    return (df.groupby(key, as_index=False)
              .agg(lambda col: col.dropna().iloc[0] if col.notna().any() else None))


def build_outputs(grn_raw, payee_raw, user_raw):
    grn_unique = grn_raw.drop_duplicates(subset=['GRN'])
    payee_unique = deduplicate(payee_raw, 'PAYEE_ID')
    user_unique = deduplicate(user_raw, 'USER_ID')
    mapping = user_raw[['GRN', 'PAYEE_ID', 'USER_ID']].drop_duplicates()
    return grn_unique, payee_unique, user_unique, mapping


def to_csv_bytes(df: pd.DataFrame) -> bytes:
    buf = io.StringIO()
    df.to_csv(buf, index=False)
    return buf.getvalue().encode('utf-8')

# ---------------- Streamlit UI ----------------

xml_file = st.file_uploader("Drop your NGR XML file here", type=["xml"])

if xml_file:
    with st.spinner("Parsing and converting…"):
        grn_raw, payee_raw, user_raw = parse_xml(xml_file)
        grn_u, payee_u, user_u, mapping = build_outputs(grn_raw, payee_raw, user_raw)

        # Build CSV downloads
        csv_data = {
            "GRN_unique.csv": grn_u,
            "PAYEE_unique.csv": payee_u,
            "USER_unique.csv": user_u,
            "GRN_PAYEE_USER.csv": mapping
        }

        for fn, df in csv_data.items():
            st.download_button(
                label=f"Download {fn}",
                data=to_csv_bytes(df),
                file_name=fn,
                mime="text/csv",
            )

        # Build ZIP in‑memory
        zip_buffer = io.BytesIO()
        with zipfile.ZipFile(zip_buffer, 'w', zipfile.ZIP_DEFLATED) as zf:
            for fn, df in csv_data.items():
                zf.writestr(fn, to_csv_bytes(df))
        zip_buffer.seek(0)
        ts = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
        st.download_button(
            label="Download all as ZIP",
            data=zip_buffer,
            file_name=f"ngr_csv_{ts}.zip",
            mime="application/zip",
        )

        st.success("Conversion complete. Import the three unique tables first in Airtable, then the mapping table, and turn each column into ‘Link to another record’.")

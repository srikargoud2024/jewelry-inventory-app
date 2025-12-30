import re
import io
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload
from googleapiclient.errors import HttpError

st.set_page_config(page_title="Inventory Memo App", layout="wide")
st.title("📦 Jewelry Inventory – Memo Upload")
st.caption("Upload memo PDF → auto extract items → preview → confirm → updates Google Sheets")

# ----------------- Google Auth -----------------
SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]
creds = Credentials.from_service_account_info(st.secrets["gcp_service_account"], scopes=SCOPES)

gc = gspread.authorize(creds)
drive = build("drive", "v3", credentials=creds)

SHEET_URL = st.secrets["sheet_url"]
sh = gc.open_by_url(SHEET_URL)
ws_inventory = sh.worksheet("INVENTORY")
ws_log = sh.worksheet("TRANSACTIONS_LOG")

# ----------------- Regex -----------------
ITEM_RE = re.compile(r"\b(?:BR|BS|GB)[2-8][YW]-14K(?:-(?:1|2|3|4))?\b", re.I)
MEMO_RE = re.compile(r"\bMemo\s*#\s*[:\-]?\s*([A-Z0-9\-]+)\b", re.I)


def now_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


def drive_ocr_pdf_to_text(pdf_bytes: bytes, filename: str) -> str:
    """
    Reliable OCR flow for Streamlit Cloud:
      1) Upload PDF as PDF
      2) Copy/convert to Google Doc (OCR happens here)
      3) Export Doc as text/plain
      4) Delete temp files
    """
    pdf_file_id = None
    doc_file_id = None

    try:
        # 1) Upload PDF
        media = MediaIoBaseUpload(io.BytesIO(pdf_bytes), mimetype="application/pdf", resumable=False)
        pdf_created = drive.files().create(
            body={
                "name": f"UPLOAD_{filename}_{int(datetime.now().timestamp())}",
                "mimeType": "application/pdf",
            },
            media_body=media,
            fields="id",
        ).execute()
        pdf_file_id = pdf_created["id"]

        # 2) Convert by copying to Google Doc (OCR)
        doc_created = drive.files().copy(
            fileId=pdf_file_id,
            body={
                "name": f"OCR_{filename}_{int(datetime.now().timestamp())}",
                "mimeType": "application/vnd.google-apps.document",
            },
            fields="id",
        ).execute()
        doc_file_id = doc_created["id"]

        # 3) Export text
        exported = drive.files().export(fileId=doc_file_id, mimeType="text/plain").execute()
        return exported.decode("utf-8", errors="ignore")

    except HttpError as e:
        # Show real error details (safe)
        try:
            detail = e.content.decode("utf-8", errors="ignore")
        except Exception:
            detail = str(e)
        raise RuntimeError(f"Drive OCR failed. Details: {detail}") from e

    finally:
        # 4) Cleanup temp files
        try:
            if doc_file_id:
                drive.files().delete(fileId=doc_file_id).execute()
        except Exception:
            pass
        try:
            if pdf_file_id:
                drive.files().delete(fileId=pdf_file_id).execute()
        except Exception:
            pass


def parse_items_from_ocr_text(text: str) -> tuple[str | None, dict]:
    """
    Returns (memo_no, {item_code: qty})
    For each line containing an item_code, pick the first integer in the same line as qty.
    """
    memo_no = None
    m = MEMO_RE.search(text)
    if m:
        memo_no = m.group(1).strip()

    items: dict[str, int] = {}
    lines = [ln.strip() for ln in text.splitlines() if ln.strip()]

    for ln in lines:
        u = ln.upper()

        # Ignore non-inventory lines
        if "SHIPPING" in u or "INSURANCE" in u:
            continue

        code_match = ITEM_RE.search(ln)
        if not code_match:
            continue

        code = code_match.group(0).upper()

        qty_match = re.search(r"\b(\d+)\b", ln)
        if not qty_match:
            continue

        qty = int(qty_match.group(1))
        if qty <= 0 or qty > 999:
            continue

        items[code] = items.get(code, 0) + qty

    return memo_no, items


def read_inventory_df() -> pd.DataFrame:
    data = ws_inventory.get_all_records()
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=["item_code", "on_hand"])

    df["item_code"] = df["item_code"].astype(str).str.upper().str.strip()
    df["on_hand"] = pd.to_numeric(df["on_hand"], errors="coerce").fillna(0).astype(int)
    return df


def memo_already_processed(memo_no: str) -> bool:
    log = ws_log.get_all_records()
    target = memo_no.strip().upper()
    for r in log:
        if str(r.get("memo_no", "")).strip().upper() == target:
            return True
    return False


def apply_updates(preview_df: pd.DataFrame, memo_no: str | None, source: str, reason: str, user: str):
    inv_df = read_inventory_df().set_index("item_code")

    # Validate codes exist
    missing = [c for c in preview_df["item_code"].tolist() if str(c).upper().strip() not in inv_df.index]
    if missing:
        raise ValueError(f"These item codes are not in INVENTORY sheet: {missing[:10]}")

    # Prepare updates and block negative stock
    updates = []
    for _, row in preview_df.iterrows():
        code = str(row["item_code"]).upper().strip()
        qty = int(row["qty"])
        current = int(inv_df.loc[code, "on_hand"])
        new = current - qty
        if new < 0:
            raise ValueError(f"Negative stock not allowed: {code} would go {current} -> {new}")
        updates.append((code, new, -qty))

    # Batch update INVENTORY
    inv_all = ws_inventory.get_all_values()
    header = inv_all[0]
    code_col = header.index("item_code") + 1
    onhand_col = header.index("on_hand") + 1

    code_to_row = {}
    for i, row in enumerate(inv_all[1:], start=2):
        if len(row) >= code_col:
            code_to_row[row[code_col - 1].strip().upper()] = i

    cell_updates = []
    for code, new_onhand, _qty_change in updates:
        r = code_to_row.get(code)
        if not r:
            raise ValueError(f"Could not locate {code} row in INVENTORY sheet.")
        cell_updates.append(gspread.Cell(r, onhand_col, str(new_onhand)))

    ws_inventory.update_cells(cell_updates)

    # Append TRANSACTIONS_LOG rows
    ts = now_str()
    log_rows = []
    for code, _new_onhand, qty_change in updates:
        log_rows.append([ts, source, memo_no or "", code, qty_change, reason, user, ""])
    ws_log.append_rows(log_rows, value_input_option="USER_ENTERED")


# ----------------- UI -----------------
with st.sidebar:
    st.header("Settings")
    user = st.text_input("Employee name", value="Employee")
    reason = st.selectbox("Reason", ["Sale", "Amazon", "Adjustment", "Return", "Damage"], index=0)
    st.divider()
    st.caption("Tip: If Streamlit ever fails, use your Google Form backup.")

uploaded = st.file_uploader("Upload memo PDF (scanned is OK)", type=["pdf"])

if uploaded:
    pdf_bytes = uploaded.read()

    try:
    with st.spinner("Running OCR via Google Drive…"):
        text = drive_ocr_pdf_to_text(pdf_bytes, uploaded.name)
except Exception as e:
    st.error("OCR failed. This is the real error detail (copy/paste it to me):")
    st.code(str(e))
    st.stop()


    memo_no, items = parse_items_from_ocr_text(text)

    col1, col2, col3 = st.columns(3)
    col1.metric("Detected items", len(items))
    col2.metric("Total qty", sum(items.values()) if items else 0)
    col3.write(f"**Memo #:** {memo_no or 'Not detected'}")

    if not items:
        st.error("No item codes detected. Expand 'Show raw OCR text' and share a snippet with me.")
        st.stop()

    preview_df = pd.DataFrame([{"item_code": k, "qty": v} for k, v in sorted(items.items())])

    st.subheader("Preview (edit if needed)")
    edited_df = st.data_editor(preview_df, num_rows="dynamic", use_container_width=True)

    if memo_no and memo_already_processed(memo_no):
        st.error(f"Memo {memo_no} was already processed. (Duplicate protection)")
        st.stop()

    st.divider()
    if st.button("✅ Confirm & Update Inventory", type="primary"):
        try:
            apply_updates(edited_df, memo_no, source="Memo PDF", reason=reason, user=user)
            st.success("Inventory updated and transactions logged.")
        except Exception as e:
            st.error(f"Update failed: {e}")

st.divider()
with st.expander("Show raw OCR text (for debugging)"):
    st.write("If something is wrong, copy/paste a few lines here and I’ll fix the parser.")
    if "text" in locals():
        st.text(text[:15000])

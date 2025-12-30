import re
import io
from datetime import datetime, timezone

import pandas as pd
import streamlit as st
import gspread

from google.oauth2.service_account import Credentials as SACredentials
from google.oauth2.credentials import Credentials as UserCredentials
from google_auth_oauthlib.flow import Flow
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseUpload

# ----------------- PAGE -----------------
st.set_page_config(page_title="Jewelry Inventory – Memo Upload", layout="wide")
st.title("📦 Jewelry Inventory – Memo Upload")
st.caption("Upload memo PDF → Google Login OCR → preview → confirm → updates inventory")

# ----------------- SHEETS (SERVICE ACCOUNT) -----------------
SHEETS_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

sa_creds = SACredentials.from_service_account_info(
    st.secrets["gcp_service_account"],
    scopes=SHEETS_SCOPES,
)
gc = gspread.authorize(sa_creds)

SHEET_URL = st.secrets["sheet_url"]
sh = gc.open_by_url(SHEET_URL)

ws_inventory = sh.worksheet("INVENTORY")
ws_log = sh.worksheet("TRANSACTIONS_LOG")

# ----------------- OAUTH (USER LOGIN FOR DRIVE OCR) -----------------
OAUTH_SCOPES = [
    "https://www.googleapis.com/auth/drive",
    "openid",
    "https://www.googleapis.com/auth/userinfo.email",
]

CLIENT_ID = st.secrets["google_oauth_client_id"]
CLIENT_SECRET = st.secrets["google_oauth_client_secret"]
REDIRECT_URI = st.secrets["redirect_uri"]


def build_flow():
    config = {
        "web": {
            "client_id": CLIENT_ID,
            "client_secret": CLIENT_SECRET,
            "auth_uri": "https://accounts.google.com/o/oauth2/auth",
            "token_uri": "https://oauth2.googleapis.com/token",
        }
    }
    return Flow.from_client_config(
        config,
        scopes=OAUTH_SCOPES,
        redirect_uri=REDIRECT_URI,
    )


def handle_oauth_callback():
    params = st.query_params
    code = params.get("code")
    if not code:
        return

    flow = build_flow()
    flow.fetch_token(code=code)

    creds = flow.credentials
    st.session_state["oauth_token"] = {
        "access_token": creds.token,
        "refresh_token": creds.refresh_token,
    }

    st.query_params.clear()
    st.success("Google login successful ✅")


def get_drive_service():
    token = st.session_state.get("oauth_token")
    if not token:
        return None

    creds = UserCredentials(
        token=token["access_token"],
        refresh_token=token.get("refresh_token"),
        token_uri="https://oauth2.googleapis.com/token",
        client_id=CLIENT_ID,
        client_secret=CLIENT_SECRET,
        scopes=OAUTH_SCOPES,
    )
    return build("drive", "v3", credentials=creds)


# Run callback handler first
handle_oauth_callback()

# ----------------- SIDEBAR -----------------
with st.sidebar:
    st.header("Settings")
    employee = st.text_input("Employee name", value="Employee")
    reason = st.selectbox(
        "Reason",
        ["Sale", "Amazon", "Adjustment", "Return", "Damage"],
        index=0,
    )

    if st.session_state.get("oauth_token"):
        if st.button("Log out (Google OCR)"):
            st.session_state.pop("oauth_token", None)
            st.experimental_rerun()

# ----------------- LOGIN GATE -----------------
drive = get_drive_service()

if drive is None:
    st.subheader("🔐 Google Login required for OCR")
    st.info("Google blocks Drive storage for service accounts. Login once to use your Drive for OCR.")

    flow = build_flow()
    auth_url, _ = flow.authorization_url(
        access_type="offline",
        include_granted_scopes="true",
        prompt="consent",
    )

    st.link_button("Login with Google", auth_url)
    st.stop()

# ----------------- REGEX -----------------
ITEM_RE = re.compile(r"\b(?:BR|BS|GB)[2-8][YW]-14K(?:-(?:1|2|3|4))?\b", re.I)
MEMO_RE = re.compile(r"\bMemo\s*#\s*[:\-]?\s*([A-Z0-9\-]+)\b", re.I)


def now_str():
    return datetime.now(timezone.utc).astimezone().strftime("%Y-%m-%d %H:%M:%S")


# ----------------- OCR -----------------
def drive_ocr_pdf_to_text(pdf_bytes: bytes, filename: str) -> str:
    pdf_file_id = None
    doc_file_id = None

    try:
        media = MediaIoBaseUpload(
            io.BytesIO(pdf_bytes),
            mimetype="application/pdf",
            resumable=False,
        )

        pdf_created = drive.files().create(
            body={"name": f"UPLOAD_{filename}_{int(datetime.now().timestamp())}"},
            media_body=media,
            fields="id",
        ).execute()
        pdf_file_id = pdf_created["id"]

        doc_created = drive.files().copy(
            fileId=pdf_file_id,
            body={
                "name": f"OCR_{filename}_{int(datetime.now().timestamp())}",
                "mimeType": "application/vnd.google-apps.document",
            },
            fields="id",
        ).execute()
        doc_file_id = doc_created["id"]

        exported = drive.files().export(
            fileId=doc_file_id,
            mimeType="text/plain",
        ).execute()

        return exported.decode("utf-8", errors="ignore")

    finally:
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


# ----------------- PARSE -----------------
def parse_items(text: str):
    memo_no = None
    m = MEMO_RE.search(text)
    if m:
        memo_no = m.group(1)

    items = {}
    for ln in text.splitlines():
        u = ln.upper()
        if "SHIPPING" in u or "INSURANCE" in u:
            continue

        code = ITEM_RE.search(ln)
        if not code:
            continue

        qty = re.search(r"\b(\d+)\b", ln)
        if not qty:
            continue

        q = int(qty.group(1))
        if q <= 0 or q > 999:
            continue

        items[code.group(0).upper()] = items.get(code.group(0).upper(), 0) + q

    return memo_no, items


# ----------------- INVENTORY -----------------
def read_inventory():
    df = pd.DataFrame(ws_inventory.get_all_records())
    df["item_code"] = df["item_code"].astype(str).str.upper()
    df["on_hand"] = pd.to_numeric(df["on_hand"], errors="coerce").fillna(0).astype(int)
    return df.set_index("item_code")


def memo_exists(memo_no):
    for r in ws_log.get_all_records():
        if str(r.get("memo_no", "")).strip() == str(memo_no):
            return True
    return False


def apply_updates(df_preview, memo_no):
    inv = read_inventory()

    updates = []
    for _, r in df_preview.iterrows():
        code = r["item_code"]
        qty = int(r["qty"])
        new = inv.loc[code, "on_hand"] - qty
        if new < 0:
            raise ValueError(f"Negative stock: {code}")
        updates.append((code, new, -qty))

    inv_all = ws_inventory.get_all_values()
    header = inv_all[0]
    code_col = header.index("item_code") + 1
    qty_col = header.index("on_hand") + 1

    code_row = {
        row[code_col - 1].upper(): i + 2
        for i, row in enumerate(inv_all[1:])
    }

    cells = []
    for code, new_qty, _ in updates:
        cells.append(gspread.Cell(code_row[code], qty_col, str(new_qty)))

    ws_inventory.update_cells(cells)

    ts = now_str()
    logs = []
    for code, _, chg in updates:
        logs.append([ts, "Memo PDF", memo_no or "", code, chg, reason, employee, ""])

    ws_log.append_rows(logs, value_input_option="USER_ENTERED")


# ----------------- UI -----------------
uploaded = st.file_uploader("Upload memo PDF", type=["pdf"])

if uploaded:
    with st.spinner("Running OCR…"):
        text = drive_ocr_pdf_to_text(uploaded.read(), uploaded.name)

    memo_no, items = parse_items(text)

    if not items:
        st.error("No item codes detected.")
        st.stop()

    st.write(f"**Memo #:** {memo_no or 'Not detected'}")

    df_preview = pd.DataFrame(
        [{"item_code": k, "qty": v} for k, v in sorted(items.items())]
    )

    st.subheader("Preview")
    edited = st.data_editor(df_preview, use_container_width=True)

    if memo_no and memo_exists(memo_no):
        st.error("This memo was already processed.")
        st.stop()

    if st.button("✅ Confirm & Update Inventory"):
        try:
            apply_updates(edited, memo_no)
            st.success("Inventory updated successfully 🎉")
        except Exception as e:
            st.error(str(e))

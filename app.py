# app.py — Generator comandă APEX + mapare SKU (Supabase client, fără DATABASE_URL)

import io
import math
import re
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st
from supabase import create_client

# =========================
#   CONFIG & CONSTANTE
# =========================
st.set_page_config(page_title="Generator comandă APEX", layout="wide")
ALLOWED_ROUNDINGS = [1, 3, 5, 10, 20, 50]

st.title("Generator comandă APEX (mapare pe catalog din Supabase)")
st.caption("Folosește view-ul public.v_sku_mapping (primary + aliasuri).")

# =========================
#   SECRETS (Supabase)
# =========================
sb = st.secrets.get("supabase", {})
SUPABASE_URL = sb.get("url", "")
SUPABASE_ANON = sb.get("anon_key", "")

if not SUPABASE_URL or not SUPABASE_ANON:
    st.error("Lipsește [supabase] url / anon_key în Secrets. Exemplu:\n\n[supabase]\nurl = \"https://<proj>.supabase.co\"\nanon_key = \"<ANON>\"")
    st.stop()

client = create_client(SUPABASE_URL, SUPABASE_ANON)

# =========================
#   HELPERS
# =========================
def round_to_allowed(value: float) -> int:
    for t in ALLOWED_ROUNDINGS:
        if value <= t:
            return t
    return ALLOWED_ROUNDINGS[-1]

def compute_order(row: pd.Series) -> int:
    iesiri = row.get("iesiri", 0)
    stoc_final = row.get("stoc final", 0)
    if pd.isna(iesiri) or pd.isna(stoc_final):
        return 0
    if iesiri > stoc_final and iesiri > 0:
        return round_to_allowed(iesiri)
    return 0

def normalize_str_series(s: pd.Series) -> pd.Series:
    return s.astype(str).str.strip()

def canon_sku(x: str) -> str:
    """Curăță codurile în notație științifică (ex: 5.6061E+11) și spațiile."""
    if x is None:
        return ""
    s = str(x).strip().replace(" ", "")
    if s == "":
        return ""
    if re.match(r"^[0-9]+(\.[0-9]+)?[eE]\+[0-9]+$", s):
        try:
            d = Decimal(s)
            s = format(d, 'f').replace(".", "")
        except InvalidOperation:
            pass
    return s

@st.cache_data(ttl=600, show_spinner=False)
def load_sku_mapping_from_supabase() -> pd.DataFrame:
    """Citește mappingul (alias -> primary) din view-ul public.v_sku_mapping, cu paginare."""
    batch = 1000
    start = 0
    rows = []
    while True:
        resp = client.table("v_sku_mapping").select("*").range(start, start + batch - 1).execute()
        data = resp.data or []
        rows.extend(data)
        if len(data) < batch:
            break
        start += batch
    df = pd.DataFrame(rows)
    if df.empty:
        return pd.DataFrame(columns=["sku_any", "primary_sku", "denumire_db"])
    df = df.drop_duplicates(subset=["sku_any"])
    return df[["sku_any", "primary_sku", "denumire_db"]].copy()

# =========================
#   UI INPUT FILES
# =========================
st.subheader("Fișiere de intrare")
c1, c2 = st.columns(2)
with c1:
    apex_file = st.file_uploader("Fișier APEX (.csv)", type=["csv"], key="apex")
with c2:
    smartbill_file = st.file_uploader("Fișier SmartBill (.xlsx sau .xls)", type=["xlsx", "xls"], key="smartbill")

# =========================
#   LOGICĂ PRINCIPALĂ
# =========================
if apex_file and smartbill_file:
    # 0) mapping din DB
    try:
        df_map = load_sku_mapping_from_supabase()
    except Exception as e:
        st.error(f"Nu am putut citi v_sku_mapping: {e}")
        st.stop()

    alt_to_principal = dict(zip(df_map["sku_any"].astype(str), df_map["primary_sku"].astype(str)))
    prim_to_name = dict(zip(
        df_map.drop_duplicates(subset=["primary_sku"])["primary_sku"].astype(str),
        df_map.drop_duplicates(subset=["primary_sku"])["denumire_db"].astype(str)
    ))

    # 1) APEX
    try:
        apex_df = pd.read_csv(apex_file)
    except Exception as e:
        st.error(f"Nu pot citi APEX CSV: {e}")
        st.stop()
    apex_df.columns = apex_df.columns.str.strip().str.lower()
    if "cod" not in apex_df.columns:
        st.error("În APEX lipsește coloana 'cod'.")
        st.stop()
    apex_df["cod"] = normalize_str_series(apex_df["cod"])
    possible_name_cols = ["nume", "denumire", "product name", "nume produs", "produs"]
    name_col_apex = next((c for c in possible_name_cols if c in apex_df.columns), None)

    # 2) SmartBill
    try:
        smart_df = pd.read_excel(smartbill_file)
    except Exception as e:
        st.error("Pentru .xls ai nevoie de xlrd>=2.0.1; pentru .xlsx, de openpyxl. Detalii: {}".format(e))
        st.stop()
    smart_df.columns = smart_df.columns.str.strip().str.lower()
    if "cod" not in smart_df.columns:
        st.error("În SmartBill lipsește coloana 'cod'.")
        st.stop()
    smart_df["cod"] = normalize_str_series(smart_df["cod"])
    for col in ["iesiri", "stoc final"]:
        if col not in smart_df.columns:
            smart_df[col] = 0
        smart_df[col] = pd.to_numeric(smart_df[col], errors="coerce").fillna(0)

    # 3) Canonizare + mapare la SKU principal
    apex_df["cod_canon"]  = apex_df["cod"].map(canon_sku)
    smart_df["cod_canon"] = smart_df["cod"].map(canon_sku)

    apex_df["cod_match"]  = apex_df["cod_canon"].map(alt_to_principal).fillna(apex_df["cod_canon"])
    smart_df["cod_match"] = smart_df["cod_canon"].map(alt_to_principal).fillna(smart_df["cod_canon"])

    # 4) Agregare SmartBill pe cod canonic
    smart_grouped = smart_df.groupby("cod_match", as_index=False)[["iesiri", "stoc final"]].sum()

    # 5) Merge + comandă
    merged = apex_df.merge(smart_grouped, on="cod_match", how="left")
    for col in ["iesiri", "stoc final"]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)
    merged["comanda"] = merged.apply(compute_order, axis=1)

    # 6) Nume DB (după SKU principal)
    merged = merged.rename(columns={"cod_match": "SKU_principal"})
    merged["Produs_DB"] = merged["SKU_principal"].map(prim_to_name)

    # 7) Afișare rezultat
    st.subheader("📦 Rezultat comandă (agregat pe SKU principal)")
    show_cols = ["cod", "SKU_principal", "Produs_DB", "iesiri", "stoc final", "comanda"]
    if name_col_apex:
        show_cols.insert(1, name_col_apex)
    show_cols = [c for c in show_cols if c in merged.columns]
    st.dataframe(merged[show_cols], use_container_width=True)

    # 8) Export CSV
    csv_buffer = io.StringIO()
    merged.to_csv(csv_buffer, index=False)
    st.download_button(
        label="⬇️ Descarcă fișierul pentru furnizor (CSV)",
        data=csv_buffer.getvalue(),
        file_name="apex_comanda.csv",
        mime="text/csv",
    )

    # 9) Raport discrepanțe
    st.subheader("Raport discrepanțe APEX vs SmartBill (după mapare)")
    smart_canon_set = set(smart_grouped["cod_match"].unique())
    apex_canon_set  = set(apex_df["cod_match"].unique())

    in_apex_not_in_smart = apex_df.loc[~apex_df["cod_match"].isin(smart_canon_set), ["cod", "cod_match"]].copy()
    in_apex_not_in_smart["categorie"] = "APEX: lipsește în SmartBill"
    if name_col_apex:
        in_apex_not_in_smart = in_apex_not_in_smart.merge(
            apex_df[["cod", name_col_apex]], on="cod", how="left"
        ).rename(columns={name_col_apex: "nume_apex"})
    in_apex_not_in_smart["iesiri"] = ""
    in_apex_not_in_smart["stoc final"] = ""

    sb_zero = smart_grouped[(smart_grouped["stoc final"] == 0) & (smart_grouped["iesiri"] == 0)].copy()
    sb_zero_in_apex = sb_zero[sb_zero["cod_match"].isin(apex_canon_set)].copy()
    sb_zero_in_apex["categorie"] = "SB: 0 stoc & 0 mișcări"
    if name_col_apex:
        apex_name_by_canon = (
            apex_df.drop_duplicates(subset=["cod_match"])[["cod_match", name_col_apex]]
            .rename(columns={name_col_apex: "nume_apex"})
        )
        sb_zero_in_apex = sb_zero_in_apex.merge(apex_name_by_canon, on="cod_match", how="left")
    apex_rep = apex_df.drop_duplicates(subset=["cod_match"])[["cod_match", "cod"]]
    sb_zero_in_apex = sb_zero_in_apex.merge(apex_rep, on="cod_match", how="left")

    discrepante_cols = ["categorie", "cod", "cod_match", "nume_apex", "iesiri", "stoc final"]
    discrepante = pd.concat(
        [
            in_apex_not_in_smart.reindex(columns=discrepante_cols, fill_value=""),
            sb_zero_in_apex.reindex(columns=discrepante_cols, fill_value=""),
        ],
        ignore_index=True,
    ).sort_values(["categorie", "cod_match", "cod"], kind="stable")

    st.dataframe(discrepante, use_container_width=True)

    disc_buffer = io.StringIO()
    discrepante.to_csv(disc_buffer, index=False)
    st.download_button(
        label="⬇️ Descarcă raport discrepanțe (CSV)",
        data=disc_buffer.getvalue(),
        file_name="apex_smartbill_discrepante.csv",
        mime="text/csv",
    )

else:
    st.info("Încarcă ambele fișiere (APEX CSV + SmartBill XLS/XLSX) pentru a continua.")

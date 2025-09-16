# app.py — Generator comandă APEX (cu mapare SKU din Supabase: primary + aliasuri)

import io
import math
import re
from decimal import Decimal, InvalidOperation

import pandas as pd
import streamlit as st
from sqlalchemy import create_engine, text

# =========================
#   CONFIG & CONSTANTE
# =========================
st.set_page_config(page_title="Generator comandă APEX", layout="wide")
ALLOWED_ROUNDINGS = [1, 3, 5, 10, 20, 50]

st.title("Generator comandă APEX (mapare pe baza catalogului din Supabase)")
st.caption("Folosește catalog.product + catalog.product_sku (primar + aliasuri).")

# =========================
#   HELPER FUNCTIONS
# =========================
def round_to_allowed(value: float) -> int:
    """Rotunjește la cea mai apropiată valoare din lista permisă (ceiling pe praguri)."""
    for threshold in ALLOWED_ROUNDINGS:
        if value <= threshold:
            return threshold
    return ALLOWED_ROUNDINGS[-1]

def compute_order(row: pd.Series) -> int:
    """Heuristică simplă: dacă ieșirile > stoc final => comandă la un prag permis."""
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
    """Curăță SKU: spații, notație științifică (ex: 5.6061E+11) -> cifre fără exponent."""
    if x is None:
        return ""
    s = str(x).strip().replace(" ", "")
    if s == "":
        return ""
    # scientific notation?
    if re.match(r"^[0-9]+(\.[0-9]+)?[eE]\+[0-9]+$", s):
        try:
            d = Decimal(s)  # precizie arbitrară; nu pierdem zerouri interioare
            s = format(d, 'f').replace(".", "")
        except InvalidOperation:
            pass
    return s

# =========================
#   CONEXIUNE DB (Supabase)
# =========================
# Adaugă în .streamlit/secrets.toml:
# DATABASE_URL = "postgresql+psycopg2://postgres:<PAROLA>@<HOST>:6543/postgres"
if "DATABASE_URL" not in st.secrets:
    st.error("Lipsește DATABASE_URL în .streamlit/secrets.toml")
    st.stop()

engine = create_engine(st.secrets["DATABASE_URL"], pool_pre_ping=True)

@st.cache_data(ttl=600, show_spinner=False)
def load_sku_mapping_from_db() -> pd.DataFrame:
    """
    Citește mapping-ul direct din catalog.product & catalog.product_sku.
    Returnează: sku_any, primary_sku, denumire_db.
    """
    with engine.begin() as conn:
        df_map = pd.read_sql(text("""
            select 
                p.id as product_id,
                p.name as denumire_db,
                ps.sku as sku_any,
                max(ps.sku) filter (where ps.is_primary) over (partition by p.id) as primary_sku
            from catalog.product p
            join catalog.product_sku ps on ps.product_id = p.id
        """), conn)
    # asigură unicitate pe fiecare cod (alias sau principal)
    df_map = df_map.drop_duplicates(subset=["sku_any"])
    return df_map[["sku_any", "primary_sku", "denumire_db"]].copy()

# =========================
#   UI INPUT FILES
# =========================
st.subheader("Fișiere de intrare")
left, right = st.columns(2)
with left:
    apex_file = st.file_uploader("Fișier APEX (.csv)", type=["csv"], key="apex")
with right:
    smartbill_file = st.file_uploader("Fișier SmartBill (.xlsx sau .xls)", type=["xlsx", "xls"], key="smartbill")

# =========================
#   LOGICĂ PRINCIPALĂ
# =========================
if apex_file and smartbill_file:
    # 0) Încarcă mapping din DB
    try:
        df_map = load_sku_mapping_from_db()
    except Exception as e:
        st.error(f"Nu am putut citi mapping-ul din DB: {e}")
        st.stop()

    alt_to_principal = dict(zip(df_map["sku_any"].astype(str), df_map["primary_sku"].astype(str)))
    sku_to_name      = dict(zip(df_map["sku_any"].astype(str), df_map["denumire_db"].astype(str)))
    prim_to_name     = dict(zip(
        df_map.drop_duplicates(subset=["primary_sku"])["primary_sku"].astype(str),
        df_map.drop_duplicates(subset=["primary_sku"])["denumire_db"].astype(str)
    ))

    # 1) Citește APEX
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
    # opțional nume produs din APEX (dacă există)
    possible_name_cols = ["nume", "denumire", "product name", "nume produs", "produs"]
    name_col_apex = next((c for c in possible_name_cols if c in apex_df.columns), None)

    # 2) Citește SmartBill
    try:
        smart_df = pd.read_excel(smartbill_file)
    except Exception as e:
        st.error("Pentru .xls ai nevoie de xlrd>=2.0.1; pentru .xlsx, de openpyxl. Detalii: {}".format(e))
        st.stop()
    smart_df.columns = smart_df.columns.str.strip().str.lower()
    if "cod" not in smart_df.columns:
        st.error("În SmartBill lipsește coloana 'cod'.")
        st.stop()
    for col in ["cod"]:
        smart_df[col] = normalize_str_series(smart_df[col])
    # Asigură numeric pt. iesiri + stoc final
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

    # 5) Merge APEX + SB și calc comandă
    merged = apex_df.merge(smart_grouped, on="cod_match", how="left")
    for col in ["iesiri", "stoc final"]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)

    merged["comanda"] = merged.apply(compute_order, axis=1)

    # 6) Anexează nume din DB (după SKU principal)
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

    # 9) Raport discrepanțe / diagnostic mapare
    st.subheader("Raport discrepanțe APEX vs SmartBill (după mapare)")
    smart_canon_set = set(smart_grouped["cod_match"].unique())
    apex_canon_set  = set(apex_df["cod_match"].unique())

    in_apex_not_in_smart = apex_df.loc[~apex_df["cod_match"].isin(smart_canon_set), ["cod", "cod_match"]].copy()
    in_apex_not_in_smart["categorie"] = "APEX: lipsește în SmartBill (după mapare)"
    if name_col_apex:
        in_apex_not_in_smart = in_apex_not_in_smart.merge(
            apex_df[["cod", name_col_apex]], on="cod", how="left"
        ).rename(columns={name_col_apex: "nume_apex"})
    in_apex_not_in_smart["iesiri"] = ""
    in_apex_not_in_smart["stoc final"] = ""

    sb_zero = smart_grouped[(smart_grouped["stoc final"] == 0) & (smart_grouped["iesiri"] == 0)].copy()
    sb_zero_in_apex = sb_zero[sb_zero["cod_match"].isin(apex_canon_set)].copy()
    sb_zero_in_apex["categorie"] = "SB: 0 stoc & 0 mișcări (după mapare)"
    if name_col_apex:
        apex_name_by_canon = (
            apex_df.drop_duplicates(subset=["cod_match"])[["cod_match", name_col_apex]]
            .rename(columns={name_col_apex: "nume_apex"})
        )
        sb_zero_in_apex = sb_zero_in_apex.merge(apex_name_by_canon, on="cod_match", how="left")
    apex_rep = apex_df.drop_duplicates(subset=["cod_match"])[["cod_match", "cod"]]
    sb_zero_in_apex = sb_zero_in_apex.merge(apex_rep, on="cod_match", how="left")

    discrepante_cols_order = ["categorie", "cod", "cod_match", "nume_apex", "iesiri", "stoc final"]
    discrepante = pd.concat(
        [
            in_apex_not_in_smart.reindex(columns=discrepante_cols_order, fill_value=""),
            sb_zero_in_apex.reindex(columns=discrepante_cols_order, fill_value=""),
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

    with st.expander("Diagnostic mapare / seturi unice"):
        st.write("SKU-uri în mapping (DB):", len(df_map))
        st.write("Coduri APEX unice:", apex_df["cod"].nunique())
        st.write("Coduri SmartBill unice:", smart_df["cod"].nunique())
        st.write("Coduri canonice APEX:", apex_df["cod_match"].nunique())
        st.write("Coduri canonice SmartBill:", smart_grouped["cod_match"].nunique())

else:
    st.info("Încarcă ambele fișiere (APEX CSV + SmartBill XLS/XLSX) pentru a continua.")

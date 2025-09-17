# app.py — Generator comandă APEX + normalizare + mapare SKU (Supabase client, fără DATABASE_URL)

import io
import re
import csv
from decimal import Decimal, InvalidOperation, ROUND_HALF_UP

import pandas as pd
import streamlit as st
from supabase import create_client

# =========================
#   CONFIG & CONSTANTE
# =========================
st.set_page_config(page_title="Generator comandă APEX", layout="wide")
ALLOWED_ROUNDINGS = [1, 3, 5, 10, 20, 50]
EUR_TO_RON = Decimal("5.1")

st.title("Generator comandă APEX (normalizare + mapare pe catalog din Supabase)")
st.caption("Pas 1: Normalizare APEX → Pas 2: Mapare pe public.v_sku_mapping + raport.")

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
    """Curăță spații, sufixe paranteze și notație științifică menținând zerourile."""
    if x is None:
        return ""
    s = str(x).strip()
    # elimină orice text din paranteze
    s = re.sub(r"\(.*?\)", "", s).strip()
    # scoate spațiile
    s = s.replace(" ", "")
    if s == "":
        return ""
    # științific (doar cifre + exponent)
    if re.match(r"^[0-9]+(\.[0-9]+)?[eE]\+[0-9]+$", s):
        try:
            d = Decimal(s)
            s = format(d, "f").rstrip("0").rstrip(".")
        except InvalidOperation:
            pass
    return s

def split_and_expand_codes(raw_code: str) -> list:
    """
    Reguli:
      - prefix = tot până la primul '-' din PRIMUL cod; dacă nu există '-', NU folosim prefix.
      - se împarte pe '/', segmentele fără '-' primesc prefix doar dacă există prefix.
      - segmentele care conțin deja '-' rămân așa.
    """
    if pd.isna(raw_code):
        return []
    s = canon_sku(str(raw_code))
    if s == "":
        return []
    parts = [p for p in s.split("/") if p != ""]
    if not parts:
        return []
    first = parts[0]
    m = re.search(r"-")
    prefix = ""
    if m:  # există '-' în primul cod
        # „tot până la primul '-'”, inclusiv '-'
        idx = first.find("-")
        prefix = first[: idx + 1]  # include '-'

    out = []
    for i, p in enumerate(parts):
        p = p.strip()
        if i > 0 and prefix and "-" not in p:
            p = prefix + p
        out.append(canon_sku(p))

    # elimină duplicate păstrând ordinea
    seen, uniq = set(), []
    for c in out:
        if c and c not in seen:
            uniq.append(c)
            seen.add(c)
    return uniq

def parse_decimal_maybe(s: str) -> Decimal:
    """Extrage număr din șiruri de tip '€ 12,34', '12.34', '12,34 EUR' etc."""
    if s is None:
        return Decimal("0")
    txt = str(s).strip()
    # scoate litere, simboluri valută
    txt = re.sub(r"[^\d,.\-]", "", txt)
    # dacă are atât virgulă cât și punct, alegem varianta în care separatorul zecimal pare ultimul
    if "," in txt and "." in txt:
        # dacă ultima apariție e virgulă -> înlocuim punctele (mii)
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "").replace(",", ".")
        else:
            txt = txt.replace(",", "")
    else:
        # doar virgulă -> zecimal
        if "," in txt and "." not in txt:
            txt = txt.replace(",", ".")
    try:
        return Decimal(txt)
    except InvalidOperation:
        return Decimal("0")

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

def read_any_apex(file) -> pd.DataFrame:
    """Citește APEX (xlsx/xls/csv) exact cum vine și întoarce DF cu coloanele brute."""
    name = file.name.lower()
    if name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str)
    else:
        df = pd.read_excel(file, dtype=str)
    df.columns = df.columns.str.strip()
    return df

def normalize_apex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mapează coloanele la setul minim necesar și curăță rândurile fără cod."""
    cols_lower = {c.lower(): c for c in df.columns}
    col_code  = cols_lower.get("product code") or cols_lower.get("product_code") or cols_lower.get("code") or cols_lower.get("cod")
    col_name  = cols_lower.get("product name") or cols_lower.get("product_name") or cols_lower.get("nume") or cols_lower.get("denumire")
    col_qty   = cols_lower.get("quantity")
    col_price = cols_lower.get("euro price") or cols_lower.get("euro_price") or cols_lower.get("price")
    col_order = cols_lower.get("order") or cols_lower.get("comanda") or cols_lower.get("order_hint")

    if not col_code:
        raise ValueError("În APEX nu am găsit coloana «Product Code».")
    keep = [c for c in [col_code, col_name, col_qty, col_price, col_order] if c]
    df2 = df[keep].copy()

    rename = {}
    if col_code:  rename[col_code]  = "cod_raw"
    if col_name:  rename[col_name]  = "nume_apex"
    if col_qty:   rename[col_qty]   = "cantitate"
    if col_price: rename[col_price] = "pret_eur"
    if col_order: rename[col_order] = "order_hint"
    df2 = df2.rename(columns=rename)

    for c in df2.columns:
        df2[c] = df2[c].astype(str).str.strip()

    df2["cod_raw"] = df2["cod_raw"].replace({"nan": "", "None": ""})
    df2 = df2[df2["cod_raw"].astype(str).str.strip() != ""].copy()

    return df2

def expand_apex_rows(df_norm_cols: pd.DataFrame) -> pd.DataFrame:
    """Duplichează rândurile cu coduri multiple separate prin '/', aplicând regulile de prefix."""
    rows = []
    for _, r in df_norm_cols.iterrows():
        codes = split_and_expand_codes(r["cod_raw"])
        if not codes:
            continue
        for c in codes:
            new_r = r.copy()
            new_r["cod"] = c  # cod final normalizat
            rows.append(new_r)
    if not rows:
        return pd.DataFrame(columns=list(df_norm_cols.columns) + ["cod"])
    out = pd.DataFrame(rows)
    out["cod"] = out["cod"].astype(str).str.replace(" ", "", regex=False).str.strip()

    # === PREȚ LEI: pret_eur * 5.1 (Decimal, 2 zecimale) ===
    if "pret_eur" in out.columns:
        eur_num = out["pret_eur"].map(parse_decimal_maybe)
        lei_num = (eur_num * EUR_TO_RON).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        out["pret_lei"] = lei_num.astype(str)  # ca text în CSV
    else:
        out["pret_lei"] = ""

    # curățăm dubluri perfecte
    out = out.drop_duplicates().reset_index(drop=True)
    return out

# =========================
#   UI INPUT FILES
# =========================
st.subheader("Fișiere de intrare")
c1, c2 = st.columns(2)
with c1:
    apex_file = st.file_uploader("Fișier APEX original (.xlsx / .xls / .csv)", type=["xlsx", "xls", "csv"], key="apex_raw")
with c2:
    smartbill_file = st.file_uploader("Fișier SmartBill (.xlsx sau .xls)", type=["xlsx", "xls"], key="smartbill")

# =========================
#   LOGICĂ PRINCIPALĂ
# =========================
apex_df_normalized = None

if apex_file:
    st.markdown("### Pas 1 — Normalizare APEX")
    try:
        apex_raw = read_any_apex(apex_file)
    except Exception as e:
        st.error(f"Nu pot citi fișierul APEX: {e}")
        st.stop()

    try:
        apex_trim = normalize_apex_columns(apex_raw)
        apex_df_normalized = expand_apex_rows(apex_trim)
    except Exception as e:
        st.error(f"Eroare la normalizare APEX: {e}")
        st.stop()

    st.success("APEX a fost normalizat. Rândurile fără «Product Code» eliminate; codurile multiple au fost despărțite conform regulilor; «pret_lei» a fost calculat.")
    cols_show_norm = [c for c in ["cod", "nume_apex", "cantitate", "pret_eur", "pret_lei", "order_hint"] if c in apex_df_normalized.columns]
    st.dataframe(apex_df_normalized[cols_show_norm].fillna(""), use_container_width=True)

    csv_buf = io.StringIO()
    apex_df_normalized.to_csv(csv_buf, index=False, quoting=csv.QUOTE_MINIMAL)
    st.download_button(
        "⬇️ Descarcă APEX normalizat (CSV)",
        data=csv_buf.getvalue(),
        file_name="apex_normalizat.csv",
        mime="text/csv",
    )

if apex_df_normalized is not None and smartbill_file:
    st.markdown("---")
    st.markdown("## Pas 2 — Mapare pe catalog (Supabase) + raport")

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

    # 1) APEX — din DF normalizat
    apex_df = apex_df_normalized.copy()
    apex_df["cod_canon"] = apex_df["cod"].map(canon_sku)
    name_col_apex = "nume_apex" if "nume_apex" in apex_df.columns else None

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
    # includ și prețurile în vizualizare dacă vrei să le vezi aici
    for extra in ["pret_eur", "pret_lei"]:
        if extra in merged.columns and extra not in show_cols:
            show_cols.append(extra)

    show_cols = [c for c in show_cols if c in merged.columns]
    st.dataframe(merged[show_cols], use_container_width=True)

    # 8) Export CSV
    out_csv = io.StringIO()
    merged.to_csv(out_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    st.download_button(
        label="⬇️ Descarcă fișierul pentru furnizor (CSV)",
        data=out_csv.getvalue(),
        file_name="apex_comanda.csv",
        mime="text/csv",
    )

    # 9) Raport discrepanțe
    st.subheader("Raport discrepanțe APEX vs SmartBill (după mapare)")
    smart_canon_set = set(smart_grouped["cod_match"].unique())
    apex_canon_set  = set(apex_df["cod_canon"].map(lambda x: alt_to_principal.get(x, x)).unique())

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
    discrepante.to_csv(disc_buffer, index=False, quoting=csv.QUOTE_MINIMAL)
    st.download_button(
        label="⬇️ Descarcă raport discrepanțe (CSV)",
        data=disc_buffer.getvalue(),
        file_name="apex_smartbill_discrepante.csv",
        mime="text/csv",
    )

else:
    st.info("Încarcă APEX (XLSX/XLS/CSV) pentru normalizare și fișierul SmartBill (.xlsx/.xls) pentru mapare.")

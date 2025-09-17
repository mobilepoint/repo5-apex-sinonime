# app.py — Generator comandă APEX: normalizare (multi-sheet, auto-header) + mapare Supabase

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
    """Curăță spații, texte din paranteze și notație științifică menținând zerourile."""
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).replace("\xa0", " ").strip()        # NBSP -> space
    s = re.sub(r"\(.*?\)", "", s).strip()          # scoate (Asmbld) etc.
    s = s.replace(" ", "")
    if s == "":
        return ""
    if re.match(r"^[0-9]+(\.[0-9]+)?[eE]\+[0-9]+$", s):
        try:
            d = Decimal(s)
            s = format(d, "f").rstrip("0").rstrip(".")
        except InvalidOperation:
            pass
    return s

def split_and_expand_codes(raw_code: str) -> list:
    """
    Reguli de split:
      - prefix = TOT până la primul '-' din primul cod; dacă nu există '-', nu folosim prefix.
      - împărțim pe '/', segmentele fără '-' primesc prefix doar dacă există prefix.
    """
    s = canon_sku(raw_code)
    if s == "":
        return []
    parts = [p for p in s.split("/") if p != ""]
    if not parts:
        return []
    first = parts[0]
    prefix = first[: first.find("-") + 1] if "-" in first else ""
    out = []
    for i, p in enumerate(parts):
        p = p.strip()
        if i > 0 and prefix and "-" not in p:
            p = prefix + p
        out.append(canon_sku(p))
    # dedupe păstrând ordinea
    seen, uniq = set(), []
    for c in out:
        if c and c not in seen:
            uniq.append(c); seen.add(c)
    return uniq

def parse_decimal_maybe(val) -> Decimal:
    """Extrage număr din '€ 12,34', '12.34', '12,34 EUR', '' sau NaN."""
    if val is None or (isinstance(val, float) and pd.isna(val)):
        return Decimal("0")
    txt = str(val).replace("\xa0", " ").strip()
    txt = re.sub(r"[^\d,.\-]", "", txt)
    if txt == "" or txt in {".", ",", "-"}:
        return Decimal("0")
    if "," in txt and "." in txt:
        if txt.rfind(",") > txt.rfind("."):
            txt = txt.replace(".", "").replace(",", ".")
        else:
            txt = txt.replace(",", "")
    elif "," in txt:
        txt = txt.replace(",", ".")
    try:
        return Decimal(txt)
    except InvalidOperation:
        return Decimal("0")

# --------- soft matching pe headere + auto-promote header row ----------
HEADER_VARIANTS = {
    "code":   ["product code", "product_code", "code", "cod", "code no", "prod code", "productcode"],
    "name":   ["product name", "product_name", "name", "nume", "denumire", "description", "descriere"],
    "qty":    ["quantity", "qty", "quant", "q-ty", "qnty"],
    "eur":    ["euro price", "euro pri", "eur price", "euro", "eur", "price(€)", "price €", "€ price", "price eur"],
    "order":  ["order", "ord", "order qty", "order_qty", "comanda", "orderhint", "order hint"],
}

def _clean_header_cell(x) -> str:
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return ""
    s = str(x).replace("\xa0", " ")
    s = re.sub(r"[\r\n]+", " ", s)
    s = re.sub(r"\s+", " ", s)
    return s.strip().lower()

def _find_by_variants(headers_clean: list, keys: list):
    """Întoarce indexul primei potriviri 'soft' (startswith/equals) pentru oricare variantă."""
    for i, h in enumerate(headers_clean):
        for k in keys:
            k0 = k.lower()
            if h == k0 or h.startswith(k0):
                return i
    return None

def _promote_header_row(df: pd.DataFrame):
    """Caută în primele 30 rânduri un rând cu 'product code' + 'product name'."""
    max_scan = min(30, len(df))
    for r in range(max_scan):
        row = df.iloc[r].tolist()
        clean = [_clean_header_cell(x) for x in row]
        if any("product" in c and "code" in c for c in clean) and any("product" in c and "name" in c for c in clean):
            # setăm headerul
            new_headers = [str(x) for x in df.iloc[r].tolist()]
            df2 = df.iloc[r+1:].copy()
            df2.columns = new_headers
            # aruncăm coloanele complet goale
            empty_cols = [c for c in df2.columns if df2[c].astype(str).str.strip().eq("").all()]
            return df2.drop(columns=empty_cols, errors="ignore")
    return df  # dacă nu găsim, lăsăm cum e

@st.cache_data(ttl=600, show_spinner=False)
def load_sku_mapping_from_supabase() -> pd.DataFrame:
    batch, start, rows = 1000, 0, []
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

# --------- citire APEX (multi-sheet) ----------
def read_any_apex(file) -> pd.DataFrame:
    """Citește APEX (xlsx/xls/csv). Dacă e Excel, procesează TOATE foile și le concatenează."""
    name = (file.name or "").lower()
    frames = []
    if name.endswith(".csv"):
        df = pd.read_csv(file, dtype=str)
        frames.append(df)
    else:
        wb = pd.read_excel(file, dtype=str, sheet_name=None)  # dict sheet -> DF
        for sheet, df in wb.items():
            if df is None or df.empty:
                continue
            # dacă primul rând nu pare header, caută-l și promovează-l
            df_fixed = _promote_header_row(df)
            frames.append(df_fixed)
    if not frames:
        return pd.DataFrame()
    df_all = pd.concat(frames, ignore_index=True)
    return df_all

def _safe_header_map(df: pd.DataFrame) -> dict:
    """Map {header_clean: index} pe headerele actuale ale DataFrame-ului."""
    headers = list(df.columns)
    clean = [_clean_header_cell(h) for h in headers]
    return {clean[i]: i for i in range(len(clean))}

def normalize_apex_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Mapează coloanele cheie și elimină rândurile fără cod (robust la denumiri prescurtate)."""
    if df.empty:
        raise ValueError("Fișierul APEX nu conține date.")
    # asigură headerele ca șiruri
    df = df.copy()
    df.columns = [str(c) for c in df.columns]

    head_clean = [_clean_header_cell(c) for c in df.columns]

    # găsește indexii col. dorite (soft-match)
    idx_code  = _find_by_variants(head_clean, HEADER_VARIANTS["code"])
    idx_name  = _find_by_variants(head_clean, HEADER_VARIANTS["name"])
    idx_qty   = _find_by_variants(head_clean, HEADER_VARIANTS["qty"])
    idx_eur   = _find_by_variants(head_clean, HEADER_VARIANTS["eur"])
    idx_order = _find_by_variants(head_clean, HEADER_VARIANTS["order"])

    if idx_code is None:
        # ultimul fallback: poate headerul e într-un rând de date — mai încercăm promovare încă o dată
        df2 = _promote_header_row(df)
        if df2 is not df:
            return normalize_apex_columns(df2)  # recursie scurtă
        raise ValueError("În APEX nu am găsit coloana «Product Code».")
    # construiește DF minim
    cols = []
    rename = {}
    def _add(idx, new_name):
        if idx is not None:
            cols.append(df.columns[idx])
            rename[df.columns[idx]] = new_name

    _add(idx_code, "cod_raw")
    _add(idx_name, "nume_apex")
    _add(idx_qty, "cantitate")
    _add(idx_eur, "pret_eur")
    _add(idx_order, "order_hint")

    out = df[cols].copy().rename(columns=rename)
    for c in out.columns:
        out[c] = out[c].astype(str).str.replace("\xa0", " ").str.strip()

    out["cod_raw"] = out["cod_raw"].replace({"nan": "", "None": ""})
    out = out[out["cod_raw"].astype(str).str.strip() != ""].copy()
    return out

def expand_apex_rows(df_norm_cols: pd.DataFrame) -> pd.DataFrame:
    """Duplichează rândurile cu coduri multiple separate pe '/', aplicând regulile de prefix."""
    rows = []
    for _, r in df_norm_cols.iterrows():
        codes = split_and_expand_codes(r["cod_raw"])
        if not codes:
            continue
        for c in codes:
            new_r = r.copy()
            new_r["cod"] = c
            rows.append(new_r)

    if not rows:
        return pd.DataFrame(columns=list(df_norm_cols.columns) + ["cod"])

    out = pd.DataFrame(rows)
    out["cod"] = out["cod"].astype(str).str.replace(" ", "", regex=False).str.strip()

    # === PREȚ LEI: pret_eur * 5.1 (2 zecimale) ===
    if "pret_eur" in out.columns:
        eur_num = out["pret_eur"].apply(parse_decimal_maybe)
        out["pret_lei"] = eur_num.apply(lambda x: (x * EUR_TO_RON).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)).astype(str)
    else:
        out["pret_lei"] = ""

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
        apex_raw = read_any_apex(apex_file)           # citește toate foile & auto-header
        apex_trim = normalize_apex_columns(apex_raw)  # mapare coloane cheie (soft)
        apex_df_normalized = expand_apex_rows(apex_trim)
    except Exception as e:
        st.error(f"Eroare la normalizare APEX: {e}")
        st.stop()

    st.success("APEX normalizat: rânduri fără «Product Code» eliminate; codurile multiple separate; «pret_lei» calculat.")
    cols_show_norm = [c for c in ["cod", "nume_apex", "cantitate", "pret_eur", "pret_lei", "order_hint"] if c in apex_df_normalized.columns]
    st.dataframe(apex_df_normalized[cols_show_norm].fillna(""), use_container_width=True)

    csv_buf = io.StringIO()
    apex_df_normalized.to_csv(csv_buf, index=False, quoting=csv.QUOTE_MINIMAL)
    st.download_button("⬇️ Descarcă APEX normalizat (CSV)", data=csv_buf.getvalue(), file_name="apex_normalizat.csv", mime="text/csv")

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

    # 3) Canonizare + mapare
    smart_df["cod_canon"] = smart_df["cod"].map(canon_sku)
    apex_df["cod_match"]  = apex_df["cod_canon"].map(alt_to_principal).fillna(apex_df["cod_canon"])
    smart_df["cod_match"] = smart_df["cod_canon"].map(alt_to_principal).fillna(smart_df["cod_canon"])

    # 4) Agregare SmartBill
    smart_grouped = smart_df.groupby("cod_match", as_index=False)[["iesiri", "stoc final"]].sum()

    # 5) Merge + comandă
    merged = apex_df.merge(smart_grouped, on="cod_match", how="left")
    for col in ["iesiri", "stoc final"]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)
    merged["comanda"] = merged.apply(compute_order, axis=1)

    # 6) Nume DB
    merged = merged.rename(columns={"cod_match": "SKU_principal"})
    merged["Produs_DB"] = merged["SKU_principal"].map(prim_to_name)

    # 7) Afișare
    st.subheader("📦 Rezultat comandă (agregat pe SKU principal)")
    show_cols = ["cod", "SKU_principal", "Produs_DB", "iesiri", "stoc final", "comanda"]
    if name_col_apex:
        show_cols.insert(1, name_col_apex)
    for extra in ["pret_eur", "pret_lei"]:
        if extra in merged.columns and extra not in show_cols:
            show_cols.append(extra)
    show_cols = [c for c in show_cols if c in merged.columns]
    st.dataframe(merged[show_cols], use_container_width=True)

    # 8) Export CSV
    out_csv = io.StringIO()
    merged.to_csv(out_csv, index=False, quoting=csv.QUOTE_MINIMAL)
    st.download_button("⬇️ Descarcă fișierul pentru furnizor (CSV)", data=out_csv.getvalue(), file_name="apex_comanda.csv", mime="text/csv")

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
    st.download_button("⬇️ Descarcă raport discrepanțe (CSV)", data=disc_buffer.getvalue(), file_name="apex_smartbill_discrepante.csv", mime="text/csv")

else:
    st.info("Încarcă APEX (XLSX/XLS/CSV) pentru normalizare și fișierul SmartBill (.xlsx/.xls) pentru mapare.")

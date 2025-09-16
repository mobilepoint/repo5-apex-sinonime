import io
import pandas as pd
import streamlit as st

ALLOWED_ROUNDINGS = [1, 3, 5, 10, 20, 50]
st.set_page_config(page_title="Generator comandă APEX", layout="wide")

# =============== HELPERS ===============
def round_to_allowed(value: float) -> int:
    """Rotunjește la cea mai apropiată valoare din lista permisă (ceiling pe praguri)."""
    for threshold in ALLOWED_ROUNDINGS:
        if value <= threshold:
            return threshold
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

@st.cache_data(show_spinner=False)
def load_sku_alternative_from_supabase(url: str, anon_key: str) -> pd.DataFrame:
    """Citește tabelul sku_alternative din Supabase și normalizează coloanele.
       Headere acceptate: COD ALTERNATIV, COD PRINCIPAL, NUME
    """
    from supabase import create_client
    client = create_client(url, anon_key)
    resp = client.table("sku_alternative").select("*").execute()
    data = resp.data or []
    df = pd.DataFrame(data)
    if df.empty:
        return pd.DataFrame(columns=["cod alternativ", "cod principal", "nume"])

    # Normalizează headerele
    df.columns = df.columns.str.strip().str.lower()
    rename_map = {}
    for col in df.columns:
        if col.replace("_", " ") == "cod alternativ":
            rename_map[col] = "cod alternativ"
        if col.replace("_", " ") == "cod principal":
            rename_map[col] = "cod principal"
        if col in {"nume", "nume produs", "denumire"}:
            rename_map[col] = "nume"
    if rename_map:
        df = df.rename(columns=rename_map)

    required = {"cod alternativ", "cod principal"}
    if not required.issubset(df.columns):
        # întoarce cadru gol dar cu headerele potrivite, ca să nu pice aplicația
        return pd.DataFrame(columns=["cod alternativ", "cod principal", "nume"])

    df["cod alternativ"] = normalize_str_series(df["cod alternativ"])
    df["cod principal"] = normalize_str_series(df["cod principal"])
    if "nume" in df.columns:
        df["nume"] = normalize_str_series(df["nume"])
    df = df[(df["cod alternativ"] != "") & (df["cod principal"] != "")]
    return df

# =============== SECRETS ===============
SUPABASE_URL = st.secrets.get("supabase", {}).get("url", "")
SUPABASE_ANON = st.secrets.get("supabase", {}).get("anon_key", "")

if not SUPABASE_URL or not SUPABASE_ANON:
    st.error("Completează .streamlit/secrets.toml cu supabase.url și supabase.anon_key.")
    st.stop()

# =============== UI ===============
st.title("Generator comandă APEX (cu sinonime din Supabase)")
st.write("Încarcă APEX (CSV) și SmartBill (Excel). Maparea de sinonime SKU se citește din `sku_alternative` (Supabase).")

apex_file = st.file_uploader("Fișier APEX (.csv)", type=["csv"])
smartbill_file = st.file_uploader("Fișier SmartBill (.xlsx, .xls)", type=["xlsx", "xls"])

if apex_file and smartbill_file:
    # --- APEX
    apex_df = pd.read_csv(apex_file)
    apex_df.columns = apex_df.columns.str.strip().str.lower()
    if "cod" not in apex_df.columns:
        st.error("În APEX lipsește coloana 'cod'.")
        st.stop()
    apex_df["cod"] = normalize_str_series(apex_df["cod"])
    possible_name_cols = ["nume", "denumire", "product name", "nume produs", "produs"]
    name_col_apex = next((c for c in possible_name_cols if c in apex_df.columns), None)

    # --- SmartBill
    try:
        smart_df = pd.read_excel(smartbill_file)
    except Exception as e:
        st.error("Pentru .xls ai nevoie de `xlrd>=2.0.1`. Pentru .xlsx, de `openpyxl`.\nDetalii: {}".format(e))
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

    # --- Sinonime din Supabase
    try:
        sku_alt_df = load_sku_alternative_from_supabase(SUPABASE_URL, SUPABASE_ANON)
    except Exception as e:
        st.error(f"Nu am putut citi `sku_alternative` din Supabase: {e}")
        st.stop()

    alt_to_principal = {}
    if not sku_alt_df.empty:
        alt_to_principal = dict(
            sku_alt_df.drop_duplicates(subset=["cod alternativ"]).set_index("cod alternativ")["cod principal"]
        )

    # --- cod_match
    apex_df["cod_match"] = apex_df["cod"].map(alt_to_principal).fillna(apex_df["cod"])
    smart_df["cod_match"] = smart_df["cod"].map(alt_to_principal).fillna(smart_df["cod"])

    # --- Agregare SmartBill pe cod canonic
    smart_grouped = smart_df.groupby("cod_match", as_index=False)[["iesiri", "stoc final"]].sum()

    # --- Merge + comandă
    merged = apex_df.merge(smart_grouped, on="cod_match", how="left")
    for col in ["iesiri", "stoc final"]:
        if col not in merged.columns:
            merged[col] = 0
        merged[col] = pd.to_numeric(merged[col], errors="coerce").fillna(0)
    merged["comanda"] = merged.apply(compute_order, axis=1)

    st.subheader("Rezultat comandă")
    show_cols = ["cod", "cod_match", "iesiri", "stoc final", "comanda"]
    if name_col_apex:
        show_cols.insert(1, name_col_apex)
    show_cols = [c for c in show_cols if c in merged.columns]
    st.dataframe(merged[show_cols], use_container_width=True)

    # --- CSV principal
    csv_buffer = io.StringIO()
    merged.to_csv(csv_buffer, index=False)
    st.download_button(
        label="Descarcă fișierul pentru furnizor (CSV)",
        data=csv_buffer.getvalue(),
        file_name="apex_comanda.csv",
        mime="text/csv",
    )

    # =========================
    #   RAPORT DISCREPANȚE (cu sinonime)
    # =========================
    smart_canon_set = set(smart_grouped["cod_match"].unique())
    apex_canon_set = set(apex_df["cod_match"].unique())

    in_apex_not_in_smart = apex_df.loc[~apex_df["cod_match"].isin(smart_canon_set), ["cod", "cod_match"]].copy()
    in_apex_not_in_smart["categorie"] = "Lipsește în SmartBill (după mapare)"
    if name_col_apex:
        in_apex_not_in_smart = in_apex_not_in_smart.merge(
            apex_df[["cod", name_col_apex]], on="cod", how="left"
        ).rename(columns={name_col_apex: "nume_apex"})
    in_apex_not_in_smart["iesiri"] = ""
    in_apex_not_in_smart["stoc final"] = ""

    sb_zero = smart_grouped[(smart_grouped["stoc final"] == 0) & (smart_grouped["iesiri"] == 0)].copy()
    sb_zero_in_apex = sb_zero[sb_zero["cod_match"].isin(apex_canon_set)].copy()
    sb_zero_in_apex["categorie"] = "SB 0 stoc & 0 mișcări (după mapare)"
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

    st.subheader("Raport discrepanțe APEX vs SmartBill (cu sinonime)")
    st.dataframe(discrepante, use_container_width=True)

    disc_buffer = io.StringIO()
    discrepante.to_csv(disc_buffer, index=False)
    st.download_button(
        label="Descarcă raport discrepanțe (CSV)",
        data=disc_buffer.getvalue(),
        file_name="apex_smartbill_discrepante.csv",
        mime="text/csv",
    )

    with st.expander("Diagnostic mapare sinonime"):
        st.write("Rânduri în `sku_alternative`:", len(sku_alt_df))
        if not sku_alt_df.empty:
            st.dataframe(sku_alt_df.head(50), use_container_width=True)
        st.write("Coduri APEX unice:", apex_df["cod"].nunique())
        st.write("Coduri SmartBill unice:", smart_df["cod"].nunique())
        st.write("Coduri canonice APEX:", apex_df["cod_match"].nunique())
        st.write("Coduri canonice SmartBill:", smart_grouped["cod_match"].nunique())

else:
    st.info("Încarcă ambele fișiere pentru a continua.")

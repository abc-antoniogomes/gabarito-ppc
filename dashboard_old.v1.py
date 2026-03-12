"""
streamlit.py
------------
Dashboard de Validação de Gabarito — MYSA.

Lê:
    - gabarito.csv
    - imagens_mdm.csv
    - validações do Google Sheets

Salva:
    - Google Sheets (a cada clique em "Aplicar alterações deste EAN")
    - gabarito_validado.csv (apenas snapshot local opcional)

Execução:
    streamlit run dashboard.py
"""

import os
import re
from datetime import datetime, timezone
from typing import Optional

import gspread
import pandas as pd
import streamlit as st
from google.oauth2.service_account import Credentials

# ─────────────────────────────────────────────────────────────────────────────
# CONFIG
# ─────────────────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="Validação de Gabarito | MYSA DEXCO",
    page_icon="✅",
    layout="wide",
    initial_sidebar_state="collapsed",
)

st.markdown(
    """
    <style>
      .section-header {
        font-size: 0.75rem;
        font-weight: 700;
        letter-spacing: 0.10em;
        text-transform: uppercase;
        opacity: 0.70;
        margin-bottom: 4px;
      }
      .mono-tag {
        font-family: ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace;
        font-size: 0.85rem;
        padding: 2px 8px;
        border-radius: 8px;
        border: 1px solid rgba(120,120,120,0.25);
        display: inline-block;
      }
    </style>
    """,
    unsafe_allow_html=True,
)

ARQUIVO_GABARITO = "gabarito.csv"
ARQUIVO_SAIDA = "gabarito_validado.csv"
ARQUIVO_IMAGENS_MDM = "imagens_mdm.csv"
ITENS_POR_PAGINA = 25

FAB_COL_CANDIDATES = [
    "MDM_CodigoFabricante",
    "CodigoProdutoFabricante"
]

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

# ─────────────────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────────────────
def _to_sim_nao(v) -> str:
    return "Sim" if v in (True, 1, "Sim", "sim", "SIM") else "Não"


def limpar_ean(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip()
    if not s:
        return ""
    if s.endswith(".0"):
        s = s[:-2]
    try:
        return str(int(float(s)))
    except Exception:
        return re.sub(r"\s+", "", s)


def limpar_vendedor(v) -> str:
    if pd.isna(v):
        return ""
    s = str(v).strip().upper()
    return "" if s.lower() in ("", "nan", "none") else s


def formata_preco(preco_raw) -> str:
    if pd.isna(preco_raw) or str(preco_raw).strip() in ("", "nan"):
        return "—"
    try:
        s = str(preco_raw).strip().replace("R$", "").strip()
        if "," in s:
            s = s.replace(".", "").replace(",", ".")
        valor = float(s)
        return f"R$ {valor:,.2f}".replace(",", "X").replace(".", ",").replace("X", ".")
    except Exception:
        return str(preco_raw)


# ─────────────────────────────────────────────────────────────────────────────
# GOOGLE SHEETS
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_resource
def get_gspread_client():
    creds = Credentials.from_service_account_info(
        dict(st.secrets["gcp_service_account"]),
        scopes=SCOPES,
    )
    return gspread.authorize(creds)


@st.cache_resource
def get_worksheet():
    client = get_gspread_client()
    spreadsheet_name = st.secrets["app"]["spreadsheet_name"]
    worksheet_name = st.secrets["app"]["worksheet_name"]
    spreadsheet = client.open(spreadsheet_name)
    worksheet = spreadsheet.worksheet(worksheet_name)
    return worksheet


@st.cache_data(ttl=30, show_spinner="Carregando validações do Google Sheets…")
def carregar_validacoes_sheet() -> pd.DataFrame:
    ws = get_worksheet()
    records = ws.get_all_records()

    if not records:
        return pd.DataFrame(
            columns=["row_id", "Revisado", "Valido", "updated_at", "updated_by"]
        )

    dfv = pd.DataFrame(records, dtype=str)

    cols_necessarias = ["row_id", "Revisado", "Valido", "updated_at", "updated_by"]
    for c in cols_necessarias:
        if c not in dfv.columns:
            dfv[c] = ""

    dfv["row_id"] = dfv["row_id"].astype(str).str.strip()
    dfv["Revisado"] = dfv["Revisado"].apply(_to_sim_nao)
    dfv["Valido"] = dfv["Valido"].apply(_to_sim_nao)

    return dfv[cols_necessarias]


def aplicar_validacoes_do_sheet(df_base: pd.DataFrame) -> pd.DataFrame:
    dfv = carregar_validacoes_sheet()
    if dfv.empty:
        return df_base

    df = df_base.copy()
    df["row_id"] = df["row_id"].astype(str)

    mapa = dfv.set_index("row_id")[["Revisado", "Valido"]].to_dict("index")

    for i in df.index:
        rid = str(df.loc[i, "row_id"])
        if rid in mapa:
            revisado = mapa[rid].get("Revisado", "")
            valido = mapa[rid].get("Valido", "")

            if revisado in ("Sim", "Não"):
                df.loc[i, "Revisado"] = revisado
            if valido in ("Sim", "Não"):
                df.loc[i, "Valido"] = valido

    return df


def salvar_alteracoes_no_sheet(alteracoes: dict, df_work: pd.DataFrame) -> int:
    if not alteracoes:
        return 0

    ws = get_worksheet()
    records = ws.get_all_records()

    if records:
        df_exist = pd.DataFrame(records, dtype=str)
    else:
        df_exist = pd.DataFrame(
            columns=["row_id", "Revisado", "Valido", "updated_at", "updated_by"]
        )

    for c in ["row_id", "Revisado", "Valido", "updated_at", "updated_by"]:
        if c not in df_exist.columns:
            df_exist[c] = ""

    df_exist["row_id"] = df_exist["row_id"].astype(str).str.strip()

    agora = datetime.now(timezone.utc).astimezone().isoformat(timespec="seconds")
    usuario = st.secrets["app"].get("usuario", "cliente")

    mapa_idx = {
        str(row["row_id"]).strip(): idx
        for idx, row in df_exist.iterrows()
    }

    for idx_int, alt in alteracoes.items():
        row_id = str(df_work.loc[idx_int, "row_id"]).strip()

        payload = {
            "row_id": row_id,
            "Revisado": alt.get("Revisado", "Não"),
            "Valido": alt.get("Valido", "Não"),
            "updated_at": agora,
            "updated_by": usuario,
        }

        if row_id in mapa_idx:
            linha = mapa_idx[row_id]
            df_exist.loc[linha, ["Revisado", "Valido", "updated_at", "updated_by"]] = [
                payload["Revisado"],
                payload["Valido"],
                payload["updated_at"],
                payload["updated_by"],
            ]
        else:
            df_exist = pd.concat([df_exist, pd.DataFrame([payload])], ignore_index=True)

    df_exist = df_exist[
        ["row_id", "Revisado", "Valido", "updated_at", "updated_by"]
    ].fillna("")

    ws.clear()
    ws.update([df_exist.columns.tolist()] + df_exist.values.tolist())

    carregar_validacoes_sheet.clear()
    return len(alteracoes)


# ─────────────────────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────────────────────
@st.cache_data(show_spinner="Carregando gabarito…")
def carregar_gabarito(caminho: str) -> pd.DataFrame:
    df = pd.read_csv(caminho, sep=";", encoding="utf-8-sig", dtype=str)

    if "row_id" not in df.columns:
        df.insert(0, "row_id", df.index.astype(str))
    else:
        df["row_id"] = df["row_id"].astype(str)

    base_columns = list(df.columns)

    if "Revisado" not in df.columns:
        df["Revisado"] = "Não"
    else:
        df["Revisado"] = df["Revisado"].apply(_to_sim_nao)

    if "Valido" not in df.columns:
        df["Valido"] = "Não"
    else:
        df["Valido"] = df["Valido"].apply(_to_sim_nao)

    if "EAN_Pesquisado" in df.columns:
        df["EAN_LIMPO"] = df["EAN_Pesquisado"].apply(limpar_ean)
    else:
        df["EAN_LIMPO"] = ""

    if "Vendedor" in df.columns:
        df["Vendedor"] = df["Vendedor"].apply(limpar_vendedor)
    else:
        df["Vendedor"] = pd.Series([""] * len(df), index=df.index)

    df["VEND_LIMPO"] = df["Vendedor"]

    fab_col = next((c for c in FAB_COL_CANDIDATES if c in df.columns), None)
    df["FAB_LIMPO"] = (
        df[fab_col].fillna("").astype(str).str.strip() if fab_col else ""
    )

    blob_cols = [
        c for c in ["Nome_do_Anuncio", "NomeTop", "MDM_Fabricante", "Vendedor"]
        if c in df.columns
    ]
    if blob_cols:
        df["SEARCH_BLOB"] = df[blob_cols].fillna("").agg(" | ".join, axis=1)
    else:
        df["SEARCH_BLOB"] = pd.Series([""] * len(df), index=df.index)

    df.attrs["base_columns"] = base_columns
    return df


@st.cache_data(show_spinner="Carregando imagens ...")
def carregar_imagens_mdm(caminho: str) -> tuple[dict, dict]:
    if not os.path.exists(caminho):
        return {}, {}

    try:
        df = pd.read_csv(caminho, sep=None, engine="python", dtype=str, encoding="utf-8")
    except Exception:
        df = None
        for sep in [",", ";", "\t", "|"]:
            try:
                df = pd.read_csv(caminho, sep=sep, dtype=str, encoding="utf-8")
                break
            except Exception:
                continue
        if df is None:
            return {}, {}

    cols_lower = {c.lower(): c for c in df.columns}
    col_ean = cols_lower.get("ean") or cols_lower.get("ean_pesquisado")
    col_fab = (
        cols_lower.get("codigoprodutofabricante")
        or cols_lower.get("codigo_fabricante")
        or cols_lower.get("codfabricante")
    )
    col_img = (
        cols_lower.get("imagem")
        or cols_lower.get("image")
        or cols_lower.get("url")
        or cols_lower.get("imagem_url")
    )

    if not (col_ean and col_img):
        return {}, {}

    ean_map: dict[str, str] = {}
    fab_map: dict[str, str] = {}

    for _, row in df.iterrows():
        ean = limpar_ean(row.get(col_ean, ""))
        img = str(row.get(col_img, "")).strip()
        fab = str(row.get(col_fab, "")).strip() if col_fab else ""

        if not img:
            continue
        if ean:
            ean_map[ean] = img
        if fab:
            fab_map[fab] = img

    return ean_map, fab_map


def get_imagem_mdm(ean: str, fab_code: str = "") -> Optional[str]:
    ean_map, fab_map = carregar_imagens_mdm(ARQUIVO_IMAGENS_MDM)
    return ean_map.get(ean) or fab_map.get(fab_code) or None


# ─────────────────────────────────────────────────────────────────────────────
# INÍCIO DO APP
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("## ✅ Validação de Gabarito")
st.markdown(
    "Valide **um EAN por vez**. Marque **Revisado** e **Válido** por linha. "
    "As alterações são salvas automaticamente no **Google Sheets** ao clicar em "
    "**Aplicar alterações deste EAN**."
)
st.caption("A base do gabarito continua local; o status de validação fica centralizado no Google Sheets.")
st.divider()

if not os.path.exists(ARQUIVO_GABARITO):
    st.error(
        f"❌ Arquivo `{ARQUIVO_GABARITO}` não encontrado.\n\n"
        "Execute primeiro: `py gerar_gabarito.py`"
    )
    st.stop()

try:
    df_original = carregar_gabarito(ARQUIVO_GABARITO)
    df_original = aplicar_validacoes_do_sheet(df_original)
except Exception as e:
    st.error(f"❌ Erro ao carregar dados: {e}")
    st.stop()

base_columns = df_original.attrs.get("base_columns", list(df_original.columns))
if "row_id" not in base_columns:
    base_columns = ["row_id"] + base_columns

helper_cols_required = {"EAN_LIMPO", "VEND_LIMPO", "FAB_LIMPO", "SEARCH_BLOB"}

if "alteracoes" not in st.session_state:
    st.session_state["alteracoes"] = {}

if (
    "df_work" not in st.session_state
    or not isinstance(st.session_state["df_work"], pd.DataFrame)
    or not helper_cols_required.issubset(set(st.session_state["df_work"].columns))
):
    st.session_state["df_work"] = df_original.copy()
    st.session_state["base_columns"] = base_columns

df_work = st.session_state["df_work"]

if "EAN_Pesquisado" not in df_work.columns:
    st.error("❌ Coluna `EAN_Pesquisado` não existe no gabarito.csv.")
    st.stop()

fab_col = next((c for c in FAB_COL_CANDIDATES if c in df_work.columns), None)

# Métricas globais
total_linhas = len(df_work)
revisados = int((df_work["Revisado"] == "Sim").sum())
validados = int((df_work["Valido"] == "Sim").sum())
pendentes = total_linhas - revisados

# ─────────────────────────────────────────────────────────────────────────────
# FILTROS + BUSCA
# ─────────────────────────────────────────────────────────────────────────────
with st.container(border=True):
    c1, c2, c3, c4 = st.columns([1.4, 1.4, 2.2, 3.0])

    with c1:
        st.markdown('<div class="section-header">Revisado</div>', unsafe_allow_html=True)
        status_filtro = st.radio(
            "status",
            ["Todos", "Pendentes", "Revisados"],
            index=1,
            horizontal=True,
            label_visibility="collapsed",
        )

    with c2:
        st.markdown('<div class="section-header">Válido</div>', unsafe_allow_html=True)
        valido_filtro = st.radio(
            "valido",
            ["Todos", "Sim", "Não"],
            index=0,
            horizontal=True,
            label_visibility="collapsed",
        )

    with c3:
        st.markdown('<div class="section-header">Resumo</div>', unsafe_allow_html=True)
        st.write(
            f"Linhas: **{total_linhas}** · Revisados: **{revisados}** · "
            f"Validados: **{validados}** · Pendentes: **{pendentes}**"
        )

    with c4:
        st.markdown('<div class="section-header">Buscar por EAN ou código interno</div>', unsafe_allow_html=True)
        busca = st.text_input(
            "busca",
            placeholder="Ex: 7894202013806 (EAN) ou 4900.C109.GD (cód. fabricante)",
            label_visibility="collapsed",
        ).strip()

vendedores = sorted([v for v in df_work["VEND_LIMPO"].unique() if v])
vendedor_filtro = st.selectbox("🏪 Filtrar vendedor (opcional)", ["Todos"] + vendedores)

df_candidates = df_work.copy()

if vendedor_filtro != "Todos":
    df_candidates = df_candidates[df_candidates["VEND_LIMPO"] == vendedor_filtro]

if status_filtro == "Pendentes":
    df_candidates = df_candidates[df_candidates["Revisado"] == "Não"]
elif status_filtro == "Revisados":
    df_candidates = df_candidates[df_candidates["Revisado"] == "Sim"]

if valido_filtro in ("Sim", "Não"):
    df_candidates = df_candidates[df_candidates["Valido"] == valido_filtro]

eans_fila = [e for e in df_candidates["EAN_LIMPO"].unique() if e]


def resolve_busca(q: str) -> list[str]:
    if not q:
        return eans_fila

    q_ean = limpar_ean(q)
    if q_ean and q_ean in df_work["EAN_LIMPO"].values:
        return [q_ean]

    if fab_col:
        mask = df_work["FAB_LIMPO"].str.contains(q, case=False, na=False)
        eans = [e for e in df_work.loc[mask, "EAN_LIMPO"].unique() if e]
        if eans:
            return eans

    mask2 = df_work["SEARCH_BLOB"].str.contains(q, case=False, na=False)
    return [e for e in df_work.loc[mask2, "EAN_LIMPO"].unique() if e]


eans_unicos = resolve_busca(busca)

if busca:
    eans_recorte = set(df_candidates["EAN_LIMPO"].unique())
    if any(e in eans_recorte for e in eans_unicos):
        eans_unicos = [e for e in eans_unicos if e in eans_recorte]

if not eans_unicos:
    st.info("Nenhum EAN encontrado com esses filtros/busca.")
    st.stop()


def ean_sort_key(e: str):
    bloco = df_work[df_work["EAN_LIMPO"] == e]
    pend = (bloco["Revisado"] == "Não").any()
    try:
        num = int(e)
    except Exception:
        num = 10**30
    return (-int(pend), num)


eans_unicos = sorted(eans_unicos, key=ean_sort_key)

# Controle de posição na fila
filtro_sig = (status_filtro, valido_filtro, vendedor_filtro, busca)

if "filtro_sig" not in st.session_state:
    st.session_state["filtro_sig"] = None
if "ean_pos" not in st.session_state:
    st.session_state["ean_pos"] = 0
if "pagina_anuncios" not in st.session_state:
    st.session_state["pagina_anuncios"] = 0
if "ean_paginado" not in st.session_state:
    st.session_state["ean_paginado"] = None

if st.session_state["filtro_sig"] != filtro_sig:
    st.session_state["ean_pos"] = 0
    st.session_state["pagina_anuncios"] = 0
    st.session_state["filtro_sig"] = filtro_sig

st.session_state["ean_pos"] = min(max(st.session_state["ean_pos"], 0), len(eans_unicos) - 1)
ean_atual = eans_unicos[st.session_state["ean_pos"]]

if st.session_state["ean_paginado"] != ean_atual:
    st.session_state["pagina_anuncios"] = 0
    st.session_state["ean_paginado"] = ean_atual

# ─────────────────────────────────────────────────────────────────────────────
# EAN ATUAL — PAINEL SUPERIOR
# ─────────────────────────────────────────────────────────────────────────────
df_ean = df_work[df_work["EAN_LIMPO"] == ean_atual].copy()
row0 = df_ean.iloc[0]

mdm_nome = str(row0.get("MDM_Nome", "—")).strip() or "—"
mdm_fabricante = str(row0.get("MDM_Fabricante", "—")).strip() or "—"
fab_code = str(row0.get(fab_col, "")).strip() if fab_col else ""

ean_total = len(df_ean)
ean_revisados = int((df_ean["Revisado"] == "Sim").sum())
ean_validos = int((df_ean["Valido"] == "Sim").sum())
ean_pct_rev = ean_revisados / ean_total if ean_total else 0

img_mdm = get_imagem_mdm(ean_atual, fab_code)

with st.container(border=True):
    cA, cB, cC = st.columns([2.6, 1.2, 1.2])

    with cA:
        st.markdown('<div class="section-header">EAN pesquisado</div>', unsafe_allow_html=True)
        st.markdown(f"### {ean_atual}")
        if fab_col and fab_code:
            st.markdown(
                f'<span class="mono-tag">Cód. fabricante: {fab_code}</span>',
                unsafe_allow_html=True,
            )
        st.write(f"**{mdm_nome}**")
        st.caption(f"Fabricante: {mdm_fabricante}")

    with cB:
        st.markdown('<div class="section-header">Progresso</div>', unsafe_allow_html=True)
        st.progress(ean_pct_rev)
        st.write(f"Revisados: **{ean_revisados}/{ean_total}**")
        st.write(f"Válidos: **{ean_validos}**")

    with cC:
        st.markdown('<div class="section-header">Fila</div>', unsafe_allow_html=True)
        st.caption(f"EAN {st.session_state['ean_pos'] + 1} de {len(eans_unicos)}")

nav_cols = st.columns([1, 1, 2])
with nav_cols[2]:
    nav1, nav2, nav3 = st.columns([1, 1, 1.2])

    with nav1:
        if st.button("← EAN anterior", use_container_width=True, disabled=st.session_state["ean_pos"] <= 0):
            st.session_state["ean_pos"] -= 1
            st.session_state["pagina_anuncios"] = 0
            st.rerun()

    with nav2:
        if st.button("Próximo EAN →", use_container_width=True, disabled=st.session_state["ean_pos"] >= len(eans_unicos) - 1):
            st.session_state["ean_pos"] += 1
            st.session_state["pagina_anuncios"] = 0
            st.rerun()

    with nav3:
        if st.button("Pular este EAN", use_container_width=True, disabled=st.session_state["ean_pos"] >= len(eans_unicos) - 1):
            st.session_state["ean_pos"] += 1
            st.session_state["pagina_anuncios"] = 0
            st.rerun()

st.divider()

# ─────────────────────────────────────────────────────────────────────────────
# FORMULÁRIO DE ANÚNCIOS
# ─────────────────────────────────────────────────────────────────────────────
st.markdown("### 📋 Anúncios deste EAN")
st.caption("Marque as opções abaixo e clique em **Aplicar alterações deste EAN**.")

total_paginas = max(1, (len(df_ean) + ITENS_POR_PAGINA - 1) // ITENS_POR_PAGINA)
st.session_state["pagina_anuncios"] = min(
    max(st.session_state["pagina_anuncios"], 0),
    total_paginas - 1,
)

inicio = st.session_state["pagina_anuncios"] * ITENS_POR_PAGINA
fim = inicio + ITENS_POR_PAGINA
df_ean_page = df_ean.iloc[inicio:fim].copy()

pag_cols = st.columns([1, 1, 2])
with pag_cols[2]:
    p1, p2, p3 = st.columns([1, 1, 1.2])

    with p1:
        if st.button("← Página anterior", use_container_width=True, disabled=st.session_state["pagina_anuncios"] <= 0):
            st.session_state["pagina_anuncios"] -= 1
            st.rerun()

    with p2:
        if st.button("Próxima página →", use_container_width=True, disabled=st.session_state["pagina_anuncios"] >= total_paginas - 1):
            st.session_state["pagina_anuncios"] += 1
            st.rerun()

    with p3:
        st.caption(f"Página {st.session_state['pagina_anuncios'] + 1} de {total_paginas}")

# sincroniza os widgets com os valores atuais da página antes de desenhar
for idx, row in df_ean_page.iterrows():
    idx_int = int(idx)
    rev_key = f"rev_{idx_int}"
    val_key = f"val_{idx_int}"

    val_df_rev = row.get("Revisado", "Não")
    val_df_val = row.get("Valido", "Não")

    if rev_key not in st.session_state:
        st.session_state[rev_key] = val_df_rev
    if val_key not in st.session_state:
        st.session_state[val_key] = val_df_val

estado_antigo = {
    int(i): {
        "revisado": df_ean_page.loc[i, "Revisado"],
        "valido": df_ean_page.loc[i, "Valido"],
    }
    for i in df_ean_page.index
}

with st.form(key=f"form_ean_{ean_atual}_pag_{st.session_state['pagina_anuncios']}"):
    for idx, row in df_ean_page.iterrows():
        idx_int = int(idx)

        link = str(row.get("Link_Real", "")).strip()
        if link.lower() in ("", "nan", "none"):
            link = ""

        origem = str(row.get("Origem", "")).lower()
        vendedor = limpar_vendedor(row.get("Vendedor", ""))
        preco_fmt = formata_preco(row.get("Preco", None))
        nome_anuncio = str(row.get("Nome_do_Anuncio", "—"))

        thumb_row = str(row.get("Thumbnail_Vendedor", "")).strip()
        if thumb_row.lower() in ("", "nan", "none"):
            thumb_row = ""

        origem_label = "🛒 Google Shopping" if "shopping" in origem else "🔵 Google Search"
        mdm_nome_linha = str(row.get("MDM_Nome", "—")).strip() or "—"

        with st.container(border=True):
            c1, c2, c2b, c3, c4, c5 = st.columns([2.6, 1.2, 1.2, 1.0, 1.2, 1.2])

            with c1:
                st.markdown('<div class="section-header">Anúncio</div>', unsafe_allow_html=True)
                st.write(f"**{nome_anuncio}**")
                st.caption(f"Produto MYSA: {mdm_nome_linha}")
                st.caption(f"Cód. Fab: {fab_code}")
                st.caption(f"Origem: {origem_label}")
                st.caption(f"Vendedor: {vendedor or '—'}")
                st.write(f"Preço: **{preco_fmt}**")

            with c2:
                st.markdown('<div class="section-header">Imagem (vendedor)</div>', unsafe_allow_html=True)
                if thumb_row and thumb_row.startswith("http"):
                    st.markdown(
                        f'''
                        <img src="{thumb_row}"
                            loading="lazy"
                            style="max-width:100%; border-radius:8px;">
                        ''',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("—")

            with c2b:
                st.markdown('<div class="section-header">Imagem</div>', unsafe_allow_html=True)
                if img_mdm and str(img_mdm).startswith("http"):
                    st.markdown(
                        f'''
                        <img src="{img_mdm}"
                            loading="lazy"
                            style="max-width:100%; border-radius:8px;">
                        ''',
                        unsafe_allow_html=True,
                    )
                else:
                    st.caption("—")

            with c3:
                st.markdown('<div class="section-header">Link</div>', unsafe_allow_html=True)
                if link and link.startswith("http"):
                    st.link_button("↗ Ver anúncio", link)
                else:
                    st.caption("Indisponível")

            with c4:
                st.markdown('<div class="section-header">Revisado</div>', unsafe_allow_html=True)
                st.radio(
                    f"Revisado_{idx_int}",
                    ["Sim", "Não"],
                    key=f"rev_{idx_int}",
                    horizontal=True,
                    label_visibility="collapsed",
                )

            with c5:
                st.markdown('<div class="section-header">Válido</div>', unsafe_allow_html=True)
                st.radio(
                    f"Valido_{idx_int}",
                    ["Sim", "Não"],
                    key=f"val_{idx_int}",
                    horizontal=True,
                    label_visibility="collapsed",
                )

    aplicar = st.form_submit_button("✅ Aplicar alterações deste EAN")

if aplicar:
    updates = {}
    validou_agora = []

    for idx in df_ean_page.index:
        idx_int = int(idx)
        novo_rev = st.session_state.get(f"rev_{idx_int}", "Não")
        novo_val = st.session_state.get(f"val_{idx_int}", "Não")

        if novo_val == "Sim":
            novo_rev = "Sim"

        old = estado_antigo.get(idx_int, {"revisado": "Não", "valido": "Não"})
        if old["valido"] != "Sim" and novo_val == "Sim":
            validou_agora.append(idx_int)

        updates[idx_int] = {
            "Revisado": novo_rev,
            "Valido": novo_val,
        }

    for idx_int in validou_agora:
        vendedor_v = limpar_vendedor(df_ean.loc[idx_int].get("Vendedor", ""))
        if not vendedor_v:
            continue

        for j in df_ean[df_ean["VEND_LIMPO"] == vendedor_v].index:
            j_int = int(j)
            if j_int == idx_int:
                continue

            revisado_atual = updates.get(j_int, {}).get(
                "Revisado",
                df_ean.loc[j_int, "Revisado"]
            )
            if revisado_atual == "Sim":
                continue

            updates[j_int] = {
                "Revisado": "Sim",
                "Valido": "Não",
            }

    for idx_int, alt in updates.items():
        st.session_state["alteracoes"][idx_int] = alt

        if idx_int in st.session_state["df_work"].index:
            st.session_state["df_work"].loc[idx_int, "Revisado"] = alt["Revisado"]
            st.session_state["df_work"].loc[idx_int, "Valido"] = alt["Valido"]

    try:
        qtd = salvar_alteracoes_no_sheet(updates, st.session_state["df_work"])
        st.success(f"✅ {qtd} alteração(ões) salva(s) no Google Sheets.")
    except Exception as e:
        st.error(f"❌ Erro ao salvar no Google Sheets: {e}")

    for idx in df_ean_page.index:
        idx_int = int(idx)
        st.session_state.pop(f"rev_{idx_int}", None)
        st.session_state.pop(f"val_{idx_int}", None)

    st.rerun()

# ─────────────────────────────────────────────────────────────────────────────
# EXPORTAR SNAPSHOT LOCAL
# ─────────────────────────────────────────────────────────────────────────────
st.divider()
cS, cP = st.columns([1.2, 3])

with cS:
    if st.button("💾 Exportar snapshot local", type="primary", use_container_width=True):
        try:
            caminho_local = os.path.join(os.getcwd(), ARQUIVO_SAIDA)
            df_salvar = st.session_state["df_work"][st.session_state["base_columns"]].copy()
            df_salvar.to_csv(caminho_local, sep=";", index=False, encoding="utf-8-sig")
            st.success(f"✅ Snapshot exportado em `{caminho_local}`")
        except PermissionError:
            st.error(f"❌ Não foi possível salvar. Feche o arquivo `{ARQUIVO_SAIDA}` se estiver aberto.")

with cP:
    total = len(df_work)
    rev = int((df_work["Revisado"] == "Sim").sum())
    val = int((df_work["Valido"] == "Sim").sum())
    pct = rev / total if total else 0

    st.markdown(f"**Revisados:** {rev}/{total} ({pct:.0%}) · **Válidos:** {val}/{total}")
    st.progress(pct)
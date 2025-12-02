# main.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import time
from typing import Optional

# ------------------------------------------------------
# CONFIG FASTAPI
# ------------------------------------------------------

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=[
        "https://andreymarks.github.io",
        "https://*.github.io"
    ],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# ------------------------------------------------------
# CONFIG GOOGLE SERVICE ACCOUNT
# ------------------------------------------------------

SERVICE_ACCOUNT_FILE = "/etc/secrets/credentials.json"
SHEET_KEY = "1A0NqMpUo2jsXRQ2_GIslZ6j2OysYrj_BLfDD9vKe2OE"

# ------------------------------------------------------
# CACHE
# ------------------------------------------------------

_cache = {
    "DEP": {"df": None, "last": 0},
    "TMA": {"df": None, "last": 0},
}
CACHE_TIME_SECONDS = 300  # 5 min


# ------------------------------------------------------
# FUNÇÕES GOOGLE
# ------------------------------------------------------

def _get_gspread_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    return gspread.authorize(creds)


def baixar_planilha(sheet_name: str) -> pd.DataFrame:
    print(f"🔄 Baixando sheet '{sheet_name}'...")
    gc = _get_gspread_client()
    sh = gc.open_by_key(SHEET_KEY)
    ws = sh.worksheet(sheet_name)
    registros = ws.get_all_records()
    df = pd.DataFrame(registros)
    df.columns = df.columns.str.upper().str.strip()
    return df


def carregar_sheet(sheet_name: str) -> pd.DataFrame:
    global _cache
    agora = time.time()

    # primeira carga
    if _cache[sheet_name]["df"] is None:
        df = baixar_planilha(sheet_name)

        if sheet_name == "DEP" and "FLIGTH ATD" in df.columns:
            df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")

        _cache[sheet_name]["df"] = df
        _cache[sheet_name]["last"] = agora
        return df

    # recarregar se expirou
    if agora - _cache[sheet_name]["last"] > CACHE_TIME_SECONDS:
        try:
            df = baixar_planilha(sheet_name)
            if sheet_name == "DEP" and "FLIGTH ATD" in df.columns:
                df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")

            _cache[sheet_name]["df"] = df
            _cache[sheet_name]["last"] = agora
            return df
        except Exception as e:
            print(f"❌ Erro ao atualizar sheet {sheet_name}, usando cache antigo:", e)

    return _cache[sheet_name]["df"]


# ------------------------------------------------------
# DEP — FEEDBACK OPERACIONAL
# ------------------------------------------------------

def gerar_feedback_operacional(df: pd.DataFrame, dep="DEP", data_extracao=None):
    """
    Gera feedback operacional detalhado, agrupando por voo, destino e turno,
    incluindo top motivos e observações.
    """
    if data_extracao is None:
        data_extracao = datetime.now() - timedelta(days=3)
    data_extracao = pd.to_datetime(data_extracao).strftime("%d/%m/%Y")

    col_obs = (
        "OBSERVAÇÕES\n"
        "(Descrever desvios, ex: número de chamado, ocorrências e etc...)"
    )

    feedback = f"📌 *Feedback Operacional {{*{dep}*}} – {data_extracao}*\n\n"

    if "DETALHE DESVIO" not in df.columns:
        return feedback + "(sem coluna DETALHE DESVIO)\n"

    df["DETALHE DESVIO"] = df["DETALHE DESVIO"].astype(str)

    top4_desvios = df["DETALHE DESVIO"].str.upper().value_counts().head(4)
    lista_top = ", ".join([f"\"{d}\"" for d in top4_desvios.index])
    total_guias = len(df)

    total_erro_manifesto = df["DETALHE DESVIO"].str.contains("ERRO DE MANIFESTO", case=False, na=False).sum()
    total_sem_manifesto = df["DETALHE DESVIO"].str.contains("VOADO SEM MAN", case=False, na=False).sum()
    total_scorecard = df["DETALHE DESVIO"].str.contains("ERRO SCORECARD", case=False, na=False).sum()
    total_perca_dep = df["DETALHE DESVIO"].str.contains("PERCA", case=False, na=False).sum()

    feedback += (
        f"📉 No total do dia, registramos **{total_guias} guias** com inconsistências: "
        f"{lista_top}.\n\n"
    )

    if "VOO" in df.columns:
        top_voos = df["VOO"].value_counts().head(2)
        feedback += "✈️ *Voos mais impactados do dia:*\n"
        for voo, qtd in top_voos.items():
            feedback += f"- {voo}: **{qtd} guias**\n"
        feedback += "\n"

    feedback += (
        "👉 *Resumo geral de inconsistências:*\n"
        f"- ❗ **Erro de manifesto:** {total_erro_manifesto} guias\n"
        f"- 📄 **Guias sem manifesto:** {total_sem_manifesto} guias\n"
        f"- 📝 **Erro de Scorecard:** {total_scorecard} guias\n"
        f"- ⛔ **Perdas de DEP:** {total_perca_dep} guias\n\n"
    )

    # ---------------------- Detalhamento por turno ----------------------
    if "TURNO" in df.columns:
        ordem_turnos = ["MANHÃ", "TARDE", "MADRUGADA"]

        def obs_agrupadas(df_grupo):
            if col_obs not in df_grupo.columns:
                return " - "
            obs = df_grupo[col_obs].astype(str).str.strip()
            obs = obs[~obs.str.lower().isin(["nan", "", "none"])]
            return " - " if obs.empty else " --> " + " | ".join(obs.unique())

        for turno in ordem_turnos:
            bloco = df[df["TURNO"] == turno]
            if bloco.empty:
                continue

            icone = "🌅" if turno == "MANHÃ" else "🌤️" if turno == "TARDE" else "🌙"
            feedback += f"{icone} *Turno {turno.title()}*\n"
            feedback += f"📦 Total: **{len(bloco)} guias**\n\n"

            # Maiores destinos
            if "DESTINO" in bloco.columns:
                destinos = bloco["DESTINO"].value_counts().head(3)
                feedback += "📍 *Maiores destinos:*\n"
                for i, (dest, qnt) in enumerate(destinos.items(), 1):
                    feedback += f"{i}️⃣ {dest} → **{qnt} guias**\n"
                feedback += "\n"

            # Detalhamento por tipo de desvio
            for tipo, emoji, titulo in [
                ("ERRO DE MANIFESTO", "⚠️", "Erro de manifesto"),
                ("VOADO SEM MAN", "📄", "Guias sem manifesto"),
                ("ERRO SCORECARD", "📉", "Erro de Scorecard"),
                ("PERCA", "⛔", "Perda de DEP"),
            ]:
                grupo = bloco[bloco["DETALHE DESVIO"].str.contains(tipo, case=False, na=False)]
                if not grupo.empty:
                    feedback += f"{emoji} *{titulo} ({len(grupo)} guia(s))*\n"
                    if "VOO" in grupo.columns and "DESTINO" in grupo.columns:
                        for (voo, dest), g in grupo.groupby(["VOO", "DESTINO"]):
                            feedback += f"{voo} → {dest} → **{len(g)} guias** {obs_agrupadas(g)}\n"
                    feedback += "\n"

    return feedback



@app.get("/feedback")
def rota_dep(data: Optional[str] = None):
    df = carregar_sheet("DEP")

    if data:
        try:
            if "-" in data:
                data_f = datetime.strptime(data, "%Y-%m-%d").date()
            else:
                data_f = datetime.strptime(data, "%d/%m/%Y").date()
        except:
            raise HTTPException(400, "Formato inválido")
    else:
        data_f = (datetime.now() - timedelta(days=3)).date()

    if "FLIGTH ATD" not in df.columns:
        raise HTTPException(500, "Coluna FLIGTH ATD não encontrada")

    filtrado = df[df["FLIGTH ATD"].dt.date == data_f]

    texto = gerar_feedback_operacional(filtrado, "DEP", data_f)

    return {"feedback": texto}


# ------------------------------------------------------
# TMA — ANÁLISE
# ------------------------------------------------------

DESCONSIDERAR = [
    "SENHA OK",
    "DUPLICIDADE",
    "SENHA DOCA COMPUTADA COMO LOJA",
    "BASE NÃO CORRESPONDE",
    "QUEDA SISTEMA"
]


def _normalizar_data_str_para_ddmmaaaa(valor: str) -> Optional[str]:
    """
    Converte qualquer coisa (incluindo '10/11/2025 14:00:18')
    para dd/mm/YYYY.
    """
    if not isinstance(valor, str):
        return None

    valor = valor.strip()
    if valor == "":
        return None

    try:
        dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        # retorna **somente a data**
        return dt.strftime("%d/%m/%Y")
    except:
        return None


def analisar_tma_por_data(df: pd.DataFrame, data_str: str):
    df = df.copy()
    df.columns = df.columns.str.upper().str.strip()

    if "INICIO" not in df.columns:
        raise HTTPException(500, "Coluna INICIO não encontrada na sheet TMA")

    df["INICIO"] = df["INICIO"].astype(str)
    df["DATA_LIMPA"] = df["INICIO"].apply(_normalizar_data_str_para_ddmmaaaa)

    base = df[df["DATA_LIMPA"] == data_str].copy()

    if base.empty:
        return {"texto": f"Nenhuma senha encontrada para {data_str}."}

    # =======================
    #  DETECTAR COLUNA MOTIVO
    # =======================
    motivo_col = None
    for col in base.columns:
        if col.replace(" ", "").replace("_", "") == "DETALHEDESVIO":
            motivo_col = col
            break

    if motivo_col is None:
        raise HTTPException(500, "Coluna DETALHE DESVIO não encontrada no sheet TMA")

    base["MOTIVO"] = base[motivo_col].astype(str).str.upper().str.strip()
    base["CLASSIFICACAO"] = base["MOTIVO"].apply(
        lambda x: "DESCONSIDERADA" if x in DESCONSIDERAR else "VALIDA"
    )

    total = len(base)
    turnos = base["TURNO"].value_counts().to_dict() if "TURNO" in base.columns else {}

    top3 = (
        base[base["CLASSIFICACAO"] == "VALIDA"]["MOTIVO"]
        .value_counts()
        .head(3)
        .reset_index()
        .values
        .tolist()
    )

    texto = f"📊 *ANÁLISE DO TMA – {data_str}*\n\n"
    texto += f"Total analisado: *{total}*\n\n"

    texto += "🔻 Senhas perdidas por turno:\n"
    for t, q in turnos.items():
        texto += f"- {t}: *{q}*\n"
    texto += "\n"

    texto += "🥉 Top 3 motivos válidos:\n"
    for i, (motivo, qtd) in enumerate(top3, 1):
        texto += f"{i}️⃣ {motivo.title()} — {qtd}\n"

    return {"texto": texto}



@app.get("/tma")
def rota_tma(data: Optional[str] = None):
    df = carregar_sheet("TMA")

    if data:
        try:
            if "-" in data:
                dt = datetime.strptime(data, "%Y-%m-%d").date()
            else:
                dt = datetime.strptime(data, "%d/%m/%Y").date()
        except:
            raise HTTPException(400, "Formato inválido")
    else:
        dt = datetime.now().date()

    data_str = dt.strftime("%d/%m/%Y")

    return analisar_tma_por_data(df, data_str)


# ------------------------------------------------------
# ROTA TESTE
# ------------------------------------------------------
@app.get("/")
def root():
    return {"mensagem": "API operando com sucesso!"}

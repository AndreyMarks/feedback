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
# CACHE CENTRALIZADO
# ------------------------------------------------------
_cache = {
    "DEP": {"df": None, "last": 0, "resultados": {}},
    "TMA": {"df": None, "last": 0, "resultados": {}},
    "RCF": {"df": None, "last": 0, "resultados": {}},
}
CACHE_TIME_SECONDS = 300  # 5 minutos

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

    # primeira carga ou cache expirado
    if _cache[sheet_name]["df"] is None or (agora - _cache[sheet_name]["last"] > CACHE_TIME_SECONDS):
        try:
            df = baixar_planilha(sheet_name)
            if sheet_name == "DEP" and "FLIGTH ATD" in df.columns:
                df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")

            _cache[sheet_name]["df"] = df
            _cache[sheet_name]["last"] = agora
            _cache[sheet_name]["resultados"] = {}
            return df
        except Exception as e:
            print(f"❌ Erro ao atualizar sheet {sheet_name}, usando cache antigo:", e)

    return _cache[sheet_name]["df"]

# ------------------------------------------------------
# DEP — FEEDBACK OPERACIONAL
# ------------------------------------------------------
from dep_functions import gerar_feedback_operacional

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
from tma_functions import analisar_tma_por_data

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
# RCF — ANÁLISE
# ------------------------------------------------------
@app.get("/rcf")
def rota_rcf(data: Optional[str] = None):
    df = carregar_sheet("RCF")

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
    return {"feedback": analisar_rcf_por_data(df, data_str)}

def analisar_rcf_por_data(df: pd.DataFrame, data_str: str):
    global _cache

    # Usar cache de resultados se existir
    if data_str in _cache["RCF"]["resultados"]:
        return _cache["RCF"]["resultados"][data_str]

    # Detectar colunas automaticamente
    col_data = [c for c in df.columns if "DATA" in c and "RESOL" not in c][0]
    col_turno = "TURNO"
    col_voo = "VOO"
    col_obs = [c for c in df.columns if "OBSERVA" in c][0]
    col_motivo = "MOTIVO"

    df[col_data] = pd.to_datetime(df[col_data], errors="coerce").dt.date
    data_dt = datetime.strptime(data_str, "%d/%m/%Y").date()
    df_dia = df[df[col_data] == data_dt].reset_index(drop=True)

    total_guias = len(df_dia)
    df_dia["DESVIO"] = df_dia[col_obs].astype(str).str.len() > 3
    df_desvios = df_dia[df_dia["DESVIO"]]

    total_desvios = len(df_desvios)
    guias_pendentes = total_guias - total_desvios

    turno_manha = len(df_desvios[df_desvios[col_turno].str.contains("MANHÃ", case=False, na=False)])
    turno_tarde = len(df_desvios[df_desvios[col_turno].str.contains("TARDE", case=False, na=False)])
    turno_madrugada = len(df_desvios[df_desvios[col_turno].str.contains("NOITE|MADRUG", case=False, na=False)])

    if len(df_desvios) > 0:
        voo_destaque = df_desvios[col_voo].value_counts().idxmax()
        descricao_voo = df_desvios[df_desvios[col_voo] == voo_destaque][col_obs].iloc[0]
        causas = df_desvios[col_motivo].value_counts(normalize=True) * 100
        top3 = causas.head(3).round(1)
    else:
        voo_destaque = "Nenhum"
        descricao_voo = "—"
        top3 = pd.Series([0, 0, 0], index=["Sem dados", "Sem dados", "Sem dados"])

    feedback = f"""
📊 RCF — Análise de {data_dt.strftime('%d/%m/%Y')}

📦 Total de guias: {total_guias}
✅ Guias com desvio: {total_desvios}
⏳ Guias pendentes: {guias_pendentes}

🚨 Desvios por turno
🟡 Manhã: {turno_manha}
🟠 Tarde: {turno_tarde}
🔵 Madrugada: {turno_madrugada}

✈️ Voo em destaque
{voo_destaque} — {descricao_voo}

📌 Top 3 motivos
1️⃣ {top3.index[0]} — {top3.iloc[0]}%
2️⃣ {top3.index[1]} — {top3.iloc[1]}%
3️⃣ {top3.index[2]} — {top3.iloc[2]}%
"""

    # Salvar no cache
    _cache["RCF"]["resultados"][data_str] = feedback
    return feedback

# ------------------------------------------------------
# ROTA TESTE
# ------------------------------------------------------
@app.get("/")
def root():
    return {"mensagem": "API operando com sucesso!"}

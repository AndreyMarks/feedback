# main.py
from fastapi import FastAPI, HTTPException, Query
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials
import time
from typing import Optional

# Importando as funções separadas
from dep_functions import gerar_feedback_operacional
from tma_functions import analisar_tma_por_data

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

    # primeira carga ou cache expirado
    if _cache[sheet_name]["df"] is None or (agora - _cache[sheet_name]["last"] > CACHE_TIME_SECONDS):
        try:
            df = baixar_planilha(sheet_name)
            if sheet_name == "DEP" and "FLIGTH ATD" in df.columns:
                df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")

            _cache[sheet_name]["df"] = df
            _cache[sheet_name]["last"] = agora

            # 🔹 resetar cache de resultados processados se TMA
            if sheet_name == "TMA":
                _cache["TMA"]["resultados"] = {}

            return df
        except Exception as e:
            print(f"❌ Erro ao atualizar sheet {sheet_name}, usando cache antigo:", e)

    return _cache[sheet_name]["df"]



# ------------------------------------------------------
# DEP — FEEDBACK OPERACIONAL (ROTA)
# ------------------------------------------------------

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

    # Chama a função importada
    texto = gerar_feedback_operacional(filtrado, "DEP", data_f)

    return {"feedback": texto}


# ------------------------------------------------------
# TMA — ANÁLISE (ROTA)
# ------------------------------------------------------

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

    # Chama a função importada
    return analisar_tma_por_data(df, data_str)


# ------------------------------------------------------
# ROTA TESTE
# ------------------------------------------------------
@app.get("/")
def root():
    return {"mensagem": "API operando com sucesso!"}

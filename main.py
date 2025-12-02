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

SERVICE_ACCOUNT_FILE = "/etc/secrets/credentials.json"  # ajuste conforme o seu ambiente
SHEET_KEY = "1A0NqMpUo2jsXRQ2_GIslZ6j2OysYrj_BLfDD9vKe2OE"

# ------------------------------------------------------
# CACHE (separado por sheet)
# ------------------------------------------------------

_cache = {
    "DEP": {"df": None, "last": 0},
    "TMA": {"df": None, "last": 0},
}
CACHE_TIME_SECONDS = 300  # 5 minutos


# ------------------------------------------------------
# UTIL: baixar planilha google (genérica)
# ------------------------------------------------------
def _get_gspread_client():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    gc = gspread.authorize(creds)
    return gc


def baixar_planilha(sheet_name: str) -> pd.DataFrame:
    """
    Baixa a worksheet sheet_name do Google Sheets e retorna DataFrame.
    """
    print(f"🔄 Baixando sheet '{sheet_name}' do Google Sheets...")
    gc = _get_gspread_client()
    sh = gc.open_by_key(SHEET_KEY)
    ws = sh.worksheet(sheet_name)
    registros = ws.get_all_records()
    df = pd.DataFrame(registros)
    # Normalizações básicas
    df.columns = df.columns.str.upper().str.strip()
    return df


# ------------------------------------------------------
# FUNÇÃO QUE GERE O CACHE
# ------------------------------------------------------
def carregar_sheet(sheet_name: str) -> pd.DataFrame:
    global _cache
    agora = time.time()

    if _cache[sheet_name]["df"] is None:
        # primeira carga obrigatória
        df = baixar_planilha(sheet_name)
        # conversões específicas
        if "FLIGTH ATD" in df.columns:
            df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")
        _cache[sheet_name]["df"] = df
        _cache[sheet_name]["last"] = agora
        return df

    # se tempo expirou, tenta atualizar, senão devolve cache
    if agora - _cache[sheet_name]["last"] > CACHE_TIME_SECONDS:
        try:
            df = baixar_planilha(sheet_name)
            if "FLIGTH ATD" in df.columns:
                df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")
            _cache[sheet_name]["df"] = df
            _cache[sheet_name]["last"] = agora
            return df
        except Exception as e:
            # log e fallback para cache antigo
            print("❌ Erro ao atualizar cache da sheet", sheet_name, "-> usando cache antigo. Erro:", e)
            return _cache[sheet_name]["df"]

    # retorna cache válido
    return _cache[sheet_name]["df"]


# ------------------------------------------------------
# FUNÇÃO DE FEEDBACK (DEP) — sua função original (sem alterações lógicas)
# ------------------------------------------------------
def gerar_feedback_operacional(df: pd.DataFrame, dep="DEP", data_extracao=None) -> str:
    dois_dias_atras = datetime.now() - timedelta(days=3)

    if data_extracao is None:
        data_extracao = dois_dias_atras.date()

    data_extracao = pd.to_datetime(data_extracao).strftime("%d/%m/%Y")

    col_obs = (
        "OBSERVAÇÕES\n"
        "(Descrever desvios, ex: número de chamado, ocorrências e etc...)"
    )

    feedback = ""
    feedback += f"📌 *Feedback Operacional {{*{dep}*}} – {data_extracao}*\n\n"

    # defensiva caso coluna não exista
    if 'DETALHE DESVIO' not in df.columns:
        return feedback + "\n(sem coluna 'DETALHE DESVIO' na planilha)\n"

    top4_desvios = (
        df['DETALHE DESVIO']
        .astype(str)
        .str.upper()
        .value_counts()
        .head(4)
    )

    lista_top = ", ".join([f"\"{d}\"" for d in top4_desvios.index])
    total_guias = len(df)

    total_erro_manifesto = df['DETALHE DESVIO'].str.contains('ERRO DE MANIFESTO', case=False, na=False).sum()
    total_sem_manifesto = df['DETALHE DESVIO'].str.contains('VOADO SEM MAN', case=False, na=False).sum()
    total_scorecard = df['DETALHE DESVIO'].str.contains('ERRO SCORECARD', case=False, na=False).sum()
    total_perca_dep = df['DETALHE DESVIO'].str.contains('PERCA', case=False, na=False).sum()

    feedback += (
        f"📉 No total do dia, registramos **{total_guias} guias** com inconsistências entre "
        f"{lista_top}.\n\n"
    )

    # Voos mais impactados
    if 'VOO' in df.columns:
        top_voos = df['VOO'].value_counts().head(2)
        feedback += "✈️ *Voos mais impactados do dia:*\n"
        for voo, qnt in top_voos.items():
            feedback += f"- {voo}: **{qnt} guias**\n"
        feedback += "\n"

    # Resumo geral
    feedback += "👉 *Resumo geral de inconsistências:*\n"
    feedback += f"- ❗ **Erro de manifesto:** {total_erro_manifesto} guias\n"
    feedback += f"- 📄 **Guias sem manifesto:** {total_sem_manifesto} guias\n"
    feedback += f"- 📝 **Erro de Scorecard:** {total_scorecard} guias\n"
    feedback += f"- ⛔ **Perdas de DEP:** {total_perca_dep} guias\n\n"

    # Turnos
    ordem_turnos = ["MANHÃ", "TARDE", "MADRUGADA"]
    if 'TURNO' in df.columns:
        turnos_ordenados = [t for t in ordem_turnos if t in df['TURNO'].unique()]
    else:
        turnos_ordenados = []

    def obs_agrupadas(df_grupo):
        if col_obs not in df_grupo.columns:
            return " - "
        obs = df_grupo[col_obs].astype(str).str.strip()
        obs = obs[~obs.str.lower().isin(["nan", "", "none"])]
        if obs.empty:
            return " - "
        return " --> " + " | ".join(obs.unique())

    # Loop turnos
    for turno in turnos_ordenados:
        bloco = df[df['TURNO'] == turno]
        total = len(bloco)

        icone = "🌅" if turno == "MANHÃ" else "🌤️" if turno == "TARDE" else "🌙"

        feedback += f"\n{icone} *Turno {turno.title()}*\n"
        feedback += f"📦 Total: **{total} guias**\n\n"

        # Maiores destinos
        if 'DESTINO' in bloco.columns:
            destinos = bloco['DESTINO'].value_counts().head(3)
            feedback += "📍 *Maiores destinos:*\n"
            for i, (dest, qnt) in enumerate(destinos.items(), 1):
                feedback += f"{i}️⃣ {dest} → **{qnt} guias**\n"
            feedback += "\n"

        # Erros agrupados (somente se coluna existir)
        if 'DETALHE DESVIO' in bloco.columns:
            erro_manifesto = bloco[bloco['DETALHE DESVIO'].str.contains("ERRO DE MANIFESTO", case=False, na=False)]
            if not erro_manifesto.empty:
                feedback += f"⚠️ *Erro de manifesto ({len(erro_manifesto)} guias):*\n"
                grouped = erro_manifesto.groupby(['VOO', 'DESTINO']) if 'VOO' in erro_manifesto.columns and 'DESTINO' in erro_manifesto.columns else []
                if grouped != []:
                    for (voo, dest), grupo_df in grouped:
                        obs_txt = obs_agrupadas(grupo_df)
                        feedback += f"❗ {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
                feedback += "\n"

            sem_man = bloco[bloco['DETALHE DESVIO'].str.contains("VOADO SEM MAN", case=False, na=False)]
            if not sem_man.empty:
                feedback += f"📄 *Guias sem manifesto ({len(sem_man)} guias):*\n"
                grouped = sem_man.groupby(['VOO', 'DESTINO']) if 'VOO' in sem_man.columns and 'DESTINO' in sem_man.columns else []
                if grouped != []:
                    for (voo, dest), grupo_df in grouped:
                        obs_txt = obs_agrupadas(grupo_df)
                        feedback += f"✈️ {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
                feedback += "\n"

            score = bloco[bloco['DETALHE DESVIO'].str.contains("ERRO SCORECARD", case=False, na=False)]
            if not score.empty:
                feedback += f"📉 *Erro de Scorecard ({len(score)} guias)* → Seguiram conforme\n"
                grouped = score.groupby(['VOO', 'DESTINO']) if 'VOO' in score.columns and 'DESTINO' in score.columns else []
                if grouped != []:
                    for (voo, dest), grupo_df in grouped:
                        obs_txt = obs_agrupadas(grupo_df)
                        feedback += f"📝 {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
                feedback += "\n"

            perda = bloco[bloco['DETALHE DESVIO'].str.contains("PERCA", case=False, na=False)]
            if not perda.empty:
                feedback += f"⛔ *Perda de DEP ({len(perda)} guia(s))*\n"
                grouped = perda.groupby(['VOO', 'DESTINO']) if 'VOO' in perda.columns and 'DESTINO' in perda.columns else []
                if grouped != []:
                    for (voo, dest), grupo_df in grouped:
                        obs_txt = obs_agrupadas(grupo_df)
                        feedback += f"{voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
                feedback += "\n"

    return feedback


# ------------------------------------------------------
# ROUTE: DEP feedback
# ------------------------------------------------------
@app.get("/feedback")
def rota_feedback(data: Optional[str] = Query(None, description="YYYY-MM-DD or DD/MM/YYYY")):
    try:
        df = carregar_sheet("DEP")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar DEP: {e}")

    # interpretar data
    if data:
        try:
            # suporta YYYY-MM-DD ou dd/mm/YYYY
            if "-" in data:
                data_filtro = datetime.strptime(data, "%Y-%m-%d").date()
            else:
                data_filtro = datetime.strptime(data, "%d/%m/%Y").date()
        except Exception:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD ou DD/MM/YYYY.")
    else:
        data_filtro = (datetime.now() - timedelta(days=3)).date()

    # filtra
    if "FLIGTH ATD" not in df.columns:
        raise HTTPException(status_code=500, detail="Coluna 'FLIGTH ATD' não encontrada na sheet DEP.")

    df_filtrado = df[df['FLIGTH ATD'].dt.date == data_filtro]

    texto = gerar_feedback_operacional(df_filtrado, dep="DEP", data_extracao=data_filtro)
    return {"feedback": texto}


# ------------------------------------------------------
# ROTAS: TMA (nova rota separada)
# ------------------------------------------------------

def _normalizar_data_str_para_ddmmaaaa(valor: str) -> Optional[str]:
    """Recebe vários formatos e retorna dd/mm/YYYY ou None."""
    if not isinstance(valor, str):
        return None
    valor = valor.strip()
    if valor == "":
        return None
    # tenta parse genérico com pandas (dayfirst=True para priorizar dd/mm)
    try:
        dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d/%m/%Y")
    except Exception:
        return None


DESCONSIDERAR = [
    "SENHA OK",
    "DUPLICIDADE",
    "SENHA DOCA COMPUTADA COMO LOJA",
    "BASE NÃO CORRESPONDE",
    "QUEDA SISTEMA"
]


def analisar_tma_por_data(df_tma: pd.DataFrame, data_analise_str: str):
    """
    df_tma: dataframe com colunas padronizadas em UPPERCASE
    data_analise_str: em formato DD/MM/YYYY
    Retorna dict com texto e resumo
    """
    # garantir colunas uppercase
    df = df_tma.copy()
    df.columns = df.columns.str.upper().str.strip()

    if "INICIO" not in df.columns:
        raise ValueError("Coluna 'INICIO' não encontrada na sheet TMA.")

    # normalizar INICIO para DATA_LIMPA (dd/mm/YYYY)
    df["INICIO"] = df["INICIO"].astype(str).fillna("").str.strip()
    df["DATA_LIMPA"] = df["INICIO"].apply(_normalizar_data_str_para_ddmmaaaa)

    base = df[df["DATA_LIMPA"] == data_analise_str].copy()

    if base.empty:
        return {
            "texto": f"Nenhuma senha encontrada para {data_analise_str} na coluna INICIO.",
            "total_senhas": 0,
            "turno_contagem": {},
            "top_motivos": [],
            "resumo_turnos": [],
        }

    motivo_col = "DETALHE DESVIO" if "DETALHE DESVIO" in base.columns else ("DETALHE_DESVIO" if "DETALHE_DESVIO" in base.columns else None)
    if motivo_col is None:
        base["MOTIVO"] = ""
    else:
        base["MOTIVO"] = base[motivo_col].astype(str).str.upper().str.strip()

    base["CLASSIFICACAO"] = base["MOTIVO"].apply(lambda x: "DESCONSIDERADA" if x in DESCONSIDERAR else "VALIDA")

    total_senhas = len(base)
    turno_contagem = base['TURNO'].value_counts().to_dict() if 'TURNO' in base.columns else {}

    top_motivos = (
        base[base["CLASSIFICACAO"] == "VALIDA"]["MOTIVO"]
        .value_counts()
        .head(3)
        .reset_index()
        .values
        .tolist()
    )

    def resumo_turno(turno):
        bloco = base[base["TURNO"].str.upper() == turno.upper()] if 'TURNO' in base.columns else base
        if bloco.empty:
            return None
        perdidas = len(bloco)
        desco = bloco[bloco["CLASSIFICACAO"] == "DESCONSIDERADA"]
        validas = bloco[bloco["CLASSIFICACAO"] == "VALIDA"]
        return {
            "turno": turno.upper(),
            "perdidas": int(perdidas),
            "desconsideradas": desco["MOTIVO"].value_counts().reset_index().values.tolist() if not desco.empty else [],
            "validas": validas["MOTIVO"].value_counts().reset_index().values.tolist() if not validas.empty else [],
        }

    turnos = ["MANHÃ", "TARDE", "MADRUGADA"]
    resumo_turnos = [resumo_turno(t) for t in turnos if resumo_turno(t) is not None]

    # montar texto (seguindo seu formato)
    texto = f"📊 *ANÁLISE DO TMA – {data_analise_str}*\n\n"
    texto += f"_Total de senhas analisadas_: *{total_senhas}*\n\n"

    texto += "🔻 _Senhas perdidas por turno_ :\n"
    for t, q in turno_contagem.items():
        texto += f"* {t.title()} : *{q}*\n"
    texto += "\n"

    texto += "🥉 _Top 3 motivos detratores (válidos)_ :\n"
    for i, (motivo, qtd) in enumerate(top_motivos, start=1):
        texto += f"{i}️⃣ *{motivo.title()}* : {qtd}\n"
    texto += "\n" + ("-"*40) + "\n\n"

    for bloco in resumo_turnos:
        texto += f"🔵 _TURNO {bloco['turno']}_\n"
        texto += f"_Senhas perdidas_ : *{bloco['perdidas']}*\n\n"

        texto += "_Desconsideradas_ :\n"
        if bloco["desconsideradas"]:
            for motivo, qtd in bloco["desconsideradas"]:
                texto += f"- {qtd} {motivo.title()}\n"
        else:
            texto += "*Nenhuma*\n"
        texto += "\n"

        texto += "_Perdidas válidas_ :\n"
        if bloco["validas"]:
            for motivo, qtd in bloco["validas"]:
                texto += f"* {qtd} {motivo.title()}\n"
        else:
            texto += "*Nenhuma*\n"

        texto += "\n" + ("-"*40) + "\n\n"

    return {
        "texto": texto,
        "total_senhas": int(total_senhas),
        "turno_contagem": {k: int(v) for k, v in turno_contagem.items()},
        "top_motivos": [[str(x[0]), int(x[1])] for x in top_motivos],
        "resumo_turnos": resumo_turnos,
    }


@app.get("/tma")
def rota_tma(data: Optional[str] = Query(None, description="Data no formato YYYY-MM-DD ou DD/MM/YYYY")):
    """
    Retorna análise TMA para a data informada (ou hoje, se não informado).
    Ex.: /tma?data=2025-11-27 ou /tma?data=27/11/2025
    """
    try:
        df_tma = carregar_sheet("TMA")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao carregar sheet TMA: {e}")

    # interpretar data
    if data:
        # aceitar YYYY-MM-DD ou DD/MM/YYYY
        try:
            if "-" in data:
                dt = datetime.strptime(data, "%Y-%m-%d").date()
                data_analise_str = dt.strftime("%d/%m/%Y")
            else:
                dt = datetime.strptime(data, "%d/%m/%Y").date()
                data_analise_str = dt.strftime("%d/%m/%Y")
        except Exception:
            raise HTTPException(status_code=400, detail="Formato de data inválido. Use YYYY-MM-DD ou DD/MM/YYYY.")
    else:
        data_analise_str = datetime.now().strftime("%d/%m/%Y")

    try:
        resultado = analisar_tma_por_data(df_tma, data_analise_str)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro ao analisar TMA: {e}")

    return resultado


# ------------------------------------------------------
# ROTA TESTE
# ------------------------------------------------------
@app.get("/")
def root():
    return {"mensagem": "API DEP operando com sucesso!"}

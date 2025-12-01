from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import pandas as pd
from datetime import datetime, timedelta
import gspread
from google.oauth2.service_account import Credentials

# ----------------------------------------------------------------
# CONFIG FASTAPI
# ----------------------------------------------------------------

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

# ----------------------------------------------------------------
# GOOGLE SHEETS - CARREGAR DEP
# ----------------------------------------------------------------

def carregar_dep():
    scope = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive"
    ]

    import os
    
    SERVICE_ACCOUNT_FILE = "/etc/secrets/credentials.json"  # coloque o nome correto
    
    creds = Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE, scopes=scope)
    gc = gspread.authorize(creds)

    # Abrir pelo ID da planilha
    sh = gc.open_by_key("1A0NqMpUo2jsXRQ2_GIslZ6j2OysYrj_BLfDD9vKe2OE")

    aba = sh.worksheet("DEP")
    registros = aba.get_all_records()

    df = pd.DataFrame(registros)

    # Converter data
    df["FLIGTH ATD"] = pd.to_datetime(df["FLIGTH ATD"], dayfirst=True, errors="coerce")

    return df

# ----------------------------------------------------------------
# FUNÇÃO PRINCIPAL DO FEEDBACK
# ----------------------------------------------------------------

def gerar_feedback_operacional(df, dep="DEP", data_extracao=None):

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

    top4_desvios = (
        df['DETALHE DESVIO']
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
    turnos_ordenados = [t for t in ordem_turnos if t in df['TURNO'].unique()]

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
        destinos = bloco['DESTINO'].value_counts().head(3)
        feedback += "📍 *Maiores destinos:*\n"
        for i, (dest, qnt) in enumerate(destinos.items(), 1):
            feedback += f"{i}️⃣ {dest} → **{qnt} guias**\n"
        feedback += "\n"

        # Erro Manifesto
        erro_manifesto = bloco[bloco['DETALHE DESVIO'].str.contains("ERRO DE MANIFESTO", case=False, na=False)]
        if not erro_manifesto.empty:
            feedback += f"⚠️ *Erro de manifesto ({len(erro_manifesto)} guias):*\n"
            grouped = erro_manifesto.groupby(['VOO', 'DESTINO'])
            for (voo, dest), grupo_df in grouped:
                obs_txt = obs_agrupadas(grupo_df)
                feedback += f"❗ {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
            feedback += "\n"

        # Sem manifesto
        sem_man = bloco[bloco['DETALHE DESVIO'].str.contains("VOADO SEM MAN", case=False, na=False)]
        if not sem_man.empty:
            feedback += f"📄 *Guias sem manifesto ({len(sem_man)} guias):*\n"
            grouped = sem_man.groupby(['VOO', 'DESTINO'])
            for (voo, dest), grupo_df in grouped:
                obs_txt = obs_agrupadas(grupo_df)
                feedback += f"✈️ {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
            feedback += "\n"

        # Scorecard
        score = bloco[bloco['DETALHE DESVIO'].str.contains("ERRO SCORECARD", case=False, na=False)]
        if not score.empty:
            feedback += f"📉 *Erro de Scorecard ({len(score)} guias)* → Seguiram conforme\n"
            grouped = score.groupby(['VOO', 'DESTINO'])
            for (voo, dest), grupo_df in grouped:
                obs_txt = obs_agrupadas(grupo_df)
                feedback += f"📝 {voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
            feedback += "\n"

        # Perda DEP
        perda = bloco[bloco['DETALHE DESVIO'].str.contains("PERCA", case=False, na=False)]
        if not perda.empty:
            feedback += f"⛔ *Perda de DEP ({len(perda)} guia(s))*\n"
            grouped = perda.groupby(['VOO', 'DESTINO'])
            for (voo, dest), grupo_df in grouped:
                obs_txt = obs_agrupadas(grupo_df)
                feedback += f"{voo} → {dest} → **{len(grupo_df)} guias** {obs_txt}\n"
            feedback += "\n"

    return feedback


# ----------------------------------------------------------------
# ROTA FASTAPI — GERA FEEDBACK COMPLETO AUTOMATICAMENTE
# ----------------------------------------------------------------

@app.get("/feedback")
def rota_feedback(data: str = None):

    df = carregar_dep()

    # Se o usuário passou data → usa ela
    if data:
        data_filtro = datetime.strptime(data, "%Y-%m-%d").date()
    else:
        # Caso NÃO passe data → usa D-3 (comportamento antigo)
        data_filtro = (datetime.now() - timedelta(days=3)).date()

    # Filtra pela data escolhida
    df_filtrado = df[df['FLIGTH ATD'].dt.date == data_filtro]

    # Gera feedback com a data correta no cabeçalho
    texto = gerar_feedback_operacional(
        df_filtrado,
        dep="DEP",
        data_extracao=data_filtro
    )

    return {"feedback": texto}




# ROTA TESTE
@app.get("/")
def root():
    return {"mensagem": "API DEP operando com sucesso!"}

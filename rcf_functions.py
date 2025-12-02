# rcf_functions.py
import pandas as pd
import time

# Cache global
_cache_rcf = {
    "df": None,
    "last": 0,
    "resultados": {}
}
CACHE_TIME_SECONDS = 300  # 5 minutos


def carregar_rcf_sheet(sheet_name: str, service_account_file: str, sheet_key: str):
    import gspread
    from google.oauth2.service_account import Credentials

    agora = time.time()

    # Atualizar se cache expirou
    if _cache_rcf["df"] is None or (agora - _cache_rcf["last"] > CACHE_TIME_SECONDS):
        scope = [
            "https://www.googleapis.com/auth/spreadsheets",
            "https://www.googleapis.com/auth/drive"
        ]
        creds = Credentials.from_service_account_file(service_account_file, scopes=scope)
        gc = gspread.authorize(creds)
        ws = gc.open_by_key(sheet_key).worksheet(sheet_name)
        data = ws.get_all_records()
        df = pd.DataFrame(data)
        df.columns = df.columns.str.upper().str.strip()

        _cache_rcf["df"] = df
        _cache_rcf["last"] = agora
        _cache_rcf["resultados"] = {}  # resetar cache de resultados
    return _cache_rcf["df"]


def analisar_rcf_por_data(df: pd.DataFrame, data_filtrar: str):
    """
    Retorna feedback do RCF para uma data específica.
    Cache de resultados por data.
    """
    global _cache_rcf
    if data_filtrar in _cache_rcf["resultados"]:
        return _cache_rcf["resultados"][data_filtrar]

    # Detectar colunas automaticamente
    col_data = [c for c in df.columns if "DATA" in c and "RESOL" not in c][0]
    col_turno = "TURNO"
    col_voo = "VOO"
    col_obs = [c for c in df.columns if "OBSERVA" in c][0]
    col_motivo = "MOTIVO"

    df[col_data] = pd.to_datetime(df[col_data], errors="coerce").dt.date
    data_dt = pd.to_datetime(data_filtrar, dayfirst=True).date()
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
    _cache_rcf["resultados"][data_filtrar] = feedback
    return feedback

import pandas as pd
from datetime import datetime
from typing import Optional
from fastapi import HTTPException

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

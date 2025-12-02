import pandas as pd
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
    Converte datas em qualquer formato reconhecível pelo pandas
    para dd/mm/YYYY. Retorna None se não for possível.
    """
    if not isinstance(valor, str):
        return None
    valor = valor.strip()
    if not valor:
        return None
    try:
        dt = pd.to_datetime(valor, dayfirst=True, errors="coerce")
        if pd.isna(dt):
            return None
        return dt.strftime("%d/%m/%Y")
    except:
        return None

def analisar_tma_por_data(df: pd.DataFrame, data_str: str):
    """
    Analisa TMA para uma data específica, retornando:
    - Total de senhas
    - Senhas por turno
    - Top 3 motivos válidos
    - Resumo detalhado por turno
    """
    df = df.copy()
    df.columns = df.columns.str.upper().str.strip()

    if "INICIO" not in df.columns:
        raise HTTPException(500, "Coluna INICIO não encontrada na sheet TMA")

    df["INICIO"] = df["INICIO"].astype(str).str.strip()
    df["DATA_LIMPA"] = df["INICIO"].apply(_normalizar_data_str_para_ddmmaaaa)

    base = df[df["DATA_LIMPA"] == data_str].copy()
    if base.empty:
        return {"texto": f"Nenhuma senha encontrada para {data_str}."}

    # DETECTAR COLUNA MOTIVO
    motivo_col = None
    for col in base.columns:
        if col.replace(" ", "").replace("_", "") == "DETALHEDESVIO":
            motivo_col = col
            break
    if motivo_col is None:
        raise HTTPException(500, "Coluna DETALHE DESVIO não encontrada no sheet TMA")

    # CLASSIFICAÇÃO
    base["MOTIVO"] = base[motivo_col].astype(str).str.upper().str.strip()
    base["CLASSIFICACAO"] = base["MOTIVO"].apply(
        lambda x: "DESCONSIDERADA" if x in DESCONSIDERAR else "VALIDA"
    )

    total_senhas = len(base)
    turno_contagem = base["TURNO"].value_counts().to_dict() if "TURNO" in base.columns else {}

    top_motivos = (
        base[base["CLASSIFICACAO"] == "VALIDA"]["MOTIVO"]
        .value_counts()
        .head(3)
        .reset_index()
        .values
        .tolist()
    )

    # RESUMO POR TURNO
    def resumo_turno(turno):
        if "TURNO" not in base.columns:
            return None
        bloco = base[base["TURNO"].str.upper() == turno.upper()]
        if bloco.empty:
            return None
        perdidas = len(bloco)
        desco = bloco[bloco["CLASSIFICACAO"] == "DESCONSIDERADA"]
        validas = bloco[bloco["CLASSIFICACAO"] == "VALIDA"]
        return {
            "turno": turno.upper(),
            "perdidas": perdidas,
            "desconsideradas": desco["MOTIVO"].value_counts().reset_index().values.tolist(),
            "validas": validas["MOTIVO"].value_counts().reset_index().values.tolist(),
        }

    turnos = ["MANHÃ", "TARDE", "MADRUGADA"]
    resumo_turnos = [resumo_turno(t) for t in turnos]

    # GERAR TEXTO FINAL
    texto = f"📊 *ANÁLISE DO TMA – {data_str}*\n\n"
    texto += f"_Total de senhas analisadas_: *{total_senhas}*\n\n"

    texto += "🔻 _Senhas perdidas por turno_ :\n"
    for t, q in turno_contagem.items():
        texto += f"* {t.title()} : *{q}*\n"
    texto += "\n"

    texto += "🥉 _Top 3 motivos válidos_ :\n"
    for i, (motivo, qtd) in enumerate(top_motivos, start=1):
        texto += f"{i}️⃣ *{motivo.title()}* : {qtd}\n"
    texto += "\n" + ("-"*80) + "\n\n"

    for bloco in resumo_turnos:
        if not bloco:
            continue
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
        texto += "\n" + ("-"*80) + "\n\n"

    return {"texto": texto}

import pandas as pd
from datetime import datetime, timedelta

def gerar_feedback_operacional(df: pd.DataFrame, dep="DEP", data_extracao=None):
    """
    Feedback operacional compatível Colab/Render/DEP.
    Percentual apenas no topo do desvio por turno.
    Inclui CHAMADOS (PCNLAT) e Top 3 desvios gerais.
    """

    # 🔥 NORMALIZAÇÃO UNIVERSAL
    df = df.replace([None, "None", "nan", "NaN"], "")
    df = df.applymap(lambda x: "-" if str(x).strip() == "" else x)

    if data_extracao is None:
        data_extracao = datetime.now() - timedelta(days=2)
    data_extracao = pd.to_datetime(data_extracao).strftime("%d/%m/%Y")

    feedback = f"📌 Feedback Operacional {{{dep}}} – {data_extracao}\n\n"

    if "DETALHE DESVIO" not in df.columns:
        return feedback + "(sem coluna DETALHE DESVIO)\n"

    df["DETALHE DESVIO"] = df["DETALHE DESVIO"].astype(str)

    # Detectar coluna de observações
    col_obs = None
    for c in df.columns:
        if "OBSERVAÇ" in c.upper() or "OBSERVAC" in c.upper():
            col_obs = c
            break
    if col_obs is None:
        df["OBSERVACOES_TEMP"] = "-"
        col_obs = "OBSERVACOES_TEMP"

    def obs_agrupadas(grupo):
        obs_txt = grupo[col_obs].astype(str).str.strip()
        obs_txt = obs_txt[~obs_txt.isin(["-", "", "nan", "None"])]
        return " - " if obs_txt.empty else " --> " + " | ".join(obs_txt.unique())

    # Totais gerais
    total_guias = len(df)
    feedback += f"📉 Total de guias analisadas: **{total_guias}**\n\n"

    import re

    # ---------------------- CHAMADOS ----------------------
    # Pega todas as células da coluna de observações
    obs_texto = df[col_obs].astype(str).str.strip()
    # Extrai todos os padrões PCNLAT-xxxxxxx
    chamados = obs_texto.apply(lambda x: re.findall(r"PCNLAT-\d+", x.upper()))
    # Desempacota a lista em várias linhas
    chamados = pd.Series([c for sublist in chamados for c in sublist])
    # Remove duplicatas e ordena
    chamados = chamados.drop_duplicates().sort_values()
    # Adiciona ao feedback
    if not chamados.empty:
        feedback += "📞 CHAMADOS:\n"
        for c in chamados:
            feedback += f"- {c}\n"
        feedback += "\n"


    # ---------------------- TOP 3 DESVIOS GERAIS ----------------------
    if "VOO" in df.columns:
        top_desvios = (
            df.groupby(["DETALHE DESVIO", "VOO"])
              .size()
              .reset_index(name="QTD")
              .sort_values("QTD", ascending=False)
              .head(3)
        )

        feedback += "🏆 Top 3 desvios (todos os turnos):\n"
        for _, row in top_desvios.iterrows():
            perc = (row["QTD"] / total_guias * 100) if total_guias else 0
            feedback += f"- {row['DETALHE DESVIO'].upper()} | {row['VOO']} → **{row['QTD']} guias** ({perc:.1f}%)\n"
        feedback += "\n"

    # ---------------------- RESUMO POR TURNO ----------------------
    if "TURNO" in df.columns:
        ordem_turnos = ["MANHÃ", "TARDE", "MADRUGADA"]

        for turno in ordem_turnos:
            bloco = df[df["TURNO"] == turno]
            if bloco.empty:
                continue

            total_turno = len(bloco)
            icone = "🌅" if turno == "MANHÃ" else "🌤️" if turno == "TARDE" else "🌙"
            feedback += f"{icone} Turno {turno.title()} — Total: **{total_turno} guias**\n\n"

            # Maiores destinos
            if "DESTINO" in bloco.columns:
                dests = bloco["DESTINO"].value_counts().head(3)
                feedback += "📍 Maiores destinos:\n"
                for i, (dest, qtd) in enumerate(dests.items(), 1):
                    feedback += f"{i}️⃣ {dest} → **{qtd} guias**\n"
                feedback += "\n"

            # Bloco dos desvios
            desvios = [
                ("ERRO DE MANIFESTO", "⚠️", "Erro de manifesto"),
                ("VOADO SEM MAN", "📄", "Guias sem manifesto"),
                ("ERRO SCORECARD", "📉", "Erro de Scorecard"),
                ("PERCA", "⛔", "Perda de DEP"),
                ("GUIAS COM IRR", "❌", "Guias com IRR"),
                ("ONHAND", "❌", "ONHAND"),
                ("CORTE", "✂️", "CORTE"),
                ("RCF DUPLICADO", "🔁", "RCF duplicado"),
                ("GUIA DUPLICADA", "🔂", "Guia duplicada"),
                ("REGULARIZAÇÃO", "🔧", "Regularização"),
            ]

            for termo, emoji, titulo in desvios:
                grupo = bloco[bloco["DETALHE DESVIO"].str.contains(termo, case=False, na=False)]
                if grupo.empty:
                    continue

                perc_turno = (len(grupo) / total_turno * 100) if total_turno else 0
                feedback += f"{emoji} {titulo} ({len(grupo)} guias, {perc_turno:.1f}% do turno):\n"

                if "VOO" in grupo.columns and "DESTINO" in grupo.columns:
                    for (voo, dest), g in grupo.groupby(["VOO", "DESTINO"]):
                        obs_txt = g[col_obs].astype(str).str.strip()
                        obs_txt = obs_txt[~obs_txt.isin(["-", "", "nan", "None"])]
                        obs_str = " - " if obs_txt.empty else " --> " + " | ".join(obs_txt.unique())
                        feedback += f"✈️ {voo} → {dest} → **{len(g)} guias** {obs_str}\n"

                feedback += "\n"

    return feedback

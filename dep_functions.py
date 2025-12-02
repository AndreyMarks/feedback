import pandas as pd
from datetime import datetime, timedelta

def gerar_feedback_operacional(df: pd.DataFrame, dep="DEP", data_extracao=None):
    """
    Feedback operacional completo, compatível Colab/Render/DEP.
    Inclui porcentagens, turnos, maiores destinos, desvios e observações agrupadas.
    """

    # ======================================================
    # 🔥 NORMALIZAÇÃO UNIVERSAL
    # ======================================================
    df = df.replace([None, "None", "nan", "NaN"], "")
    df = df.applymap(lambda x: "-" if str(x).strip() == "" else x)

    # ---------------------- DATA ----------------------
    if data_extracao is None:
        data_extracao = datetime.now() - timedelta(days=2)
    data_extracao = pd.to_datetime(data_extracao).strftime("%d/%m/%Y")

    feedback = f"📌 Feedback Operacional {{{dep}}} – {data_extracao}\n\n"

    if "DETALHE DESVIO" not in df.columns:
        return feedback + "(sem coluna DETALHE DESVIO)\n"

    df["DETALHE DESVIO"] = df["DETALHE DESVIO"].astype(str)

    # ---------------------- DETECTAR COLUNA DE OBSERVAÇÕES ----------------------
    col_obs = None
    for c in df.columns:
        if "OBSERVAÇ" in c.upper() or "OBSERVAC" in c.upper():
            col_obs = c
            break
    if col_obs is None:
        df["OBSERVACOES_TEMP"] = "-"
        col_obs = "OBSERVACOES_TEMP"

    # ======================================================
    # FUNÇÃO PARA AGRUPAR OBSERVAÇÕES
    # ======================================================
    def obs_agrupadas(df_grupo):
        obs = df_grupo[col_obs].astype(str).str.strip()
        obs = obs[~obs.isin(["-", "", "nan", "None"])]
        if obs.empty:
            return " - "
        return " --> " + " | ".join(obs.unique())

    # ---------------------- TOP DESVIOS ----------------------
    top4_desvios = df["DETALHE DESVIO"].str.upper().value_counts().head(4)
    lista_top = ", ".join([f"\"{d}\"" for d in top4_desvios.index])
    total_guias = len(df)

    # Totais por tipo de desvio
    total_erro_manifesto = df["DETALHE DESVIO"].str.contains("ERRO DE MANIFESTO", case=False, na=False).sum()
    total_sem_manifesto = df["DETALHE DESVIO"].str.contains("VOADO SEM MAN", case=False, na=False).sum()
    total_scorecard = df["DETALHE DESVIO"].str.contains("ERRO SCORECARD", case=False, na=False).sum()
    total_perca_dep = df["DETALHE DESVIO"].str.contains("PERCA", case=False, na=False).sum()

    # Percentuais gerais
    perc_erro_manifesto = (total_erro_manifesto / total_guias * 100) if total_guias else 0
    perc_sem_manifesto = (total_sem_manifesto / total_guias * 100) if total_guias else 0
    perc_scorecard = (total_scorecard / total_guias * 100) if total_guias else 0
    perc_perca_dep = (total_perca_dep / total_guias * 100) if total_guias else 0

    feedback += (
        f"📉 No total do dia, registramos **{total_guias} guias** com inconsistências: "
        f"{lista_top}.\n\n"
    )

    # ---------------------- TOP VOOS ----------------------
    if "VOO" in df.columns:
        top_voos = df["VOO"].value_counts().head(3)
        feedback += "✈️ Voos mais impactados do dia:\n"
        for voo, qtd in top_voos.items():
            perc_voo = (qtd / total_guias * 100) if total_guias else 0
            feedback += f"- {voo}: **{qtd} guias** ({perc_voo:.1f}%)\n"
        feedback += "\n"

    # ---------------------- RESUMO GERAL ----------------------
    feedback += (
        "👉 Resumo geral de inconsistências:\n"
        f"- ❗ **Erro de manifesto:** {total_erro_manifesto} guias ({perc_erro_manifesto:.1f}%)\n"
        f"- 📄 **Guias sem manifesto:** {total_sem_manifesto} guias ({perc_sem_manifesto:.1f}%)\n"
        f"- 📝 **Erro de Scorecard:** {total_scorecard} guias ({perc_scorecard:.1f}%)\n"
        f"- ⛔ **Perdas de DEP:** {total_perca_dep} guias ({perc_perca_dep:.1f}%)\n\n"
    )

    # ---------------------- RESUMO POR TURNO ----------------------
    if "TURNO" in df.columns:
        ordem_turnos = ["MANHÃ", "TARDE", "MADRUGADA"]

        for turno in ordem_turnos:
            bloco = df[df["TURNO"] == turno]
            if bloco.empty:
                continue

            total_turno = len(bloco)
            icone = "🌅" if turno == "MANHÃ" else "🌤️" if turno == "TARDE" else "🌙"
            feedback += f"{icone} Turno {turno.title()}\n"
            feedback += f"📦 Total: **{total_turno} guias**\n\n"

            # Maiores destinos
            if "DESTINO" in bloco.columns:
                dests = bloco["DESTINO"].value_counts().head(3)
                feedback += "📍 Maiores destinos:\n"
                for i, (dest, qtd) in enumerate(dests.items(), 1):
                    perc_dest = (qtd / total_turno * 100) if total_turno else 0
                    feedback += f"{i}️⃣ {dest} → **{qtd} guias** ({perc_dest:.1f}%)\n"
                feedback += "\n"

            # Bloco dos desvios
            desvios = [
                ("ERRO DE MANIFESTO", "⚠️", "Erro de manifesto"),
                ("VOADO SEM MAN", "📄", "Guias sem manifesto"),
                ("ERRO SCORECARD", "📉", "Erro de Scorecard"),
                ("PERCA", "⛔", "Perda de DEP"),
            ]

            for termo, emoji, titulo in desvios:
                grupo = bloco[bloco["DETALHE DESVIO"].str.contains(termo, case=False, na=False)]
                if grupo.empty:
                    continue

                perc_grupo = (len(grupo) / total_turno * 100) if total_turno else 0
                feedback += f"{emoji} {titulo} ({len(grupo)} guias, {perc_grupo:.1f}% do turno):\n"

                if "VOO" in grupo.columns and "DESTINO" in grupo.columns:
                    for (voo, dest), g in grupo.groupby(["VOO", "DESTINO"]):
                        obs_txt = g[col_obs].astype(str).str.strip()
                        obs_txt = obs_txt[~obs_txt.isin(["-", "", "nan", "None"])]
                        obs_str = " - " if obs_txt.empty else " --> " + " | ".join(obs_txt.unique())
                        perc_voo = (len(g) / total_turno * 100) if total_turno else 0
                        feedback += f"✈️ {voo} → {dest} → **{len(g)} guias** ({perc_voo:.1f}% do turno) {obs_str}\n"

                feedback += "\n"

    return feedback

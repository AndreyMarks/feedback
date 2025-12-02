import pandas as pd
from datetime import datetime, timedelta
from typing import Optional

def gerar_feedback_operacional(df: pd.DataFrame, dep="DEP", data_extracao=None):
    """
    Gera feedback operacional com mesma lógica do Colab:
    - Observações aparecem apenas no grupo total do desvio
    - Agrupamento por voo/destino mostra apenas " - "
    """

    # ---------------------- DATA ----------------------
    if data_extracao is None:
        data_extracao = datetime.now() - timedelta(days=3)
    data_extracao = pd.to_datetime(data_extracao).strftime("%d/%m/%Y")

    # Nome exato da coluna de observações
    col_obs = (
        "OBSERVAÇÕES\n"
        "(Descrever desvios, ex: número de chamado, ocorrências e etc...)"
    )

    feedback = f"📌 Feedback Operacional {{{dep}}} – {data_extracao}\n\n"

    if "DETALHE DESVIO" not in df.columns:
        return feedback + "(sem coluna DETALHE DESVIO)\n"

    df["DETALHE DESVIO"] = df["DETALHE DESVIO"].astype(str)

    # ---------------------- TOP DESVIOS ----------------------
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

    # ---------------------- TOP VOOS ----------------------
    if "VOO" in df.columns:
        top_voos = df["VOO"].value_counts().head(2)
        feedback += "✈️ Voos mais impactados do dia:\n"
        for voo, qtd in top_voos.items():
            feedback += f"- {voo}: **{qtd} guias**\n"
        feedback += "\n"

    # ---------------------- RESUMO GERAL ----------------------
    feedback += (
        "👉 Resumo geral de inconsistências:\n"
        f"- ❗ **Erro de manifesto:** {total_erro_manifesto} guias\n"
        f"- 📄 **Guias sem manifesto:** {total_sem_manifesto} guias\n"
        f"- 📝 **Erro de Scorecard:** {total_scorecard} guias\n"
        f"- ⛔ **Perdas de DEP:** {total_perca_dep} guias\n\n"
    )

    # ==========================================================
    # FUNÇÃO DE AGRUPAR OBSERVAÇÕES (idêntica ao Colab)
    # ==========================================================
    def obs_agrupadas(df_grupo):
        if col_obs not in df_grupo.columns:
            return " - "
        obs = df_grupo[col_obs].astype(str).str.strip()
        obs = obs[~obs.str.lower().isin(["nan", "", "none"])]
        if obs.empty:
            return " - "
        return " --> " + " | ".join(obs.unique())

    # ==========================================================
    # TURNOS — Mesma lógica do Colab
    # ==========================================================
    if "TURNO" in df.columns:
        ordem_turnos = ["MANHÃ", "TARDE", "MADRUGADA"]

        for turno in ordem_turnos:
            bloco = df[df["TURNO"] == turno]
            if bloco.empty:
                continue

            icone = "🌅" if turno == "MANHÃ" else "🌤️" if turno == "TARDE" else "🌙"
            feedback += f"{icone} Turno {turno.title()}\n"
            feedback += f"📦 Total: **{len(bloco)} guias**\n\n"

            # Maiores destinos
            if "DESTINO" in bloco.columns:
                dests = bloco["DESTINO"].value_counts().head(3)
                feedback += "📍 Maiores destinos:\n"
                for i, (dest, qtd) in enumerate(dests.items(), 1):
                    feedback += f"{i}️⃣ {dest} → **{qtd} guias**\n"
                feedback += "\n"

            # --------------------------------------------------
            # AQUI começa o bloco dos desvios (igual ao Colab)
            # --------------------------------------------------
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

                # 🔥 Observações aparecem APENAS AQUI (no total)
                obs_txt = obs_agrupadas(grupo)
                feedback += f"{emoji} {titulo} ({len(grupo)} guias): {obs_txt}\n"

                # 🔥 Agrupamento por voo/destino SEM observações (igual ao Colab)
                grupos_voo = grupo.groupby(["VOO", "DESTINO"])
                for (voo, dest), g in grupos_voo:
                    feedback += f"✈️ {voo} → {dest} → **{len(g)} guias**  -\n"

                feedback += "\n"

    return feedback

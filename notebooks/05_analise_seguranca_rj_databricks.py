# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 05 — Análise | Segurança Pública RJ
# MAGIC
# MAGIC Responde as 6 perguntas de negócio a partir das tabelas Gold:
# MAGIC `fato_criminalidade_cisp` (2003-2026, grão CISP) e
# MAGIC `fato_criminalidade_municipio` (2014-2026, grão município).

# COMMAND ----------

from pyspark.sql import functions as F
import pandas as pd
import matplotlib.pyplot as plt

pd.set_option("display.max_columns", None)

df_cisp = spark.table("projeto_seguranca_rj.gold.fato_criminalidade_cisp")
df_municipio = spark.table("projeto_seguranca_rj.gold.fato_criminalidade_municipio")
df_dim_tempo = spark.table("projeto_seguranca_rj.gold.dim_tempo")
df_dim_municipio = spark.table("projeto_seguranca_rj.gold.dim_municipio")

# COMMAND ----------

def perfil_estatistico(df, coluna):
    """Calcula média, quartis e limites de outlier (IQR) para uma coluna numérica."""
    media = df.agg(F.avg(coluna)).collect()[0][0]
    q1, mediana, q3 = df.approxQuantile(coluna, [0.25, 0.5, 0.75], 0.01)
    iqr = q3 - q1
    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    qtd_outliers = df.filter(
        (F.col(coluna) < limite_inferior) | (F.col(coluna) > limite_superior)
    ).count()
    qtd_total = df.count()

    return {
        "coluna": coluna,
        "media": round(media, 2) if media is not None else None,
        "mediana": mediana,
        "q1": q1,
        "q3": q3,
        "limite_inferior_outlier": round(limite_inferior, 2),
        "limite_superior_outlier": round(limite_superior, 2),
        "qtd_outliers": qtd_outliers,
        "pct_outliers": round(100 * qtd_outliers / qtd_total, 2)
    }

# COMMAND ----------

metricas_municipio = [
    "crimes_violentos",
    "letalidade_violenta",
    "letalidade_violenta_taxa",
    "roubo_rua",
    "roubo_comercio",
    "roubo_veiculo",
    "furto_veiculos",
    "recuperacao_veiculos",
    "indice_recuperacao_veiculos",
    "total_furtos",
    "feminicidio",
    "tentativa_feminicidio"
]

metricas_cisp = [
    "crimes_violentos",
    "letalidade_violenta",
    "roubo_rua",
    "total_furtos",
    "crimes_patrimoniais",
    "apf",
    "cmp",
    "atividade_policial",
    "variacao_crimes_mes_seguinte"
]

# Mantém apenas as métricas presentes em cada fato
metricas_municipio = [c for c in metricas_municipio if c in df_municipio.columns]
metricas_cisp = [c for c in metricas_cisp if c in df_cisp.columns]

perfil_municipio = pd.DataFrame([perfil_estatistico(df_municipio, c) for c in metricas_municipio])
perfil_cisp = pd.DataFrame([perfil_estatistico(df_cisp, c) for c in metricas_cisp])

print("Perfil estatístico — Município")
display(spark.createDataFrame(perfil_municipio))

print("Perfil estatístico — CISP")
display(spark.createDataFrame(perfil_cisp))

# COMMAND ----------

# Boxplot das métricas de fato_criminalidade_cisp (amostra para não sobrecarregar o driver)
amostra_cisp = df_cisp.select(*metricas_cisp).sample(fraction=0.3, seed=42).toPandas()

fig, ax = plt.subplots(figsize=(10, 6))
amostra_cisp.boxplot(ax=ax)
ax.set_title("Distribuição das métricas de criminalidade por CISP/mês")
ax.set_ylabel("Ocorrências no mês")
plt.xticks(rotation=15)
plt.tight_layout()
plt.show()

# TODO: comente no README quais CISPs/meses concentram os outliers de crimes_violentos --
# são eventos pontuais conhecidos (chacinas, operações) ou merecem investigação?

# COMMAND ----------

total_com_indice = df_municipio.filter(F.col("indice_recuperacao_veiculos").isNotNull()).count()
acima_de_1 = df_municipio.filter(F.col("indice_recuperacao_veiculos") > 1).count()

print(f"Linhas com índice de recuperação > 1: {acima_de_1} de {total_com_indice} ({100 * acima_de_1 / total_com_indice:.2f}%)")

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 1 — Crimes violentos por região (2003-2026)
# MAGIC
# MAGIC Usa `fato_criminalidade_cisp`, única com série longa e com `regiao` já
# MAGIC corrigida (encoding de "Grande Niterói" tratado na Gold).
# MAGIC
# MAGIC `crimes_violentos` (calculada na Gold) amplia o escopo de
# MAGIC `letalidade_violenta`: além dos desfechos letais (homicídio doloso, lesão
# MAGIC corporal seguida de morte, latrocínio, morte por intervenção policial),
# MAGIC soma também tentativa de homicídio, lesão corporal dolosa e estupro --
# MAGIC três variáveis que o dicionário do ISP-RJ documenta como independentes,
# MAGIC sem overlap com os componentes letais.

# COMMAND ----------

pergunta1_sdf = (
    df_cisp
    .groupBy("ano", "regiao")
    .agg(
        F.sum("crimes_violentos").alias("crimes_violentos_total"),
        F.sum("letalidade_violenta").alias("letalidade_violenta_total")
    )
    .orderBy("ano", "regiao")
)

pergunta1_df = pergunta1_sdf.toPandas()
display(pergunta1_sdf)

# COMMAND ----------

pivot1 = pergunta1_df.pivot(index="ano", columns="regiao", values="crimes_violentos_total")

fig, ax = plt.subplots(figsize=(12, 6))
pivot1.plot(ax=ax, marker="o")
ax.set_title("Crimes violentos por região (2003-2026)")
ax.set_xlabel("Ano")
ax.set_ylabel("Total de vítimas de crimes violentos")
ax.legend(title="Região")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# TODO (discussão a escrever no README após rodar):
# - Qual região tem a maior queda proporcional desde o pico?
# - Valores são absolutos (não normalizados por população/número de CISPs) --
#   comente essa limitação ao comparar regiões de tamanhos diferentes.
# - Compare com letalidade_violenta_total (também retornada na tabela): a
#   tendência geral muda quando se olha só para os casos letais?

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 2 — CISPs da Capital com maior concentração de crimes patrimoniais

# COMMAND ----------

pergunta2_sdf = (
    df_cisp
    .filter(F.col("regiao") == "Capital")
    .groupBy("cisp")
    .agg(
        F.sum("roubo_rua").alias("roubo_rua_total"),
        F.sum("total_furtos").alias("furtos_total"),
        F.sum("crimes_patrimoniais").alias("crimes_patrimoniais_total")
    )
    .orderBy(F.col("crimes_patrimoniais_total").desc())
)

pergunta2_df = pergunta2_sdf.limit(15).toPandas()
display(pergunta2_sdf.limit(15))

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pergunta2_df["cisp"].astype(str), pergunta2_df["crimes_patrimoniais_total"])
ax.set_xlabel("Total de roubo de rua + furtos (2003-2026)")
ax.set_ylabel("CISP")
ax.set_title("Top 15 CISPs da Capital em crimes patrimoniais")
ax.invert_yaxis()
plt.tight_layout()
plt.show()

total_capital = (
    df_cisp.filter(F.col("regiao") == "Capital")
    .agg(F.sum("crimes_patrimoniais"))
    .collect()[0][0]
)
top5 = pergunta2_df.head(5)["crimes_patrimoniais_total"].sum()
print(f"Top 5 CISPs concentram {100 * top5 / total_capital:.1f}% dos crimes patrimoniais da Capital")

# TODO: a base não traz nome de delegacia -- os CISPs aparecem só pelo código numérico.

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 3 — Atividade policial e crimes patrimoniais do mês seguinte
# MAGIC
# MAGIC `crimes_patrimoniais_mes_seguinte` só é preenchida quando o próximo registro
# MAGIC do CISP é de fato o mês seguinte (checagem de continuidade já feita na Gold),
# MAGIC evitando comparar meses não consecutivos quando há lacuna na série.
# MAGIC
# MAGIC **Importante:** tratada aqui como associação temporal, não causalidade --
# MAGIC mais prisões tendem a ocorrer justamente onde já há mais crime (causalidade
# MAGIC reversa), o que pode inflar uma correlação positiva sem significar que
# MAGIC prender mais gera mais crime depois.

# COMMAND ----------

pergunta3_sdf = df_cisp.filter(F.col("crimes_patrimoniais_mes_seguinte").isNotNull())

correlacao = pergunta3_sdf.stat.corr("atividade_policial", "crimes_patrimoniais_mes_seguinte")
print(f"Correlação (atividade policial x crimes patrimoniais do mês seguinte): {correlacao:.3f}")

pergunta3_df = (
    pergunta3_sdf
    .select("atividade_policial", "crimes_patrimoniais_mes_seguinte")
    .sample(fraction=0.3, seed=42)
    .toPandas()
)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(8, 6))
ax.scatter(
    pergunta3_df["atividade_policial"],
    pergunta3_df["crimes_patrimoniais_mes_seguinte"],
    alpha=0.3, s=10
)
ax.set_xlabel("Atividade policial no mês (APF + Cumprimento de Mandado de Prisão)")
ax.set_ylabel("Crimes patrimoniais no mês seguinte")
ax.set_title(f"Atividade policial x crime do mês seguinte (corr = {correlacao:.2f})")
plt.tight_layout()
plt.show()

pergunta3_regiao_sdf = (
    df_cisp
    .filter(F.col("crimes_patrimoniais_mes_seguinte").isNotNull())
    .groupBy("regiao")
    .agg(F.corr("atividade_policial", "crimes_patrimoniais_mes_seguinte").alias("correlacao"))
)
display(pergunta3_regiao_sdf)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 4 — Evolução do índice de recuperação de veículos
# MAGIC
# MAGIC `indice_recuperacao_veiculos` (Gold) = recuperações / (roubo de veículo +
# MAGIC furto de veículo) no mesmo mês. É uma razão analítica agregada, não
# MAGIC rastreamento do mesmo veículo.

# COMMAND ----------

pergunta4_sdf = (
    df_municipio
    .filter(F.col("indice_recuperacao_veiculos").isNotNull())
    .groupBy("ano", "regiao")
    .agg(F.avg("indice_recuperacao_veiculos").alias("indice_recuperacao_medio"))
    .orderBy("ano", "regiao")
)

pergunta4_df = pergunta4_sdf.toPandas()
display(pergunta4_sdf)

# COMMAND ----------

pivot4 = pergunta4_df.pivot(index="ano", columns="regiao", values="indice_recuperacao_medio")

fig, ax = plt.subplots(figsize=(12, 6))
pivot4.plot(ax=ax, marker="o")
ax.set_title("Índice médio de recuperação de veículos por região (2014-2026)")
ax.set_xlabel("Ano")
ax.set_ylabel("Recuperados / (roubados + furtados)")
ax.legend(title="Região")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Feminicídio e tentativa de feminicídio desde 2015

# COMMAND ----------

pergunta7_sdf = (
    df_municipio

    .withColumn(
        "data_referencia",
        F.make_date(
            F.col("ano"),
            F.col("mes"),
            F.lit(1)
        )
    )

    .filter(
        F.col("data_referencia") >= F.lit("2024-10-01")
    )

    .groupBy(
        "ano",
        "mes",
        "data_referencia"
    )

    .agg(
        F.sum("feminicidio")
        .alias("total_feminicidio"),

        F.sum("tentativa_feminicidio")
        .alias("total_tentativa_feminicidio")
    )

    .orderBy("data_referencia")
)

display(pergunta7_sdf)


# ============================================================
# TRANSFORMA PARA PANDAS
# ============================================================

pergunta7_df = pergunta7_sdf.toPandas()


# ============================================================
# NOMES DOS MESES
# ============================================================

nomes_meses = {
    1: "Jan",
    2: "Fev",
    3: "Mar",
    4: "Abr",
    5: "Mai",
    6: "Jun",
    7: "Jul",
    8: "Ago",
    9: "Set",
    10: "Out",
    11: "Nov",
    12: "Dez"
}

pergunta7_df["periodo"] = (
    pergunta7_df["mes"].map(nomes_meses)
    + "/"
    + pergunta7_df["ano"].astype(str)
)

# COMMAND ----------

# ============================================================
# GRÁFICO
# ============================================================

fig, ax = plt.subplots(figsize=(12, 6))

ax.plot(
    pergunta7_df["periodo"],
    pergunta7_df["total_feminicidio"],
    marker="o",
    label="Feminicídio"
)

ax.plot(
    pergunta7_df["periodo"],
    pergunta7_df["total_tentativa_feminicidio"],
    marker="o",
    label="Tentativa de feminicídio"
)

ax.set_xlabel("Mês")
ax.set_ylabel("Número de registros")
ax.set_title("Evolução mensal de feminicídios e tentativas de feminicídio")
ax.legend()
ax.grid(alpha=0.3)
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 6 — Sazonalidade mensal (roubo de rua x roubo a comércio)

# COMMAND ----------

pergunta6_sdf = (
    df_municipio
    .groupBy("mes")
    .agg(
        F.avg("roubo_rua").alias("media_roubo_rua"),
        F.avg("roubo_comercio").alias("media_roubo_comercio")
    )
    .orderBy("mes")
)

pergunta6_df = pergunta6_sdf.toPandas()
display(pergunta6_sdf)

# COMMAND ----------

# ============================================================
# GRÁFICO 1 — SAZONALIDADE DO ROUBO DE RUA
# ============================================================

fig, ax = plt.subplots(figsize=(10, 5))

ax.plot(
    pergunta6_df["mes"],
    pergunta6_df["media_roubo_rua"],
    marker="o"
)

ax.set_xticks(range(1, 13))
ax.set_xlabel("Mês")
ax.set_ylabel("Média de ocorrências por município")
ax.set_title("Sazonalidade mensal: roubo de rua")

ax.grid(alpha=0.3)

plt.tight_layout()
plt.show()


# COMMAND ----------

# ============================================================
# GRÁFICO 2 — SAZONALIDADE DO ROUBO A COMÉRCIO
# ============================================================

fig, ax = plt.subplots(figsize=(10, 5))

ax.plot(
    pergunta6_df["mes"],
    pergunta6_df["media_roubo_comercio"],
    marker="o"
)

ax.set_xticks(range(1, 13))
ax.set_xlabel("Mês")
ax.set_ylabel("Média de ocorrências por município")
ax.set_title("Sazonalidade mensal: roubo a comércio")

ax.grid(alpha=0.3)

plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo para o README (Etapa 4.5 — Análise de Dados)

# COMMAND ----------

print("PERGUNTA 1 - Crimes violentos por região: ver tabela e gráfico acima")
print("PERGUNTA 2 - Top 5 CISPs concentram {:.1f}% dos crimes patrimoniais da Capital".format(100 * top5 / total_capital))
print(f"PERGUNTA 3 - Correlação geral atividade policial x crime do mês seguinte: {correlacao:.3f}")
print("PERGUNTA 4 - Índice de recuperação de veículos por região/ano: ver tabela e gráfico acima")
print("PERGUNTA 5 - Feminicídio/tentativa por ano e região: ver tabelas e gráfico acima")
print("PERGUNTA 6 - Sazonalidade mensal de roubo de rua/comércio: ver tabela e gráfico acima")
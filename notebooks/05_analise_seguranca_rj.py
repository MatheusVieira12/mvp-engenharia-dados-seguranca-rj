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
from pyspark.sql import Window
import pandas as pd
import matplotlib.pyplot as plt

# COMMAND ----------

pd.set_option("display.max_columns", None)

df_cisp = spark.table("projeto_seguranca_rj.gold.fato_criminalidade_cisp")
df_municipio = spark.table("projeto_seguranca_rj.gold.fato_criminalidade_municipio")

# COMMAND ----------

def perfil_estatistico(df, coluna):
    """Calcula média, quartis e limites de outlier (IQR)."""

    df_valido = df.filter(
        F.col(coluna).isNotNull()
    )

    qtd_total = df_valido.count()

    if qtd_total == 0:
        return {
            "coluna": coluna,
            "media": None,
            "mediana": None,
            "q1": None,
            "q3": None,
            "limite_inferior_outlier": None,
            "limite_superior_outlier": None,
            "qtd_outliers": 0,
            "pct_outliers": 0
        }

    media = (
        df_valido
        .agg(F.avg(coluna))
        .collect()[0][0]
    )

    q1, mediana, q3 = df_valido.approxQuantile(
        coluna,
        [0.25, 0.5, 0.75],
        0.01
    )

    iqr = q3 - q1

    limite_inferior = q1 - 1.5 * iqr
    limite_superior = q3 + 1.5 * iqr

    qtd_outliers = (
        df_valido
        .filter(
            (F.col(coluna) < limite_inferior)
            | (F.col(coluna) > limite_superior)
        )
        .count()
    )

    return {
        "coluna": coluna,
        "media": round(media, 2),
        "mediana": mediana,
        "q1": q1,
        "q3": q3,
        "limite_inferior_outlier":
            round(limite_inferior, 2),
        "limite_superior_outlier":
            round(limite_superior, 2),
        "qtd_outliers": qtd_outliers,
        "pct_outliers":
            round(100 * qtd_outliers / qtd_total, 2)
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

# MAGIC %md
# MAGIC ## Pergunta 1 — Crimes violentos por região (2003-2026)
# MAGIC
# MAGIC Usa `fato_criminalidade_cisp`, única com série longa e com `regiao`
# MAGIC já padronizada na camada Silver, incluindo a correção de
# MAGIC `Grande Niterói`.
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

fig, ax = plt.subplots(figsize=(16, 8))
pivot1.plot(ax=ax, marker="o")
ax.set_title("Crimes violentos por região (2003-2026)")
ax.set_xlabel("Ano")
ax.set_ylabel("Registros de crimes violentos")
ax.legend(title="Região")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

w_pico = Window.partitionBy("regiao").orderBy(
    F.col("crimes_violentos_total").desc()
)

resumo_p1 = (
    pergunta1_sdf
    .withColumn("rn", F.row_number().over(w_pico))
    .filter(F.col("rn") == 1)
    .select(
        "regiao",
        F.col("ano").alias("ano_pico"),
        F.col("crimes_violentos_total").alias("valor_pico")
    )
)

display(resumo_p1)

print("Último mês disponível por ano:")
display(
    df_cisp
    .groupBy("ano")
    .agg(F.max("mes").alias("ultimo_mes_disponivel"))
    .orderBy("ano")
)

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

mapa_cisp = {
    "5": "CISP 5 — Centro/Lapa",
    "16": "CISP 16 — Barra da Tijuca",
    "35": "CISP 35 — Campo Grande",
    "34": "CISP 34 — Bangu",
    "12": "CISP 12 — Copacabana/Leme"
}

pergunta2_df["cisp_rotulo"] = (
    pergunta2_df["cisp"]
    .astype(str)
    .map(mapa_cisp)
    .fillna(
        "CISP "
        + pergunta2_df["cisp"].astype(str)
    )
)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(10, 6))
ax.barh(pergunta2_df["cisp_rotulo"],pergunta2_df["crimes_patrimoniais_total"])
ax.set_xlabel("Total de roubo de rua e furtos (2003-2026)")
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


# COMMAND ----------

# ============================================================
# CRUZAMENTO: OUTLIERS DE CRIMES PATRIMONIAIS x TOP 5 CISPs
# ============================================================

# Mesmo critério IQR usado no perfil estatístico
q1, mediana, q3 = df_cisp.approxQuantile(
    "crimes_patrimoniais",
    [0.25, 0.5, 0.75],
    0.01
)

iqr = q3 - q1
limite_superior = q3 + 1.5 * iqr

print(f"Limite superior de outlier: {limite_superior}")

# Outliers de crimes patrimoniais somente da Capital
outliers_capital = (
    df_cisp
    .filter(
        (F.col("regiao") == "Capital") &
        (F.col("crimes_patrimoniais") > limite_superior)
    )
)

# Quantidade de meses outliers por CISP
ranking_outliers = (
    outliers_capital
    .groupBy("cisp")
    .agg(
        F.count("*").alias("qtd_meses_outliers"),
        F.sum("crimes_patrimoniais").alias("total_crimes_nos_outliers")
    )
    .orderBy(
        F.col("qtd_meses_outliers").desc(),
        F.col("total_crimes_nos_outliers").desc()
    )
)

print("CISPs da Capital com mais meses classificados como outliers:")
display(ranking_outliers.limit(15))

# Top 5 CISPs da Pergunta 2
top5_cisp = (
    pergunta2_sdf
    .limit(5)
    .select("cisp")
    .withColumn("top5_pergunta2", F.lit("SIM"))
)

# Mostra quais CISPs de outliers também pertencem ao Top 5
comparacao_outliers = (
    ranking_outliers
    .join(
        top5_cisp,
        on="cisp",
        how="left"
    )
    .fillna({"top5_pergunta2": "NAO"})
)

print("Comparação com o Top 5 da Pergunta 2:")
display(comparacao_outliers.limit(15))

# Percentual dos outliers da Capital pertencentes ao Top 5
total_outliers_capital = outliers_capital.count()

outliers_top5 = (
    outliers_capital
    .join(
        top5_cisp.select("cisp"),
        on="cisp",
        how="inner"
    )
    .count()
)

percentual_top5_outliers = (
    100 * outliers_top5 / total_outliers_capital
    if total_outliers_capital > 0
    else 0
)

print(f"Outliers de crimes patrimoniais na Capital: {total_outliers_capital}")
print(f"Outliers pertencentes às Top 5 CISPs: {outliers_top5}")
print(
    f"Percentual dos outliers da Capital concentrados nas Top 5 CISPs: "
    f"{percentual_top5_outliers:.1f}%"
)

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
    pergunta3_sdf
    .groupBy("regiao")
    .agg(
        F.corr(
            "atividade_policial",
            "crimes_patrimoniais_mes_seguinte"
        ).alias("correlacao")
    )
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
    .groupBy("ano", "regiao")
    .agg(
        F.sum("recuperacao_veiculos").alias("recuperados"),
        F.sum("veiculos_subtraidos").alias("subtraidos")
    )
    .withColumn(
        "indice_recuperacao",
        F.when(
            F.col("subtraidos") > 0,
            F.col("recuperados") / F.col("subtraidos")
        )
    )
    .orderBy("ano", "regiao")
)

display(pergunta4_sdf)

pergunta4_df = pergunta4_sdf.toPandas()

pivot4 = pergunta4_df.pivot(
    index="ano",
    columns="regiao",
    values="indice_recuperacao"
)

# COMMAND ----------

total_com_indice = (
    df_municipio
    .filter(F.col("indice_recuperacao_veiculos").isNotNull())
    .count()
)

acima_de_1 = (
    df_municipio
    .filter(F.col("indice_recuperacao_veiculos") > 1)
    .count()
)

percentual = (
    100 * acima_de_1 / total_com_indice
    if total_com_indice > 0
    else 0
)

print(
    f"Linhas com índice de recuperação > 1: "
    f"{acima_de_1} de {total_com_indice} "
    f"({percentual:.2f}%)"
)

# COMMAND ----------

fig, ax = plt.subplots(figsize=(12, 6))
pivot4.plot(ax=ax, marker="o")
ax.set_title("Índice de recuperação de veículos por região e ano")
ax.set_xlabel("Ano")
ax.set_ylabel("Recuperados / veículos subtraídos")
ax.legend(title="Região")
ax.grid(alpha=0.3)
plt.tight_layout()
plt.show()

# COMMAND ----------

# MAGIC %md
# MAGIC ## Pergunta 5 — Feminicídio e tentativa de feminicídio a partir de outubro de 2024

# COMMAND ----------

pergunta5_sdf = (
    df_municipio

    .filter(
        F.col("data_referencia")
        >= F.lit("2024-10-01")
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

display(pergunta5_sdf)

pergunta5_df = pergunta5_sdf.toPandas()

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

pergunta5_df["periodo"] = (
    pergunta5_df["mes"].map(nomes_meses)
    + "/"
    + pergunta5_df["ano"].astype(str)
)

# COMMAND ----------

# ============================================================
# GRÁFICO — EVOLUÇÃO MENSAL DE FEMINICÍDIO
# ============================================================

fig, ax = plt.subplots(figsize=(12, 6))

ax.plot(
    pergunta5_df["periodo"],
    pergunta5_df["total_feminicidio"],
    marker="o",
    label="Feminicídio"
)

ax.plot(
    pergunta5_df["periodo"],
    pergunta5_df["total_tentativa_feminicidio"],
    marker="o",
    label="Tentativa de feminicídio"
)

ax.set_xlabel("Mês")
ax.set_ylabel("Número de registros")
ax.set_title(
    "Evolução mensal de feminicídios "
    "e tentativas de feminicídio"
)

ax.legend()
ax.grid(alpha=0.3)

plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# COMMAND ----------

print(
    "Maior número mensal de feminicídios:"
)

display(
    pergunta5_sdf
    .orderBy(
        F.col(
            "total_feminicidio"
        ).desc()
    )
    .limit(5)
)

print(
    "Maior número mensal de tentativas "
    "de feminicídio:"
)

display(
    pergunta5_sdf
    .orderBy(
        F.col(
            "total_tentativa_feminicidio"
        ).desc()
    )
    .limit(5)
)

display(
    pergunta5_sdf.agg(
        F.sum(
            "total_feminicidio"
        ).alias(
            "total_feminicidios"
        ),

        F.sum(
            "total_tentativa_feminicidio"
        ).alias(
            "total_tentativas"
        )
    )
)

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

mensal_p6 = (
    df_municipio
    .groupBy("ano", "mes")
    .agg(
        F.sum("roubo_rua").alias("roubo_rua_total"),
        F.sum("roubo_comercio").alias("roubo_comercio_total")
    )
)

w_rua = Window.partitionBy("ano").orderBy(
    F.col("roubo_rua_total").desc()
)

w_comercio = Window.partitionBy("ano").orderBy(
    F.col("roubo_comercio_total").desc()
)

top_rua = (
    mensal_p6
    .withColumn(
    "posicao",
    F.dense_rank().over(w_rua)
)
    .filter(F.col("posicao") == 1)
    .select(
        "ano",
        F.col("mes").alias("mes_destaque"),
        "roubo_rua_total"
    )
    .orderBy("ano")
)

top_comercio = (
    mensal_p6
    .withColumn(
    "posicao",
    F.dense_rank().over(w_comercio)
)
    .filter(F.col("posicao") == 1)
    .select(
        "ano",
        F.col("mes").alias("mes_destaque"),
        "roubo_comercio_total"
    )
    .orderBy("ano")
)

print("Mês com maior roubo de rua em cada ano:")
display(top_rua)

print("Mês com maior roubo a comércio em cada ano:")
display(top_comercio)

# COMMAND ----------

repeticao_rua = (
    top_rua
    .groupBy("mes_destaque")
    .count()
    .orderBy(F.col("count").desc())
)

repeticao_comercio = (
    top_comercio
    .groupBy("mes_destaque")
    .count()
    .orderBy(F.col("count").desc())
)

print("Frequência dos meses de maior roubo de rua:")
display(repeticao_rua)

print("Frequência dos meses de maior roubo a comércio:")
display(repeticao_comercio)

# COMMAND ----------

# MAGIC %md
# MAGIC ## Resumo para o README (Etapa 4.5 — Análise de Dados)

# COMMAND ----------

print(
    "PERGUNTA 1 - Crimes violentos por região: "
    "ver tabela, picos regionais e gráfico acima."
)

print(
    "PERGUNTA 2 - Top 5 CISPs concentram "
    "{:.1f}% dos crimes patrimoniais da Capital."
    .format(
        100 * top5 / total_capital
    )
)

print(
    "PERGUNTA 3 - Correlação geral entre "
    "atividade policial e crimes patrimoniais "
    f"do mês seguinte: {correlacao:.3f}."
)

print(
    "PERGUNTA 4 - Índice de recuperação "
    "de veículos por região e ano: "
    "ver tabela e gráfico acima."
)

print(
    "PERGUNTA 5 - Evolução mensal de "
    "feminicídio e tentativa de feminicídio "
    "a partir de outubro de 2024."
)

print(
    "PERGUNTA 6 - Meses de maior roubo "
    "de rua e roubo a comércio por ano, "
    "com frequência de repetição dos picos."
)
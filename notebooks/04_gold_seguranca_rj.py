# Databricks notebook source
# /// script
# [tool.databricks.environment]
# environment_version = "6"
# ///
# MAGIC %md
# MAGIC # 04 — Camada Gold | Segurança Pública RJ
# MAGIC
# MAGIC Modelo adotado: **Esquema Estrela simplificado**
# MAGIC
# MAGIC Tabelas:
# MAGIC - `dim_tempo`
# MAGIC - `dim_municipio`
# MAGIC - `fato_criminalidade_municipio`
# MAGIC - `fato_criminalidade_cisp`

# COMMAND ----------

from pyspark.sql import functions as F
from pyspark.sql import Window

# COMMAND ----------

# MAGIC %md
# MAGIC ## 0. Limpeza de tabelas antigas
# MAGIC
# MAGIC Uma versão anterior deste pipeline criou também uma `dim_cisp`, que deixou de
# MAGIC existir neste modelo (os atributos de CISP passaram a viver dentro da própria
# MAGIC `fato_criminalidade_cisp`). Removemos a tabela órfã para não deixar lixo no
# MAGIC Unity Catalog.

# COMMAND ----------

spark.sql("CREATE SCHEMA IF NOT EXISTS projeto_seguranca_rj.gold")

spark.sql("DROP TABLE IF EXISTS projeto_seguranca_rj.gold.dim_cisp")

df_ocorrencias = spark.table("projeto_seguranca_rj.silver.ocorrencias_municipio")

df_taxas = spark.table("projeto_seguranca_rj.silver.taxas_municipio")

df_dp = spark.table("projeto_seguranca_rj.silver.dp_municipio")

print("Ocorrências município:", df_ocorrencias.count())
print("Taxas município:", df_taxas.count())
print("CISP:", df_dp.count())

# COMMAND ----------

# MAGIC %md
# MAGIC ## 1. Validação das tabelas Silver

# COMMAND ----------

colunas_obrigatorias = {
    "ocorrencias_municipio": [
        "fmun_cod", "fmun", "regiao", "ano", "mes",
        "letalidade_violenta", "hom_doloso", "lesao_corp_morte",
        "latrocinio", "hom_por_interv_policial", "tentat_hom",
        "lesao_corp_dolosa", "estupro", "roubo_rua", "roubo_comercio",
        "roubo_veiculo", "furto_veiculos",
        "recuperacao_veiculos", "total_furtos",
        "feminicidio", "tentativa_feminicidio"
    ],
    "taxas_municipio": [
        "fmun_cod", "ano", "mes", "letalidade_violenta"
    ],
    "dp_municipio": [
        "cisp", "aisp", "risp", "munic", "mcirc", "regiao",
        "ano", "mes", "roubo_rua", "total_furtos", "apf", "cmp",
        "letalidade_violenta", "hom_doloso", "lesao_corp_morte",
        "latrocinio", "hom_por_interv_policial", "tentat_hom",
        "lesao_corp_dolosa", "estupro"
    ]
}

dfs = {
    "ocorrencias_municipio": df_ocorrencias,
    "taxas_municipio": df_taxas,
    "dp_municipio": df_dp
}

for nome, obrigatorias in colunas_obrigatorias.items():
    faltantes = [c for c in obrigatorias if c not in dfs[nome].columns]
    if faltantes:
        raise ValueError(f"{nome}: colunas ausentes -> {faltantes}")

print("Schemas validados.")

# COMMAND ----------

validacoes_chave = {
    "ocorrencias_municipio": (
        df_ocorrencias,
        ["fmun_cod", "ano", "mes"]
    ),
    "taxas_municipio": (
        df_taxas,
        ["fmun_cod", "ano", "mes"]
    ),
    "dp_municipio": (
        df_dp,
        ["cisp", "munic", "ano", "mes"]
    )
}

for nome, (df, chave) in validacoes_chave.items():
    duplicados = (
        df
        .groupBy(*chave)
        .count()
        .filter(F.col("count") > 1)
    )

    qtd_duplicados = duplicados.count()

    print(
        f"Chaves duplicadas em {nome}:",
        qtd_duplicados
    )

    if qtd_duplicados > 0:
        display(duplicados)

        raise ValueError(
            f"A Silver {nome} possui "
            "duplicidade na chave."
        )

print("Unicidade das tabelas Silver validada.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 2. Dimensão Tempo

# COMMAND ----------

dim_tempo = (
    df_ocorrencias
    .select("ano", "mes")
    .unionByName(df_dp.select("ano", "mes"))
    .distinct()
    .withColumn(
        "data_referencia",
        F.make_date(F.col("ano"), F.col("mes"), F.lit(1))
    )
    .withColumn(
        "ano_mes",
        F.date_format("data_referencia", "yyyy-MM")
    )
    .withColumn(
        "trimestre",
        F.quarter("data_referencia")
    )
    .withColumn(
        "semestre",
        F.when(F.col("mes") <= 6, 1).otherwise(2)
    )
)

display(dim_tempo.orderBy("ano", "mes"))

# COMMAND ----------

(
    dim_tempo.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("projeto_seguranca_rj.gold.dim_tempo")
)

print("dim_tempo salva.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 3. Dimensão Município
# MAGIC
# MAGIC Para garantir uma linha por município, é mantido o cadastro do período mais recente disponível.

# COMMAND ----------

w_municipio = (
    Window
    .partitionBy("fmun_cod")
    .orderBy(F.col("ano").desc(), F.col("mes").desc())
)

dim_municipio = (
    df_ocorrencias
    .select("fmun_cod", "fmun", "regiao", "ano", "mes")
    .withColumn("rn", F.row_number().over(w_municipio))
    .filter(F.col("rn") == 1)
    .select(
        "fmun_cod",
        F.col("fmun").alias("municipio"),
        "regiao"
    )
)

display(dim_municipio.orderBy("municipio"))

# COMMAND ----------

duplicados_dim_municipio = (
    dim_municipio
    .groupBy("fmun_cod")
    .count()
    .filter(F.col("count") > 1)
)

qtd_duplicados_dim = (
    duplicados_dim_municipio.count()
)

print(
    "Duplicidades na dim_municipio:",
    qtd_duplicados_dim
)

if qtd_duplicados_dim > 0:
    display(duplicados_dim_municipio)

    raise ValueError(
        "A dim_municipio possui "
        "fmun_cod duplicado."
    )

print(
    "Unicidade da dim_municipio "
    "validada com sucesso."
)

# COMMAND ----------

(
    dim_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("projeto_seguranca_rj.gold.dim_municipio")
)

print("dim_municipio salva.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 4. Fato Criminalidade por Município
# MAGIC
# MAGIC Combina contagens municipais com a taxa de letalidade violenta.
# MAGIC
# MAGIC Também calcula:
# MAGIC - `veiculos_subtraidos`
# MAGIC - `indice_recuperacao_veiculos`
# MAGIC
# MAGIC Observação: o índice de recuperação é uma razão analítica entre recuperações registradas no mês e roubos + furtos de veículos do mesmo mês. Ele não deve ser interpretado como rastreamento individual dos mesmos veículos.
# MAGIC
# MAGIC `feminicidio` e `tentativa_feminicidio` foram incluídas porque alimentam
# MAGIC diretamente a pergunta de negócio sobre a evolução mensal dos registros
# MAGIC de feminicídio e tentativa de feminicídio a partir de
# MAGIC outubro de 2024.

# COMMAND ----------

taxas_letalidade = (
    df_taxas
    .select(
        "fmun_cod",
        "ano",
        "mes",
        F.col("letalidade_violenta").alias("letalidade_violenta_taxa")
    )
)

fato_criminalidade_municipio = (
    df_ocorrencias
    .select(
        "fmun_cod",
        "fmun",
        "regiao",
        "ano",
        "mes",
        "hom_doloso",
        "lesao_corp_morte",
        "latrocinio",
        "hom_por_interv_policial",
        "letalidade_violenta",
        "tentat_hom",
        "lesao_corp_dolosa",
        "estupro",
        "roubo_rua",
        "roubo_comercio",
        "roubo_veiculo",
        "furto_veiculos",
        "recuperacao_veiculos",
        "total_furtos",
        "feminicidio",
        "tentativa_feminicidio"
    )
    .join(
        taxas_letalidade,
        on=["fmun_cod", "ano", "mes"],
        how="left"
    )
    .withColumn(
        "data_referencia",
        F.make_date(F.col("ano"), F.col("mes"), F.lit(1))
    )
    .withColumn(
        "crimes_violentos",
        F.coalesce(F.col("hom_doloso"), F.lit(0))
        + F.coalesce(F.col("lesao_corp_morte"), F.lit(0))
        + F.coalesce(F.col("latrocinio"), F.lit(0))
        + F.coalesce(F.col("hom_por_interv_policial"), F.lit(0))
        + F.coalesce(F.col("tentat_hom"), F.lit(0))
        + F.coalesce(F.col("lesao_corp_dolosa"), F.lit(0))
        + F.coalesce(F.col("estupro"), F.lit(0))
    )
    .withColumn(
        "veiculos_subtraidos",
        F.coalesce(F.col("roubo_veiculo"), F.lit(0))
        + F.coalesce(F.col("furto_veiculos"), F.lit(0))
    )
    .withColumn(
        "indice_recuperacao_veiculos",
        F.when(
            F.col("veiculos_subtraidos") > 0,
            F.col("recuperacao_veiculos") / F.col("veiculos_subtraidos")
        )
    )
)

display(
    fato_criminalidade_municipio
    .orderBy(
        F.desc("ano"),
        F.desc("mes")
    )
    .limit(10)
)

# COMMAND ----------

duplicados_fato_municipio = (
    fato_criminalidade_municipio
    .groupBy("fmun_cod", "ano", "mes")
    .count()
    .filter(F.col("count") > 1)
)

qtd_dup_fato_municipio = duplicados_fato_municipio.count()

print(
    "Duplicidades na fato municipal:",
    qtd_dup_fato_municipio
)

if qtd_dup_fato_municipio > 0:
    display(duplicados_fato_municipio)

    raise ValueError(
        "A fato municipal ficou duplicada. "
        "Verifique 02_silver_ocorrencias_rj "
        "e 02_silver_taxas_rj."
    )

print("Unicidade da fato municipal validada com sucesso.")

# COMMAND ----------

(
    fato_criminalidade_municipio.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("projeto_seguranca_rj.gold.fato_criminalidade_municipio")
)

print("fato_criminalidade_municipio salva.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 5. Fato Criminalidade por CISP
# MAGIC
# MAGIC Métricas derivadas:
# MAGIC - `atividade_policial = apf + cmp`
# MAGIC - `crimes_patrimoniais = roubo_rua + total_furtos`
# MAGIC - crimes patrimoniais do mês seguinte
# MAGIC - variação para o mês seguinte
# MAGIC
# MAGIC A relação será analisada como **associação temporal**, não como causalidade.
# MAGIC
# MAGIC `crimes_violentos` substitui `letalidade_violenta` como métrica da pergunta 1
# MAGIC (evolução por região). `letalidade_violenta` conta só os desfechos letais
# MAGIC (homicídio doloso + lesão corporal seguida de morte + latrocínio + morte por
# MAGIC intervenção policial). `crimes_violentos` amplia esse escopo somando também
# MAGIC três crimes violentos não-letais que o dicionário do ISP-RJ documenta como
# MAGIC variáveis independentes (não fazem parte de nenhum outro indicador
# MAGIC composto): tentativa de homicídio, lesão corporal dolosa e estupro.
# MAGIC
# MAGIC ```
# MAGIC crimes_violentos = hom_doloso + lesao_corp_morte + latrocinio
# MAGIC                  + hom_por_interv_policial
# MAGIC                  + tentat_hom + lesao_corp_dolosa + estupro
# MAGIC ```
# MAGIC
# MAGIC Os componentes atômicos foram utilizados diretamente para
# MAGIC tornar explícita a composição da métrica analítica
# MAGIC `crimes_violentos`. O campo oficial `letalidade_violenta`
# MAGIC é mantido separadamente, permitindo distinguir a letalidade
# MAGIC dos demais registros de violência considerados na métrica.
# MAGIC
# MAGIC `letalidade_violenta` é mantida como coluna própria (não descartada), para
# MAGIC quem quiser distinguir "só mortes" de "violência em sentido amplo".

# COMMAND ----------

fato_cisp_base = (
    df_dp
    .select(
        "cisp",
        "aisp",
        "risp",
        "munic",
        "mcirc",
        "regiao",
        "ano",
        "mes",
        "hom_doloso",
        "lesao_corp_morte",
        "latrocinio",
        "hom_por_interv_policial",
        "letalidade_violenta",
        "tentat_hom",
        "lesao_corp_dolosa",
        "estupro",
        "roubo_rua",
        "total_furtos",
        "apf",
        "cmp"
    )
    .withColumn(
        "data_referencia",
        F.make_date(F.col("ano"), F.col("mes"), F.lit(1))
    )
    .withColumn(
        "crimes_violentos",
        F.coalesce(F.col("hom_doloso"), F.lit(0))
        + F.coalesce(F.col("lesao_corp_morte"), F.lit(0))
        + F.coalesce(F.col("latrocinio"), F.lit(0))
        + F.coalesce(F.col("hom_por_interv_policial"), F.lit(0))
        + F.coalesce(F.col("tentat_hom"), F.lit(0))
        + F.coalesce(F.col("lesao_corp_dolosa"), F.lit(0))
        + F.coalesce(F.col("estupro"), F.lit(0))
    )
    .withColumn(
        "atividade_policial",
        F.coalesce(F.col("apf"), F.lit(0))
        + F.coalesce(F.col("cmp"), F.lit(0))
    )
    .withColumn(
        "crimes_patrimoniais",
        F.coalesce(F.col("roubo_rua"), F.lit(0))
        + F.coalesce(F.col("total_furtos"), F.lit(0))
    )
)

# COMMAND ----------

# MAGIC %md
# MAGIC ### Checagem de consistência: `letalidade_violenta` bate com seus componentes?
# MAGIC
# MAGIC O dicionário do ISP-RJ documenta `letalidade_violenta` como
# MAGIC `hom_doloso + lesao_corp_morte + latrocinio + hom_por_interv_policial`.
# MAGIC Essa checagem é informativa (não trava a execução): Essa checagem é informativa e não interrompe a execução.
# MAGIC Pequenas divergências entre o campo oficial
# MAGIC `letalidade_violenta` e a soma de seus componentes foram
# MAGIC identificadas na fonte. Por isso, a quantidade e o percentual
# MAGIC são apresentados para acompanhamento da qualidade dos dados.

# COMMAND ----------

divergencia_letalidade = (
    fato_cisp_base
    .withColumn(
        "letalidade_calculada",
        F.coalesce(F.col("hom_doloso"), F.lit(0))
        + F.coalesce(F.col("lesao_corp_morte"), F.lit(0))
        + F.coalesce(F.col("latrocinio"), F.lit(0))
        + F.coalesce(F.col("hom_por_interv_policial"), F.lit(0))
    )
    .filter(F.col("letalidade_calculada") != F.col("letalidade_violenta"))
)

qtd_divergencia = divergencia_letalidade.count()
qtd_total = fato_cisp_base.count()

percentual_divergencia = (
    100 * qtd_divergencia / qtd_total
    if qtd_total > 0
    else 0
)

print(
    "Linhas onde letalidade_violenta diverge "
    "da soma dos componentes: "
    f"{qtd_divergencia} de {qtd_total} "
    f"({percentual_divergencia:.2f}%)"
)

w_cisp = (
    Window
    .partitionBy("cisp")
    .orderBy("data_referencia")
)

fato_criminalidade_cisp = (
    fato_cisp_base
    .withColumn(
        "_data_proximo_registro",
        F.lead("data_referencia", 1).over(w_cisp)
    )
    .withColumn(
        "_crimes_proximo_registro",
        F.lead("crimes_patrimoniais", 1).over(w_cisp)
    )
    .withColumn(
        "crimes_patrimoniais_mes_seguinte",
        F.when(
            F.col("_data_proximo_registro")
            == F.add_months(F.col("data_referencia"), 1),
            F.col("_crimes_proximo_registro")
        )
    )
    .withColumn(
        "variacao_crimes_mes_seguinte",
        F.when(
            F.col("crimes_patrimoniais_mes_seguinte").isNotNull(),
            F.col("crimes_patrimoniais_mes_seguinte")
            - F.col("crimes_patrimoniais")
        )
    )
    .drop("_data_proximo_registro", "_crimes_proximo_registro")
)

display(
    fato_criminalidade_cisp
    .orderBy("cisp", "ano", "mes")
    .limit(10)
)

# COMMAND ----------

duplicados_fato_cisp = (
    fato_criminalidade_cisp
    .groupBy("cisp", "ano", "mes")
    .count()
    .filter(F.col("count") > 1)
)

qtd_dup_fato_cisp = duplicados_fato_cisp.count()

print(
    "Duplicidades na fato CISP:",
    qtd_dup_fato_cisp
)

if qtd_dup_fato_cisp > 0:
    display(duplicados_fato_cisp)

    raise ValueError(
        "A fato CISP ficou duplicada. "
        "Verifique 02_silver_dp_rj."
    )

print(
    "Unicidade da fato CISP "
    "validada com sucesso."
)

# COMMAND ----------

(
    fato_criminalidade_cisp.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("projeto_seguranca_rj.gold.fato_criminalidade_cisp")
)

print("fato_criminalidade_cisp salva.")

# COMMAND ----------

# MAGIC %md
# MAGIC ## 6. Validação final

# COMMAND ----------

validacoes_gold = {
    "dim_tempo": dim_tempo,
    "dim_municipio": dim_municipio,
    "fato_criminalidade_municipio":
        fato_criminalidade_municipio,
    "fato_criminalidade_cisp":
        fato_criminalidade_cisp
}

for tabela, df in validacoes_gold.items():
    nome_completo = (
        f"projeto_seguranca_rj.gold.{tabela}"
    )

    linhas_dataframe = df.count()

    linhas_tabela = (
        spark.table(nome_completo)
        .count()
    )

    print(
        f"{tabela}: "
        f"DataFrame={linhas_dataframe} | "
        f"Tabela={linhas_tabela}"
    )

    if linhas_dataframe != linhas_tabela:
        raise ValueError(
            f"Divergência na persistência de {tabela}."
        )

print(
    "Persistência das quatro tabelas Gold "
    "validada com sucesso."
)

# COMMAND ----------

spark.sql("""
SHOW TABLES IN projeto_seguranca_rj.gold
""").show(truncate=False)
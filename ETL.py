import dlt
from pyspark.sql import functions as F
from pyspark.sql.functions import (
    col, when, lag, lit, lpad, concat_ws, quarter, to_date, create_map, row_number
)
from pyspark.sql.window import Window

# Caminho onde estão os ficheiros de origem
SOURCE_PATH = "/Volumes/tad/default/etl_data"


def filtro_continente():
    """Remove Açores e Madeira, mantendo apenas Portugal Continental."""
    return ~(
        col("nuts_1").rlike("(?i)pt20|pt30|açores|acores|madeira|região autónoma dos açores|regiao autonoma dos acores|região autónoma da madeira|regiao autonoma da madeira")
        | col("nuts_2").rlike("(?i)pt20|pt30|açores|acores|madeira|região autónoma dos açores|regiao autonoma dos acores|região autónoma da madeira|regiao autonoma da madeira")
        | col("nuts_3").rlike("(?i)pt20|pt30|açores|acores|madeira|região autónoma dos açores|regiao autonoma dos acores|região autónoma da madeira|regiao autonoma da madeira")
    )


# ============================================================
# BRONZE TABLES - leitura dos ficheiros originais
# ============================================================

@dlt.table(
    name="bronze_alojamento_local",
    comment="Tabela bronze com os dados brutos de alojamento local."
)
def bronze_alojamento_local():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/alojamento_local_2014_2024.csv")
        .withColumnRenamed("NUTS I", "NUTS_I")
        .withColumnRenamed("NUTS II", "NUTS_II")
        .withColumnRenamed("NUTS III", "NUTS_III")
    )


@dlt.table(
    name="bronze_dormidas",
    comment="Tabela bronze com os dados brutos de dormidas mensais por município."
)
def bronze_dormidas():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/dormidas_municipios_mensal_2014_2024_hierarquico.csv")
        .withColumnRenamed("NUTS I", "NUTS_I")
        .withColumnRenamed("NUTS II", "NUTS_II")
        .withColumnRenamed("NUTS III", "NUTS_III")
    )


@dlt.table(
    name="bronze_populacao",
    comment="Tabela bronze com os dados brutos de população residente anual por município."
)
def bronze_populacao():
    return (
        spark.read
        .format("csv")
        .option("header", "true")
        .option("inferSchema", "true")
        .load(f"{SOURCE_PATH}/populacao_municipios_2014_2024.csv")
        .withColumnRenamed("NUTS I", "NUTS_I")
        .withColumnRenamed("NUTS II", "NUTS_II")
        .withColumnRenamed("NUTS III", "NUTS_III")
    )


# ============================================================
# SILVER TABLES - limpeza, normalização e integração
# ============================================================

@dlt.table(
    name="silver_alojamento_local",
    comment="Dados de alojamento local limpos e normalizados."
)
@dlt.expect_or_drop("ano_mes_valido", "ano_mes IS NOT NULL")
@dlt.expect_or_drop("municipio_valido", "municipio IS NOT NULL")
def silver_alojamento_local():
    month_num_to_name = {
        1: "Janeiro", 2: "Fevereiro", 3: "Março", 4: "Abril",
        5: "Maio", 6: "Junho", 7: "Julho", 8: "Agosto",
        9: "Setembro", 10: "Outubro", 11: "Novembro", 12: "Dezembro"
    }
    month_num_to_name_expr = create_map([lit(x) for pair in month_num_to_name.items() for x in pair])

    df = dlt.read("bronze_alojamento_local")

    if "Selo_clean_safe" in df.columns:
        df = df.drop("Selo_clean_safe")

    df = (
        df
        .withColumnRenamed("Mes", "mes_num")
        .withColumnRenamed("Modalidade", "tipo_alojamento")
        .withColumnRenamed("Nr_utentes", "capacidade_hospedes")
    )

    df = df.withColumn(
        "tipo_alojamento",
        when(col("tipo_alojamento") == "EstabelecimentoHospedagem", "Estabelecimento_hospedagem")
        .when(col("tipo_alojamento") == "EstabelecimentoHospedagemHostel", "Hostel")
        .otherwise(col("tipo_alojamento"))
    )

    df = df.withColumn("mes_nome", month_num_to_name_expr[col("mes_num")])
    df = df.withColumn("ano_mes", concat_ws("-", col("Ano"), lpad(col("mes_num"), 2, "0")))
    df = df.withColumn("trimestre", quarter(to_date(concat_ws("-", col("Ano"), col("mes_num"), lit("01")))))

    df = df.withColumn(
        "estacao",
        when(col("mes_num").isin(12, 1, 2), "Inverno")
        .when(col("mes_num").isin(3, 4, 5), "Primavera")
        .when(col("mes_num").isin(6, 7, 8), "Verão")
        .otherwise("Outono")
    )

    df = df.select(
        col("ano_mes"),
        col("Ano").alias("ano"),
        col("mes_num"),
        col("mes_nome"),
        col("trimestre"),
        col("estacao"),
        col("NUTS_I").alias("nuts_1"),
        col("NUTS_II").alias("nuts_2"),
        col("NUTS_III").alias("nuts_3"),
        col("Municipio").alias("municipio"),
        col("tipo_alojamento"),
        col("capacidade_hospedes")
    )

    return df.filter(filtro_continente())


@dlt.table(
    name="silver_dormidas",
    comment="Dados de dormidas limpos e normalizados."
)
@dlt.expect_or_drop("ano_mes_valido", "ano_mes IS NOT NULL")
@dlt.expect_or_drop("municipio_valido", "municipio IS NOT NULL")
def silver_dormidas():
    month_name_to_num = {
        "Janeiro": 1, "Fevereiro": 2, "Março": 3, "Abril": 4,
        "Maio": 5, "Junho": 6, "Julho": 7, "Agosto": 8,
        "Setembro": 9, "Outubro": 10, "Novembro": 11, "Dezembro": 12
    }
    month_name_to_num_expr = create_map([lit(x) for pair in month_name_to_num.items() for x in pair])

    df = (
        dlt.read("bronze_dormidas")
        .withColumnRenamed("mes", "mes_nome")
        .withColumnRenamed("nr_dormidas", "total_dormidas")
    )

    df = df.withColumn("mes_num", month_name_to_num_expr[col("mes_nome")])
    df = df.withColumn("ano_mes", concat_ws("-", col("ano"), lpad(col("mes_num"), 2, "0")))
    df = df.withColumn("trimestre", quarter(to_date(concat_ws("-", col("ano"), col("mes_num"), lit("01")))))

    df = df.withColumn(
        "estacao",
        when(col("mes_num").isin(12, 1, 2), "Inverno")
        .when(col("mes_num").isin(3, 4, 5), "Primavera")
        .when(col("mes_num").isin(6, 7, 8), "Verão")
        .otherwise("Outono")
    )

    df = df.select(
        col("ano_mes"),
        col("ano"),
        col("mes_num"),
        col("mes_nome"),
        col("trimestre"),
        col("estacao"),
        col("NUTS_I").alias("nuts_1"),
        col("NUTS_II").alias("nuts_2"),
        col("NUTS_III").alias("nuts_3"),
        col("Municipio").alias("municipio"),
        col("total_dormidas")
    )

    return df.filter(filtro_continente())


@dlt.table(
    name="silver_populacao",
    comment="Dados de população residente limpos e normalizados."
)
@dlt.expect_or_drop("ano_valido", "ano IS NOT NULL")
@dlt.expect_or_drop("municipio_valido", "municipio IS NOT NULL")
def silver_populacao():
    df = (
        dlt.read("bronze_populacao")
        .withColumnRenamed("Nr_populacao", "populacao_residente")
        .select(
            col("Ano").alias("ano"),
            col("NUTS_I").alias("nuts_1"),
            col("NUTS_II").alias("nuts_2"),
            col("NUTS_III").alias("nuts_3"),
            col("Municipio").alias("municipio"),
            col("populacao_residente")
        )
    )

    return df.filter(filtro_continente())


@dlt.table(
    name="silver_dormidas_populacao",
    comment="Tabela integrada com dormidas mensais e população anual por município."
)
def silver_dormidas_populacao():
    return (
        dlt.read("silver_dormidas")
        .join(
            dlt.read("silver_populacao"),
            on=["ano", "nuts_1", "nuts_2", "nuts_3", "municipio"],
            how="left"
        )
    )


# ============================================================
# GOLD TABLES - dimensões e factos do Data Warehouse
# ============================================================

@dlt.table(
    name="dim_data",
    comment="Dimensão temporal do modelo em estrela."
)
def dim_data():
    df = dlt.read("silver_alojamento_local").select(
        "ano", "ano_mes", "mes_num", "mes_nome", "trimestre", "estacao"
    ).union(
        dlt.read("silver_dormidas").select(
            "ano", "ano_mes", "mes_num", "mes_nome", "trimestre", "estacao"
        )
    ).distinct()

    w_data = Window.orderBy("ano", "mes_num")
    return df.withColumn("tempo_id", row_number().over(w_data)).select(
        "tempo_id", "ano", "mes_num", "mes_nome", "ano_mes", "trimestre", "estacao"
    )


@dlt.table(
    name="dim_geografia",
    comment="Dimensão geográfica do modelo em estrela."
)
def dim_geografia():
    df = dlt.read("silver_alojamento_local").select(
        "municipio", "nuts_3", "nuts_2"
    ).union(
        dlt.read("silver_dormidas").select(
            "municipio", "nuts_3", "nuts_2"
        )
    ).distinct()

    w_geo = Window.orderBy("municipio")
    return df.withColumn("geografia_id", row_number().over(w_geo)).select(
        "geografia_id", "municipio", "nuts_3", "nuts_2"
    )


@dlt.table(
    name="gold_fact_alojamento_base",
    comment="Fact table intermédia de alojamento antes da atribuição das chaves das dimensões."
)
def gold_fact_alojamento_base():
    return (
        dlt.read("silver_alojamento_local")
        .groupBy(
            "ano", "ano_mes", "mes_num", "mes_nome", "trimestre", "estacao",
            "municipio", "nuts_1", "nuts_2", "nuts_3", "tipo_alojamento"
        )
        .agg(
            F.count("*").alias("numero_alojamentos"),
            F.sum("capacidade_hospedes").alias("capacidade_total")
        )
        .withColumn(
            "capacidade_por_alojamento",
            col("capacidade_total") / col("numero_alojamentos")
        )
    )


@dlt.table(
    name="gold_fact_dormidas_base",
    comment="Fact table intermédia de dormidas antes da atribuição das chaves das dimensões."
)
def gold_fact_dormidas_base():
    df = (
        dlt.read("silver_dormidas_populacao")
        .groupBy(
            "ano", "ano_mes", "mes_num", "mes_nome", "trimestre", "estacao",
            "municipio", "nuts_1", "nuts_2", "nuts_3"
        )
        .agg(
            F.sum("total_dormidas").alias("numero_dormidas"),
            F.first("populacao_residente", ignorenulls=True).alias("populacao_residente")
        )
        .withColumn(
            "dormidas_por_habitante",
            col("numero_dormidas") / col("populacao_residente")
        )
    )

    w = Window.partitionBy("municipio").orderBy("ano", "ano_mes")

    return (
        df.withColumn("dormidas_mes_anterior", lag("numero_dormidas").over(w))
        .withColumn(
            "taxa_crescimento_dormidas",
            when(
                (col("dormidas_mes_anterior").isNull()) |
                (col("dormidas_mes_anterior") == 0),
                None
            ).otherwise(
                (col("numero_dormidas") - col("dormidas_mes_anterior")) /
                col("dormidas_mes_anterior")
            )
        )
        .drop("dormidas_mes_anterior")
    )


@dlt.table(
    name="fact_alojamento",
    comment="Fact table final de alojamento com chaves estrangeiras para as dimensões."
)
def fact_alojamento():
    fa = dlt.read("gold_fact_alojamento_base").alias("fa")
    dd = dlt.read("dim_data").alias("dd")
    dg = dlt.read("dim_geografia").alias("dg")

    df = (
        fa
        .join(dd, fa["ano_mes"] == dd["ano_mes"], "left")
        .join(dg, fa["municipio"] == dg["municipio"], "left")
        .select(
            col("dd.tempo_id"),
            col("dg.geografia_id"),
            col("fa.tipo_alojamento"),
            col("fa.numero_alojamentos"),
            col("fa.capacidade_total"),
            col("fa.capacidade_por_alojamento")
        )
    )

    w = Window.orderBy("tempo_id", "geografia_id", "tipo_alojamento")
    return df.withColumn("fact_alojamento_id", row_number().over(w)).select(
        "fact_alojamento_id",
        "tempo_id",
        "geografia_id",
        "tipo_alojamento",
        "numero_alojamentos",
        "capacidade_total",
        "capacidade_por_alojamento"
    )


@dlt.table(
    name="fact_dormidas",
    comment="Fact table final de dormidas com chaves estrangeiras para as dimensões."
)
def fact_dormidas():
    fd = dlt.read("gold_fact_dormidas_base").alias("fd")
    dd = dlt.read("dim_data").alias("dd")
    dg = dlt.read("dim_geografia").alias("dg")

    df = (
        fd
        .join(dd, fd["ano_mes"] == dd["ano_mes"], "left")
        .join(dg, fd["municipio"] == dg["municipio"], "left")
        .select(
            col("dd.tempo_id"),
            col("dg.geografia_id"),
            col("fd.numero_dormidas"),
            col("fd.populacao_residente"),
            col("fd.dormidas_por_habitante"),
            col("fd.taxa_crescimento_dormidas")
        )
    )

    w = Window.orderBy("tempo_id", "geografia_id")
    return df.withColumn("fact_dormidas_id", row_number().over(w)).select(
        "fact_dormidas_id",
        "tempo_id",
        "geografia_id",
        "numero_dormidas",
        "populacao_residente",
        "dormidas_por_habitante",
        "taxa_crescimento_dormidas"
    )

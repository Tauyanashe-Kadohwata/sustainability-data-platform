from pyspark.sql.functions import lit
from pyspark.sql.window import Window
#--------------------------------------------- GOLD TABLE -------------------------------------------------------#

display(spark.table("dbw_sustainability_platform.default.silver_esg_news"))

#------------------------------------------dim_published_date-------------------------------------------------------#
dim_published_date = silver_df.select(
    "published_date"
).distinct()
dim_published_date = dim_published_date.withColumnRenamed("published_date", "full_date")
dim_published_date = dim_published_date.withColumn(
    "month",
    F.month("full_date")
)
dim_published_date = dim_published_date.withColumn(
    "published_date_key",
    F.date_format("full_date", "yyyyMMdd")
)
dim_published_date = dim_published_date.withColumn(
    "weekday",
    F.day("full_date")
)
dim_published_date = dim_published_date.withColumn(
    "year",
    F.year("full_date")
)
dim_published_date = dim_published_date.withColumn(
    "quarter",
    F.quarter("full_date")
)
dim_published_date.show(5)

#---------------------------------------------- dim_source ----------------------------------------------------#
dim_source = silver_df.select(
    "source_channel",
    "scraper_engine"
).distinct()

window = Window.orderBy("source_channel")

dim_source = dim_source.withColumn(
    "source_key",
    F.row_number().over(window)
)

dim_source.show(5)

#---------------------------------------------- dim_esg_pillar ------------------------------------------------#
dim_esg_pillar = silver_df.select(
    "suggested_esg_pillar"
).distinct()

dim_esg_pillar = dim_esg_pillar.withColumnRenamed("suggested_esg_pillar", "pillar")

window = Window.orderBy("pillar")

dim_esg_pillar = dim_esg_pillar.withColumn(
    "pillar_key",
    F.row_number().over(window)
)

dim_esg_pillar.show(5)

#--------------------------------------------- dim_severity --------------------------------------------------#
dim_severity = silver_df.select(
    "initial_severity_score"
).distinct()

dim_severity = dim_severity.withColumnRenamed("initial_severity_score", "severity")

window = Window.orderBy("severity")

dim_severity = dim_severity.withColumn(
    "severity_key",
    F.row_number().over(window)
)

dim_severity.show(5)

#---------------------------------------------dim_extracted_date-------------------------------------------------#
dim_extracted_date = silver_df.select(
    "extraction_date"
).distinct()
dim_extracted_date = dim_extracted_date.withColumnRenamed("extraction_date", "full_date")
dim_extracted_date = dim_extracted_date.withColumn(
    "month",
    F.month("full_date")
)
dim_extracted_date = dim_extracted_date.withColumn(
    "extracted_date_key",
    F.date_format("full_date", "yyyyMMdd")
)
dim_extracted_date = dim_extracted_date.withColumn(
    "weekday",
    F.day("full_date")
)
dim_extracted_date = dim_extracted_date.withColumn(
    "year",
    F.year("full_date")
)
dim_extracted_date = dim_extracted_date.withColumn(
    "quarter",
    F.quarter("full_date")
)
dim_extracted_date.show(5)

#------------------------------------------- fact_esg_articles ---------------------------------------------------#
facts_esg_articles = silver_df.select(
   "article_id",
   "published_date",
    "extraction_date",
    "source_channel",
    "suggested_esg_pillar",
    "initial_severity_score"
)

#-------------------------------------------- JOINs
facts_esg_articles = facts_esg_articles.join(
    dim_published_date, 
    facts_esg_articles["published_date"] == dim_published_date["full_date"],
     "inner")
facts_esg_articles = facts_esg_articles.join(
    dim_extracted_date, 
    facts_esg_articles["extraction_date"] == dim_extracted_date["full_date"],
     "inner")
facts_esg_articles = facts_esg_articles.join(
    dim_esg_pillar, 
    facts_esg_articles["suggested_esg_pillar"] == dim_esg_pillar["pillar"],
     "inner")
facts_esg_articles = facts_esg_articles.join(
    dim_severity, 
    facts_esg_articles["initial_severity_score"] == dim_severity["severity"],
     "inner")
facts_esg_articles = facts_esg_articles.join(
    dim_source, 
    facts_esg_articles["source_channel"] == dim_source["source_channel"],
     "inner")

#-----------------------------------------------Drop
facts_esg_articles = facts_esg_articles.select(
    "article_id",
    "extracted_date_key",
    "published_date_key",
    "severity_key",
    "source_key",
    "pillar_key"
)

facts_esg_articles.show(3)
#------------------------------------------- fact_article_entities ---------------------------------------------#
fact_articles_entities = silver_df.select(
    "inferred_entities",
    "article_id"
).distinct()
fact_articles_entities = fact_articles_entities.withColumn(
    "entity_struct",
    F.explode("inferred_entities")
)
fact_articles_entities = fact_articles_entities.select(
    "article_id",

    F.col("entity_struct.confidence").alias("confidence"),
    F.col("entity_struct.entity_name").alias("entity_name"),
    F.col("entity_struct.type").alias("entity_type")
)

fact_articles_entities.show(5)

#---------------------------------------------- Save Tables ----------------------------------------------------#
facts_esg_articles.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.facts_esg_articles")

fact_articles_entities.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.fact_articles_entities")

dim_source.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.dim_source")

dim_extracted_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.dim_extracted_date")

dim_published_date.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.dim_published_date")

dim_esg_pillar.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.dim_esg_pillar")

dim_severity.write \
    .format("delta") \
    .mode("overwrite") \
    .saveAsTable("dbw_sustainability_platform.default.dim_severity")
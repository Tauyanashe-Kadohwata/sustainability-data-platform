from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, BooleanType

# -------------------- Schema for raw_tags_json -------------------- #
tags_schema = StructType([
    StructField("suggested_esg_pillar", StringType(), True),
    StructField("initial_severity_score", StringType(), True),
    StructField("flagged_by_automation", BooleanType(), True)
])

# -------------------- Silver Transformations -------------------- #
silver_df = (
    bronze_df

    # Parse JSON string into a struct
    .withColumn(
        "raw_tags",
        F.from_json(F.col("raw_tags_json"), tags_schema)
    )

    # Clean HTML from body text
    .withColumn(
        "body_text",
        F.regexp_replace(
            F.col("article_data.body_text"),
            r"</?(div|p)>",
            " "
        )
    )

    # Published date/time
    .withColumn(
        "published_date",
        F.to_date("article_data.published_timestamp")
    )
    .withColumn(
        "published_time",
        F.date_format("article_data.published_timestamp", "HH:mm:ss")
    )

    # Extraction date/time
    .withColumn(
        "extraction_date",
        F.to_date("metadata.extracted_at")
    )
    .withColumn(
        "extraction_time",
        F.date_format("metadata.extracted_at", "HH:mm:ss")
    )

    # Final Silver schema
    .select(
        F.col("article_data.id").alias("article_id"),
        F.col("article_data.headline").alias("headline"),
        F.col("body_text"),
        F.col("article_data.inferred_entities").alias("inferred_entities"),
        F.col("published_date"),
        F.col("published_time"),

        F.col("metadata.scraper_engine").alias("scraper_engine"),
        F.col("metadata.source_channel").alias("source_channel"),
        F.col("extraction_date"),
        F.col("extraction_time"),

        F.col("raw_tags.suggested_esg_pillar").alias("suggested_esg_pillar"),
        F.col("raw_tags.initial_severity_score").alias("initial_severity_score"),
        F.col("raw_tags.flagged_by_automation").alias("flagged_by_automation"),

        F.col("_rescued_data")
    )
)

# ---------------------------------------------------------- Save Silver Table ---------------------------------- #
( 
 silver_df.write 
 .format("delta") 
 .mode("overwrite") 
 .saveAsTable("dbw_sustainability_platform.default.silver_esg_news") 
 )

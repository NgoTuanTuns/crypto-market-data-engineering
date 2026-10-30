# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "7494be6e-6123-4648-8b0c-15947d86a69a",
# META       "default_lakehouse_name": "crypto",
# META       "default_lakehouse_workspace_id": "bc9c9e37-b69a-4c99-a276-20bdfd99eb01",
# META       "known_lakehouses": [
# META         {
# META           "id": "7494be6e-6123-4648-8b0c-15947d86a69a"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

import pyspark.sql.functions as F
from pyspark.sql import Window

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.readStream.table('bronze.sentiment')


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_published_date = df.withColumn('published_date', F.to_timestamp(F.col('published_date')))

df_critical_null = df_published_date.withColumn(
    '_is_critical_null',
    F.col('url').isNull() | F.col('title').isNull() | F.col('published_date').isNull()
)

df_description_null_handling = df_critical_null.withColumn(
    'description',
    F.when(F.col('description').isNull() | (F.col('description') == ""), F.col('title'))
     .otherwise(F.col('description'))
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_to_silver(batch_df, batch_id):
    spark_session = batch_df.sparkSession
    invalid = batch_df.filter(F.col('_is_critical_null')).drop('_is_critical_null')
    valid = batch_df.filter(~F.col('_is_critical_null')).drop('_is_critical_null')

    invalid.write.format('delta').mode('append').saveAsTable('bronze.sentiment_quarantine')

    if not spark_session.catalog.tableExists('silver.sentiment'):
        (
            valid.limit(0)
            .write.format('delta')
            .saveAsTable('silver.sentiment')
        )
    w = Window.partitionBy("url").orderBy(F.col("_ingestion_at").desc())
    valid_dedup = valid.withColumn('_rn', F.row_number().over(w)).filter(F.col('_rn') == 1).drop('_rn')

    valid_dedup.createOrReplaceTempView('updates')
    spark_session.sql("""
        MERGE INTO silver.sentiment AS TARGET
        USING updates AS SOURCE
        ON TARGET.url = SOURCE.url
        WHEN NOT MATCHED THEN INSERT *
    """)

try:
    query = (
        df_description_null_handling.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", "Files/checkpoint/sentiment/bronze_to_silver")
        .trigger(availableNow=True)
        .start()
    )
    query.awaitTermination()
except Exception as e:
    import traceback
    traceback.print_exc()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_sent = (
    spark.read.table("silver.sentiment")
    .withColumnRenamed("symbol", "base_asset")
    .withColumnRenamed("published_date", "published_ts")
    .withColumn("date", F.col("published_ts").cast("date"))
    .select("base_asset", "title", "description", "url", "source", "published_ts", "date",
            "entity_match_score", "entity_sentiment_score")
)
df_sent.write.format("delta").mode("overwrite").saveAsTable("gold.sentiment_articles")

df_sent_daily = (
    df_sent.groupBy("base_asset", "date")
    .agg(F.avg("entity_sentiment_score").alias("avg_sentiment"),
         F.count("*").alias("sentiment_article_count"))
)
df_sent_daily.write.format("delta").mode("overwrite").saveAsTable("gold.sentiment_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

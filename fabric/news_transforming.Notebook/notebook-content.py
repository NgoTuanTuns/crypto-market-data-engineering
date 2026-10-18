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
from pyspark.sql.types import ArrayType
from pyspark.sql import Window
df = spark.readStream.table('bronze.news')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_critical_null = df.withColumn(
    "_is_critical_null",
    F.col("url").isNull() | F.col("headline").isNull() | F.col("datetime").isNull() | F.col("id").isNull()
)

df_matched_symbol_null_handling = df_critical_null.withColumn('matched_symbols',
    F.when(F.size(F.col('matched_symbols')) == 0, F.lit(['Market'])).otherwise(F.col('matched_symbols'))
)

df_summary_null_handling = df_matched_symbol_null_handling.withColumn('summary', 
    F.when((F.col('summary') == "") | (F.col('summary').isNull()), F.col('headline')).otherwise(F.col('summary')))

df_invalid_value_handling = df_summary_null_handling.withColumn("published_ts", F.from_unixtime(F.col("datetime")).cast("timestamp"))


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

    invalid.write.format('delta').mode('append').saveAsTable('bronze.news_quarantine')

    if not spark_session.catalog.tableExists('silver.news'):
        (
            valid.limit(0)
            .write.format('delta')
            .saveAsTable('silver.news')
        )
    w = Window.partitionBy("id", "url").orderBy(F.col("_ingestion_at").desc())
    valid_dedup = valid.withColumn('_rn', F.row_number().over(w)).filter(F.col('_rn') == 1).drop('_rn')

    valid_dedup.createOrReplaceTempView('updates')
    spark_session.sql("""
        MERGE INTO silver.news AS TARGET
        USING updates AS SOURCE
        ON TARGET.id = SOURCE.id
        AND TARGET.url = SOURCE.url
        WHEN NOT MATCHED THEN INSERT *
    """)

try:
    query = (
        df_invalid_value_handling.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", "Files/checkpoint/news/bronze_to_silver")
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

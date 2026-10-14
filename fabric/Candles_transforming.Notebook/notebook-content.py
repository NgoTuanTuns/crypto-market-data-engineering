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
df_bronze = spark.readStream.table('bronze.candles')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_cleaned = (
    df_bronze.withColumn('_is_critical_null',
        F.col('close').isNull() | F.col('high').isNull() |
        F.col('low').isNull() | F.col('open').isNull() |
        F.col("symbol").isNull() | F.col("minute_ms").isNull())
        .withColumn("volume", F.when(F.col("volume").isNull(), 0.0).otherwise(F.col("volume")))
        .withColumn("trades", F.when(F.col("trades").isNull(), 0).otherwise(F.col("trades")))
)

df_typed = (df_cleaned
    .withColumn("minute_ts", (F.col("minute_ms") / 1000).cast("timestamp"))
    .withColumn("_is_valid_ohlc", 
        (F.col('low') <= F.col('open')) & (F.col('low') <= F.col('close')) &
        (F.col('high') >= F.col('open')) & (F.col('high') >= F.col('close')) &
        (F.col('high') >= F.col('low')))
)

df_typed = df_typed.withColumn("date", F.col("minute_ts").cast("date"))


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_to_silver(batch_df, batch_id):
    spark_session = batch_df.sparkSession
    invalid = batch_df.filter(F.col("_is_critical_null")).drop("_is_critical_null")
    valid = batch_df.filter(~F.col("_is_critical_null")).drop("_is_critical_null")

    if not spark_session.catalog.tableExists("silver.candles"):
        (
            valid.limit(0)
            .write.format("delta")
            .partitionBy("date")
            .saveAsTable("silver.candles")
        )

    
    invalid.write.format("delta").mode("append").saveAsTable("bronze.candles_quarantine")

    w = Window.partitionBy("symbol", "minute_ms", "interval").orderBy(F.col("_ingestion_at").desc())
    df_dedup = (
        valid
        .withColumn("_rn", F.row_number().over(w))
        .filter(F.col("_rn") == 1)
        .drop("_rn")
    )

    df_dedup.createOrReplaceTempView("updates")
    spark_session.sql("""
        MERGE INTO silver.candles AS target
        USING updates AS source
        ON target.symbol = source.symbol
           AND target.minute_ms = source.minute_ms
           AND target.interval = source.interval
        WHEN NOT MATCHED THEN INSERT *
    """)


try:
    query = (
        df_typed.writeStream
        .foreachBatch(upsert_to_silver)
        .option("checkpointLocation", "Files/checkpoint/candles/bronze_to_silver")
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

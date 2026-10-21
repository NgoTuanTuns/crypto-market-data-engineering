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
df_silver = spark.readStream.table('silver.candles')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def upsert_to_gold(batch_df, batch_id):
    spark_session = batch_df.sparkSession
    if not spark_session.catalog.tableExists('gold.candles'):
        (
            batch_df.limit(0)
            .write.format('delta')
            .saveAsTable('gold.candles')
        )
    
    w = Window.partitionBy("symbol", "minute_ms", "interval").orderBy(F.col("_ingestion_at").desc())
    df_silver_dedup = (
        batch_df.withColumn('dedup', F.row_number().over(w))
        .filter(F.col('dedup') == 1)
        .drop(F.col('dedup'))
    )

    df_silver_dedup.createOrReplaceTempView('updates')

    spark_session.sql("""
        MERGE INTO gold.candles AS TARGET
        USING updates AS SOURCE
        ON TARGET.interval = SOURCE.interval
        AND TARGET.minute_ms = SOURCE.minute_ms
        AND TARGET.symbol = SOURCE.symbol
        WHEN NOT MATCHED THEN INSERT *
    """)

try:
    query = (
        df_silver.writeStream
        .foreachBatch(upsert_to_gold)
        .option("checkpointLocation", "Files/checkpoint/candles/silver_to_gold")
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

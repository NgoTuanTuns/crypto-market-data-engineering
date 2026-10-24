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
df_silver = spark.read.table('silver.candles')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w5 = Window.partitionBy("symbol").orderBy("minute_ts").rowsBetween(-4, 0)
w15 = Window.partitionBy("symbol").orderBy("minute_ts").rowsBetween(-14, 0)
w_lag = Window.partitionBy("symbol").orderBy("minute_ts")

df_features = (
    df_silver
    .filter(F.col("_is_valid_ohlc"))         
    .withColumn("return", (F.col("close") - F.col("open")) / F.col("open"))
    .withColumn("prev_close", F.lag("close", 1).over(w_lag))
    .withColumn("return_vs_prev", (F.col("close") - F.col("prev_close")) / F.col("prev_close"))
    .withColumn("ma5", F.avg("close").over(w5))
    .withColumn("ma15", F.avg("close").over(w15))
    .withColumn("volatility5", F.stddev("return").over(w5))
    .withColumn("typical_price", (F.col("open") + F.col("high") + F.col("low") + F.col("close")) / 4)  # thay VWAP tạm thời
)

(
    df_features.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("date")
    .saveAsTable("gold.ohlcv_1m_features")
)
spark.sql("OPTIMIZE gold.ohlcv_1m_features ZORDER BY (symbol, minute_ts)")
df_features_snapshot = df_features.withColumn("_computed_at", F.current_timestamp())

(
    df_features_snapshot.write
    .format("delta")
    .mode("append")                     
    .partitionBy("date")
    .saveAsTable("gold.ohlcv_1m_features_history")
)


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

w_day_asc = Window.partitionBy("symbol", "date").orderBy("minute_ts")
w_day_desc = Window.partitionBy("symbol", "date").orderBy(F.col("minute_ts").desc())

df_with_rank = (
    df_silver
    .filter(F.col("_is_valid_ohlc"))
    .withColumn("rn_first", F.row_number().over(w_day_asc))
    .withColumn("rn_last", F.row_number().over(w_day_desc))
)

df_open = df_with_rank.filter(F.col("rn_first") == 1).select("symbol", "date", F.col("open").alias("day_open"))
df_close = df_with_rank.filter(F.col("rn_last") == 1).select("symbol", "date", F.col("close").alias("day_close"))

df_agg = (
    df_silver.filter(F.col("_is_valid_ohlc"))
    .groupBy("symbol", "date")
    .agg(
        F.max("high").alias("day_high"),
        F.min("low").alias("day_low"),
        F.sum("volume").alias("day_volume"),
        F.sum("trades").alias("day_trades"),
    )
)

df_ohlcv_daily = (
    df_agg
    .join(df_open, ["symbol", "date"])
    .join(df_close, ["symbol", "date"])
    .withColumn("base_asset", F.regexp_extract(F.split("symbol", ":").getItem(1), r"^(.*)USDT$", 1))
)

df_ohlcv_daily.write.format("delta").mode("overwrite").saveAsTable("gold.ohlcv_daily")
spark.sql("OPTIMIZE gold.ohlcv_daily ZORDER BY (symbol, date)")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

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

assets = [
    ("BTC", "Bitcoin", "BINANCE:BTCUSDT", "CC:BTC"),
    ("ETH", "Ethereum", "BINANCE:ETHUSDT", "CC:ETH"),
    ("BNB", "BNB", "BINANCE:BNBUSDT", "CC:BNB"),
    ("AVAX", "Avalanche", "BINANCE:AVAXUSDT", "CC:AVAX"),
    ("LTC", "Litecoin", "BINANCE:LTCUSDT", "CC:LTC"),
    ("MARKET", "Toàn thị trường crypto", None, None),
]
(spark.createDataFrame(assets, "base_asset string, asset_name string, finnhub_symbol string, marketaux_symbol string")
    .write.format("delta").mode("overwrite").saveAsTable("gold.dim_asset"))

df_date = (
    spark.sql("SELECT explode(sequence(to_date('2026-10-01'), to_date('2027-12-31'), interval 1 day)) AS date")
    .withColumn("year", F.year("date"))
    .withColumn("month", F.month("date"))
    .withColumn("day", F.dayofmonth("date"))
    .withColumn("day_of_week", F.date_format("date", "E"))
    .withColumn("week_of_year", F.weekofyear("date"))
)
df_date.write.format("delta").mode("overwrite").saveAsTable("gold.dim_date")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_daily = spark.read.table("gold.ohlcv_daily")
df_news_daily = (
    spark.read.table("gold.news_by_symbol")
    .groupBy("base_asset", "date").agg(F.countDistinct("id").alias("news_count"))
)
df_sent_daily = spark.read.table("gold.sentiment_daily")

df_market_daily = (
    df_daily
    .join(df_sent_daily, ["base_asset", "date"], "left")
    .join(df_news_daily, ["base_asset", "date"], "left")
    .withColumn("news_count", F.coalesce("news_count", F.lit(0)))
    .withColumn("daily_return", (F.col("day_close") - F.col("day_open")) / F.col("day_open"))
)
df_market_daily.write.format("delta").mode("overwrite").saveAsTable("gold.market_daily")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

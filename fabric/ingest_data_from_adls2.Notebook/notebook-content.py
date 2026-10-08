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

from pyspark.sql.types import *

candles_schema = StructType([StructField('close', DoubleType(), True), 
                    StructField('high', DoubleType(), True), 
                    StructField('interval', StringType(), True), 
                    StructField('low', DoubleType(), True), 
                    StructField('minute_ms', LongType(), True), 
                    StructField('open', DoubleType(), True), 
                    StructField('source', StringType(), True), 
                    StructField('symbol', StringType(), True), 
                    StructField('trades', LongType(), True), 
                    StructField('volume', DoubleType(), True)])

checkpoint_path = 'Files/checkpoint'


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df_candles = (
    spark.readStream
    .schema(candles_schema)
    .option("maxFilesPerTrigger", 1)
    .json("Files/source/candles")
)

candles_stream = df_candles.writeStream\
                    .format('delta')\
                    .outputMode('append')\
                    .option('checkpointLocation', checkpoint_path)\
                    .start('Tables/bronze/candles')
                    

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

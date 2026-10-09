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

from pyspark.sql.types import StructType, StructField, StringType, LongType, ArrayType
from pyspark.sql.functions import current_timestamp, input_file_name, regexp_extract

schema = StructType([StructField('category', StringType(), True), 
            StructField('datetime', LongType(), True), 
            StructField('headline', StringType(), True), 
            StructField('id', LongType(), True), 
            StructField('image', StringType(), True), 
            StructField('matched_symbols', ArrayType(StringType(), True), True), 
            StructField('related', StringType(), True), 
            StructField('source', StringType(), True), 
            StructField('summary', StringType(), True), 
            StructField('url', StringType(), True)])

SOURCE_PATH = 'Files/source/news'
CHECKPOINT_PATH = 'Files/checkpoint/news/source_to_bronze'

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = (
        spark.readStream
            .schema(schema)
            .option("maxFilesPerTrigger", 1)
            .json(SOURCE_PATH)
)

df = df.withColumn('_ingestion_at', current_timestamp())\
        .withColumn('_from_source', regexp_extract(input_file_name(), r"(Files/.*)", 1))

query = (
        df.writeStream
            .format('delta')
            .outputMode('append')
            .option('checkpointLocation', CHECKPOINT_PATH)
            .trigger(availableNow=True)
            .start('Tables/bronze/news')
)

query.awaitTermination()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

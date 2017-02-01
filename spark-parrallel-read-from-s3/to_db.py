from input_output import DbWriter
df = spark.read.parquet('dates_by_seconds')
DbWriter.upsert(df, 'TheTable')  

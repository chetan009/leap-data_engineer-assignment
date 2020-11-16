import os, sys
from pyspark.sql import SparkSession, functions as f
import datetime


APPName = "Energy Meter Data"

if len(sys.argv) < 2:
    print(f""" Usage: {sys.argv[0]}  <PATH_TO_Meter_data>
    Example :-  {sys.argv[0]} meter_data""")  
    sys.exit(1)

InputDirPath = sys.argv[1]

# Create Spark session
spark = SparkSession.builder \
    .appName(APPName) \
    .getOrCreate()

# Logging
spark.sparkContext.setLogLevel("WARN")

df = spark.read.format('csv') \
                .option('header',True) \
                .load(InputDirPath)
                
# adding new column as timestamp for interval_date_time
df = df.withColumn('timestamp', f.col('interval_date_time').cast('timestamp'))

#get minimum timestamp and maximum
mi,ma = df.select(f.min('timestamp').cast('long'), f.max('timestamp').cast('long')).first()   
# 15 min interval
st = 15 * 60

meter_ids = df.select('meter_id').distinct()

ref = spark.range((mi / st)*st, (ma/st + 1) * st, st).select( \
                     f.col('id').cast('timestamp').alias('timestamp_ref'))

# Cross join with all meter_ids
ref_mids = ref.crossJoin(meter_ids).withColumnRenamed('meter_id', 'meter_id_ref')

join_res = ref_mids.join(df, 
                        ((ref_mids.timestamp_ref == df.timestamp) &\
                        (ref_mids.meter_id_ref == df.meter_id)), 
                        'left')\
                   .drop('meter_id').drop('timestamp')\
                   .withColumn("energy_wh", f.coalesce('energy_wh', f.lit(0.0)))

# adding flag 0 if value is 0.0 else 1
df_flag = join_res.withColumn('flag', f.when(f.col('energy_wh') == 0.0, 0).otherwise(1))

#calculate hourly average energy 
hr_res =  df_flag.select('meter_id_ref', 
                         f.date_format('timestamp_ref', "yyyy mm dd hh").alias('hr_date'), 
                         'energy_wh')\
                 .orderBy('meter_id_ref', 'hr_date')\
                 .groupBy('meter_id_ref', 'hr_date')\
                 .agg(f.mean('energy_wh').alias('avg_hr_energy'))\
                 .withColumnRenamed('meter_id_ref', 'meter_id')

# result 
result = df_flag.join(hr_res, 
                     (df_flag.meter_id_ref == hr_res.meter_id) & \
                     (f.date_format('timestamp_ref', "yyyy mm dd hh")== hr_res.hr_date),
                     'inner').drop(hr_res.meter_id)
 
result.select('meter_id_ref',
              'timestamp_ref',
              'energy_wh',
              'flag',
              'avg_hr_energy')\
      .withColumnRenamed('meter_id_ref', 'meter_id')\
      .withColumnRenamed('timestamp_ref', 'timestamp')\
      .orderBy('meter_id', 'timestamp').show(1000, truncate=False)

sys.exit(0)

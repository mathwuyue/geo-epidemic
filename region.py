from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import geohash

data_path = 'hdfs://192.168.2.3:9000/shanghai.parquet'
spark = SparkSession \
    .builder \
    .appName("Check ppl in each date") \
    .getOrCreate()

def read_data():
    df = spark.sql("SELECT imei_id, lgt, ltt, ts FROM parquet.`hdfs://192.168.2.3:9000/shanghai.parquet` where date='2019-07-12'")
    return df


def group_by_hour(df):
    df = df.withColumn('hour', date_trunc('hour', df['ts']))
    return df


 df.select(countDistinct('imei_id')).show()


df = df.withColumn('hour', date_trunc('hour', df['ts']))

df_region = spark.read.options(delimiter='\t', header='True').csv('region.csv')

def region_geohash(ltt, lgt, precision=5):
    return geohash.encode(float(ltt), float(lgt), precision)

geohashUDF = udf(lambda x,y: region_geohash(x, y, 5))
geohashUDF6 = udf(lambda x,y: region_geohash(x, y, 6))
geohashUDF7 = udf(lambda x,y: region_geohash(x, y, 7))
geohashUDF8 = udf(lambda x,y: region_geohash(x, y, 8))

def cal_region_geohash():
    df_region.withColumn('geohash5', geohashUDF(df_region['latitude'],df_region['longitude']))
    df_region.withColumn('geohash6', geohashUDF6(df_region['latitude'],df_region['longitude']))
    df_region.withColumn('geohash7', geohashUDF7(df_region['latitude'],df_region['longitude']))
    df_region.withColumn('geohash8', geohashUDF8(df_region['latitude'],df_region['longitude']))

df_region = df_region.withColumn('abc',lit("ABC"))
w = Window().partitionBy('abc').orderBy('geohash7')
df_region = df_region.withColumn("agent_id", dense_rank().over(w)).drop('abc')
df_region_geohash = df_region.select('agent_id', 'geohash5', 'geohash6', 'geohash7', 'geohash8')

df_region_geohash.write.option('header', True).format("parquet").save('region_geohash.parquet')
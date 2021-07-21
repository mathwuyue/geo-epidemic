from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql import Row
# import geohash


spark = SparkSession \
    .builder \
    .appName("Check ppl in region per hour") \
    .getOrCreate()

def read_data():
    df_ppl = spark.sql("SELECT imei_id, ts, geohash5, geohash6, geohash7, date FROM parquet.`shanghai_region_geohash.parquet`")
    # df_ppl = df_ppl.withColumn('hour', date_trunc('hour', df_ppl['ts']))
    df_region = spark.read.parquet('region_geohash3.parquet')
    return df_ppl, df_region

def cal_ppl_region(df_ppl):
    # region 7 geohash
    # df_ppl_region = df_ppl.join(broadcast(df_region_7), df_ppl.geohash7 == df_region_7.geohash7, 'left')
    # df_ppl_region_7 = df_ppl_region.filter(df_ppl_region.agent_id.isNotNull()).select('imei_id', 'agent_id', 'date', 'ts')
    # df_ppl_null_region = df_ppl_region.filter(df_ppl_region.agent_id.isNull())
    # region 6 geohash
    df_ppl_region = df_ppl.join(broadcast(df_region_6), df_ppl.geohash6 == df_region_6.geohash6, 'left')
    df_ppl_region_6 = df_ppl_region.filter(df_ppl_region.agent_id.isNotNull()).select('imei_id', 'agent_id', 'date', 'ts')
    df_ppl_null_region = df_ppl_region.filter(df_ppl_region.agent_id.isNull())
    # df_ppl_region = df_ppl.join(broadcast(df_region_6), df_ppl.)
    # df_ppl_left = df_ppl_null_region.select('imei_id', 'geohash5', 'geohash6', 'date', 'ts')
    # df_ppl_region = df_ppl_left.join(broadcast(df_region_6), df_ppl_left.geohash6 == df_region_6.geohash6, 'left')
    # df_ppl_region_6 = df_ppl_region.filter(df_ppl_region.agent_id.isNotNull()).select('imei_id', 'agent_id', 'date', 'ts')
    # df_ppl_null_region = df_ppl_region.filter(df_ppl_region.agent_id.isNull())
    # region 5 geohash
    df_ppl_left = df_ppl_null_region.select('imei_id', 'geohash5', 'date', 'ts')
    df_ppl_region = df_ppl_left.join(broadcast(df_region_5), df_ppl_left['geohash5'] == df_region_5['geohash5'], 'left')
    df_ppl_region_5 = df_ppl_region.filter(df_ppl_region.agent_id.isNotNull()).select('imei_id', 'agent_id', 'date', 'ts')
    df_ppl_null_region = df_ppl_region.filter(df_ppl_region.agent_id.isNull())
    return df_ppl_region_6.union(df_ppl_region_5), df_ppl_null_region

# def region_geohash(ltt, lgt, precision=5):
#     return geohash.encode(float(ltt), float(lgt), precision)


# geohashUDF5 = udf(lambda x,y: region_geohash(x, y, 5))
# geohashUDF6 = udf(lambda x,y: region_geohash(x, y, 6))
# geohashUDF7 = udf(lambda x,y: region_geohash(x, y, 7))


# def cal_shanghai_geohash(df_region):
#     df_region = df_region.withColumn('geohash5', geohashUDF5(df_region['ltt'], df_region['lgt']))
#     df_region = df_region.withColumn('geohash6', geohashUDF6(df_region['ltt'], df_region['lgt']))
#     df_region = df_region.withColumn('geohash7', geohashUDF7(df_region['ltt'], df_region['lgt']))
#     return df_region
    

# def cal_region_adj(df_region):
#     df_region_list = df_region.select('agent_id', 'geohash5', 'geohash6', 'geohash7').collect()
#     adj_df_list = []
#     for row in df_region_list:
#         agent_id_list = [row['agent_id'] for i in range(9)]
#         geohash5_list = geohash.expand(row['geohash5'])
#         geohash6_list = geohash.expand(row['geohash6'])
#         geohash7_list = geohash.expand(row['geohash7'])
#         for r in zip(agent_id_list, geohash5_list, geohash6_list, geohash7_list):
#             adj_df_list.append(r)
#     return spark.createDataFrame(data=adj_df_list, schema=['agent_id', 'geohash5', 'geohash6', 'geohash7'])





df_ppl, df_adj_region = read_data()
df_region_7 = df_adj_region.select('agent_id', 'geohash7').dropDuplicates(['geohash7'])
df_region_6 = df_adj_region.select('agent_id', 'geohash6').dropDuplicates(['geohash6'])
df_region_5 = df_adj_region.select('agent_id', 'geohash5').dropDuplicates(['geohash5'])

# df_ppl = cal_shanghai_geohash(df_ppl)
# df_ppl.write.option('header', True).partitionBy('date').mode('overwrite').format('parquet').save('shanghai_region_geohash.parquet')   
# df_adj_region = cal_region_adj(df_region)
df_ppl_region_id, df_ppl_region_null = cal_ppl_region(df_ppl)
_tempudf = udf(lambda x: '0')
df_ppl_region_null = df_ppl_region_null.withColumn('agent_id', _tempudf(df_ppl_region_null['agent_id'])).select('imei_id', 'agent_id', 'date', 'ts')
df_ppl_region_id = df_ppl_region_id.union(df_ppl_region_null)
df_ppl_region_id.write.option('header', True).partitionBy('date').mode('overwrite').format('parquet').save('shanghai_region_id2.parquet')


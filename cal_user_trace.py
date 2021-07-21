from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import numpy as np


spark = SparkSession \
    .builder \
    .appName("Check ppl in region per hour") \
    .getOrCreate()

dt_list = pd.date_range(start="2019-07-01",end="2019-08-01", freq='1h').to_pydatetime().tolist()[0:-1]

def read_data():
    df_shanghai = spark.sql("SELECT * FROM parquet.`shanghai_region_id2.parquet`")
    df_shanghai = df_shanghai.withColumn('hour', date_trunc('hour', df_shanghai['ts']))
    # df_region = spark.read.parquet('region_adj_geohash.parquet').dropDuplicates(['geohash6']).select('agent_id', 'geohash6')
    return df_shanghai


def cal_ppl_region_hour(df_shanghai):
    # ppl_list = df_shanghai.select('imei_id').distinct()
    agent_list = df_shanghai.select('agent_id').distinct().toPandas()
    for dt in dt_list:
        h = dt.strftime('%Y-%m-%d %H:%M:%S')
        print('calculate %s...' % h)
        df_dt_shanghai = df_shanghai.filter(df_shanghai['hour'] == h)
        # calculate the number of ppl in each region
        _df1 = df_dt_shanghai.groupBy('agent_id').agg(countDistinct('imei_id'))
        _df1 = _df1.select(col('agent_id').alias('h_id'), col('count(imei_id)').alias(h)).toPandas()
        # agent_list = agent_list.join(broadcast(_df1), agent_list['agent_id'] == _df1['h_id'], 'left').drop('h_id')
        agent_list = pd.merge(left=agent_list, right=_df1, left_on='agent_id', right_on='h_id', how='left')
        # calculate the region of each ppl
        # imei_id_list = imei_id_list.join(broadcast(df_dt_shanghai), imei_id_list['agent_id'] == df_dt_shanghai['agent_id'], 'left')
        # df_dt_shanghai = df_dt_shanghai.join(broadcast(df_dt_shanghai), )
    # region count set all null to -1
    agent_list = agent_list.fillna(-1)
    # save to file
    # agent_list.write.option('header', True).format('parquet').save('region_ppl_count_h.parquet')
    agent_list.to_csv('region_ppl_count_h.csv')
    return agent_list


def cal_ppl_start_region(df_shanghai):
    st_list = ['2019-07-'+str(i)+' 19:00:00' for i in range(2, 31)]
    et_list = ['2019-07-'+str(i)+' 07:00:00' for i in range(3, 32)]
    mt_list = ['2019-07-'+str(i)+' 01:00:00' for i in range(3, 32)]
    ppl_list = df_shanghai.select('imei_id').distinct()
    w = Window().partitionBy('i_id').orderBy('hour')
    for st, et, mt in zip(st_list, et_list, mt_list):
        times = df_shanghai.filter((df_shanghai['hour']>=st) & (df_shanghai['hour']<=et)).select(col('imei_id').alias('i_id'), 'agent_id', 'hour')
        df_times = times.withColumn('row', row_number().over(w)) \
                .withColumn('max_time', max(col('hour')).over(w)) \
                .filter((col('row')==1) & (col('max_time') <= mt)) \
                .select('i_id', 'agent_id')
        ppl_list = ppl_list.join(df_times, ppl_list['imei_id'] == df_times['i_id'], 'left').drop('i_id')
    _new_data = []
    for row in ppl_list.collect():
        agent_id = _count_max_agent_id(row)
        _new_data.append([row[0], agent_id])
    ppl_list = spark.createDataFrame(data=_new_data, schema=['imei_id', 'agent_id'])
    return ppl_list


def _count_max_agent_id(row):
    _dict = {}
    _idx = 0
    agent_id = ''
    for i in range(1, 30):
        if row[i]:
            _i = _dict.get(row[i], 0)
            _dict[row[i]] = _i+1
            if _i+1 > _idx:
                _idx = _i + 1
                agent_id = row[i]
    return agent_id

max_count_agent_udf = udf(lambda x: _count_max_agent_id(x))



df_shanghai = read_data()
# agent_list = cal_ppl_region_hour(df_shanghai)
ppl_list = cal_ppl_start_region(df_shanghai)


_new_data = []
for row in ppl_list.collect():
    agent_id = _count_max_agent_id(row)
    _new_data.append([row[0], agent_id])
ppl_start_region = spark.createDataFrame(data=_new_data, schema=['imei_id', 'agent_id'])


st_list = ['2019-07-'+str(i)+' 19:00:00' for i in range(1, 31)]
et_list = ['2019-07-'+str(i)+' 07:00:00' for i in range(2, 32)]
mt_list = ['2019-07-'+str(i)+' 01:00:00' for i in range(3, 32)]

for st, et, mt in zip(st_list, et_list, mt_list):
    times = df_shanghai.filter((df_shanghai['hour']>=st) & (df_shanghai['hour']<=et)).select(col('imei_id').alias('i_id'), 'agent_id', 'hour')
    df_times = times.withColumn('row', row_number().over(w)) \
            .withColumn('max_time', max(col('hour')).over(w)) \
            .filter((col('row')==1) & (col('max_time') <= mt)) \
            .select('i_id', 'agent_id')
    ppl_list = ppl_list.join(df_times, ppl_list['imei_id'] == df_times['i_id'], 'left').drop('i_id')

# caclulates time between 19:00 - 7:00, if it is
dates = pd.date_range(start="2019-07-01",end="2019-08-01").to_pydatetime().tolist()[0:-1]
dfs = []
for d in dates:
    _df = spark.sql("SELECT * FROM parquet.`shanghai_region_id2.parquet` where date='%s'" % d)
    dfs.append(_df)

def _count_only_one_ts(ppl_list, df_shanghai):
    for st, et in zip(st_list, et_list):
        _df = df_shanghai.filter((col('hour') >= st) & (col('hour') <= et)).select(col('imei_id').alias('i_id'), 'agent_id', 'hour')
        _df_1 = _df.groupBy('i_id').agg(first('agent_id').alias('agent_id'+st), countDistinct('agent_id').alias('num_agent_ids')).filter(col('num_agent_ids')==1)
        ppl_list = ppl_list.join(_df_1, ppl_list['imei_id'] == _df_1['i_id'], 'left').drop('num_agent_ids').drop('i_id')
    return ppl_list

_df = _count_only_one_ts(df_ppl_next, df_shanghai)



df_shanghai_next = df_shanghai.join(df_ppl_next, df_shanghai['imei_id']==df_ppl_next['i_id']) \
                                .select('imei_id','agent_id','hour',hour(col('hour')).alias('n_hour')) \
                                .filter((col('n_hour') >= 19) | (col('n_hour') <= 8)) \
                                .cache()
num_total_ppl = 24281400
num_sample_ppl = 3357267
times = num_total_ppl / num_sample_ppl
def start_region_count(row):
    return (row['agent_id'], int(np.round(float(row['count']) * times)))

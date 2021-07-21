from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.window import Window
import pandas as pd
import numpy as np
from scipy.sparse import csr_matrix
from numpy.matlib import repmat
from datetime import timedelta
import pickle

spark = SparkSession \
    .builder \
    .appName("Check ppl in region per hour") \
    .getOrCreate()


dt_list = pd.date_range(start="2019-07-01",end="2019-08-01", freq='1h').to_pydatetime().tolist()[0:-1]


def read_data():
    df_1 = spark.sql("SELECT * FROM parquet.`shanghai_region_id2.parquet` where date=='2019-07-01'")
    df_2 = spark.sql("SELECT * FROM parquet.`shanghai_region_id2.parquet` where date=='2019-07-09'")
    df_shanghai = spark.sql("SELECT * FROM parquet.`shanghai_region_id2.parquet` where date>='2019-07-02' and date<='2019-07-08'")
    df_shanghai = df_shanghai.withColumn('hour', date_trunc('hour', df_shanghai['ts']))
    df_1 = df_1.withColumn('hour', date_trunc('hour', df_1['ts']))
    df_2 = df_2.withColumn('hour', date_trunc('hour', df_2['ts']))
    return df_1, df_2, df_shanghai


def cal_full_records(df1, df2, df_shanghai):
    # add df1 to df_shanghai
    w = Window().partitionBy('imei_id').orderBy(col('ts').desc())
    _df = df1.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    df_shanghai = df_shanghai.union(_df)
    # add df2 to df_shanghai
    w = Window().partitionBy('imei_id').orderBy(col('ts'))
    _df = df2.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    df_shanghai = df_shanghai.union(_df)
    # remove any duplicate region records
    df_shanghai = df_shanghai.dropDuplicates(['imei_id', 'agent_id', 'hour'])
    # check time diff
    w = Window().partitionBy('imei_id').orderBy('ts')
    df_shanghai = df_shanghai.withColumn('pre_hour', lag('hour', 1).over(w)).withColumn('pre_agent_id', lag('agent_id', 1).over(w)).withColumn('next_agent_id', lead('agent_id', 1).over(w))
    # fill pre_hour na as 2019-07-01 00:00:00
    df_shanghai = df_shanghai.fillna({'pre_hour': '2019-07-01 00:00:00'})
    # cal hour diff
    df_shanghai = df_shanghai.withColumn('hour', to_timestamp(col('hour'))).withColumn('pre_hour', to_timestamp(col('pre_hour'))) \
                            .withColumn('hour_diff', round((unix_timestamp('hour') - unix_timestamp('pre_hour')) / 3600))
    # find hour diff within 24
    _df = df_shanghai.filter((col('hour_diff') > 1) & (col('hour_diff') <= 24))
    # add records
    records_rdd = _df.rdd.flatMap(lambda x: _add_records(x))
    # return ResultIteratable
    return records_rdd
    # return df_shanghai

def cal_full_records_max(df1, df2, df_shanghai):
    # add df1 to df_shanghai
    w = Window().partitionBy('imei_id').orderBy(col('ts').desc())
    _df = df1.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    # get max ts as the position of a user within that hour
    w = Window().partitionBy([col('imei_id'), col('hour')]).orderBy(col('ts').desc())
    _df2 = df_shanghai.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    df_shanghai = _df2.union(_df)
    # add df2 to df_shanghai
    w = Window().partitionBy('imei_id').orderBy(col('ts'))
    _df = df2.withColumn('rn', row_number().over(w)).filter(col('rn')==1).drop('rn')
    df_shanghai = df_shanghai.union(_df)
    # check time diff
    w = Window().partitionBy('imei_id').orderBy('ts')
    df_shanghai = df_shanghai.withColumn('pre_hour', lag('hour', 1).over(w)).withColumn('pre_agent_id', lag('agent_id', 1).over(w)).withColumn('next_agent_id', lead('agent_id', 1).over(w))
    # fill pre_hour na as 2019-07-01 00:00:00
    df_shanghai = df_shanghai.fillna({'pre_hour': '2019-07-01 00:00:00'})
    # cal hour diff
    df_shanghai = df_shanghai.withColumn('hour', to_timestamp(col('hour'))).withColumn('pre_hour', to_timestamp(col('pre_hour'))) \
                            .withColumn('hour_diff', round((unix_timestamp('hour') - unix_timestamp('pre_hour')) / 3600))
    # find hour diff within 24
    # _df = df_shanghai.filter((col('hour_diff') > 1))
    # add records
    records_rdd = df_shanghai.rdd.flatMap(lambda x: _add_records(x))
    # return rdd
    return records_rdd


def get_more_24(df_shanghai):
    df_shanghai.filter(col('hour_diff') > 24).repartition(1).write.option('header', True).format('csv').save('more_24.csv')

def _map_row_region(row):
    return (row[1]+';'+row[2], 1)

def _map_mobility_matrix(row):
    pre_agent_id = row[4]
    return (pre_agent_id+';'+row[1]+';'+row[3], 1)

def _add_records(row):
    # no previous agent_id. first record in the dataset
    if not row['pre_agent_id'] or row['pre_agent_id']=='null':
        if not row['next_agent_id'] or row['next_agent_id']=='null':
            _item1 = (row['imei_id'], row['agent_id'], row['hour'].strftime('%Y-%m-%d %H:%M:%S'), row['pre_hour'].strftime('%Y-%m-%d %H:%M:%S'), '0')
            _item2 = (row['imei_id'], '0', (row['hour']+timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), row['hour'].strftime('%Y-%m-%d %H:%M:%S'), row['agent_id'])
            return [_item1, _item2]
        else:
            return [(row['imei_id'], row['agent_id'], row['hour'].strftime('%Y-%m-%d %H:%M:%S'), row['pre_hour'].strftime('%Y-%m-%d %H:%M:%S'), '0')]
    _count = int(row['hour_diff'])
    if _count <= 1:
        _x = [(row['imei_id'], row['agent_id'], row['hour'].strftime('%Y-%m-%d %H:%M:%S'), row['pre_hour'].strftime('%Y-%m-%d %H:%M:%S'), row['pre_agent_id'])]
    elif _count > 1 and _count <= 24:
        dt_list = pd.date_range(start=row['pre_hour'],end=row['hour'], freq='1h').to_pydatetime().tolist()[0:-1]
        _list = [(row['imei_id'], row['pre_agent_id'], dt_list[i].strftime('%Y-%m-%d %H:%M:%S'), dt_list[i-1].strftime('%Y-%m-%d %H:%M:%S'), row['pre_agent_id']) for i in range(1, _count)]
        _x = _list + [(row['imei_id'], row['agent_id'], row['hour'].strftime('%Y-%m-%d %H:%M:%S'), dt_list[-1].strftime('%Y-%m-%d %H:%M:%S'), row['pre_agent_id'])]
    else:
        _item1 = (row['imei_id'], '0', (row['pre_hour']+timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), row['pre_hour'].strftime('%Y-%m-%d %H:%M:%S'), row['pre_agent_id'])
        _item2 = (row['imei_id'], row['agent_id'], row['hour'].strftime('%Y-%m-%d %H:%M:%S'), (row['hour']-timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), '0')
        _x = [_item1, _item2]
    if not row['next_agent_id'] or row['next_agent_id']=='null':
        _x = _x + [(row['imei_id'], '0', (row['hour']+timedelta(hours=1)).strftime('%Y-%m-%d %H:%M:%S'), row['hour'].strftime('%Y-%m-%d %H:%M:%S'), row['agent_id'])]
    return _x

def _map_region_hour_ppl(x):
    agent_id, hour = x[0].split(';')
    return (hour, (agent_id, x[1]))

def _map_region_hour_mobility(x):
    pre, cur, hour = x[0].split(';')
    return (hour, ((pre, cur), x[1]))

def _to_list(a):
    return [a]

def _append(a, b):
    a.append(b)
    return a

def _extend(a, b):
    a.extend(b)
    return a


df1, df2, df_shanghai = read_data()
# fill out all the records
rdd_shanghai = cal_full_records_max(df1, df2, df_shanghai)
# region rdd
region_rdd =rdd_shanghai.map(lambda a: _map_row_region(a)).reduceByKey(lambda a,b: a+b)
# cal region ppl number vectors
ppl_region_rdd = region_rdd.map(lambda a: _map_region_hour_ppl(a)).combineByKey(_to_list, _append, _extend)
ppl_region = ppl_region_rdd.collect()
# sort by ts
ppl_region.sort(key=lambda x: x[0])
ppl_region_dict = {}
for i in range(22, 191):
    _l = [0 for j in range(5081)]
    for p in ppl_region[i][1]:
        _l[int(p[0])] = int(p[1])
    ppl_region_dict[ppl_region[i][0]] = np.array(_l) 
# mobility rdd
mobility_matrix_rdd = rdd_shanghai.map(lambda a: _map_mobility_matrix(a)).reduceByKey(lambda a,b: a+b)
# mobility to ts
mobility_raw_matrix = mobility_matrix_rdd.map(lambda a: _map_region_hour_mobility(a)).combineByKey(_to_list, _append, _extend).collect()
# sort by ts
mobility_raw_matrix.sort(key=lambda x: x[0])
# calculate mobility matrix in number of ppl
mobility_dict = {}
for i in range(23, 191):
    # _l = [[0 for j in range(5081)] for k in range(5081)]
    rows = []
    cols = []
    data = []
    for p in mobility_raw_matrix[i][1]: 
        rows.append(int(p[0][1]))
        cols.append(int(p[0][0]))
        data.append(int(p[1]))
    mobility_dict[mobility_raw_matrix[i][0]] = csr_matrix((data, (rows, cols)), shape=(5081, 5081)) 

# cal mobility frac matrix
mobility_frac_dict = {}
for k in mobility_dict:
    _tmp = mobility_dict[k].toarray()
    _x = np.diagonal(_tmp)
    _y = _tmp[0, :]
    np.fill_diagonal(_tmp, _x+_y)
    mobility_frac_dict[k] = _tmp / repmat(ppl_region_dict[k], 5081, 1)
    mobility_frac_dict[k] = np.nan_to_num(mobility_frac_dict[k])
    mobility_frac_dict[k] = csr_matrix(mobility_frac_dict[k])

with open('ppl_region_max.pkl', 'wb') as wf:
    pickle.dump(ppl_region_dict, wf)

with open('mobility_frac_matrix_max.pkl', 'wb') as wf:
    pickle.dump(mobility_frac_dict, wf)
import pandas as pd
import numpy as np
from ftfy import fix_text

# Loads data

path = "/usr/local/spark/data/bks_data/"
cjxx = "bks_cjxx_out.csv"
kcsjxx = "bks_kcsjxx_out.csv"
pksjxx = "bks_pksjxx_out.csv"
xjjbsjxx = "bks_xjjbsjxx_out.csv"
xsjbsjxx = "bks_xsjbsjxx_out.csv"
ykt = "ykt_jyrz_lishi.csv"
def readfile(name):
    if name.endswith("csv"):
        df = pd.read_csv(path+name)
        return pd.DataFrame(df)
    elif name.endswith("txt"):
        return pd.read_table(path+name)
    else:
        print("The encoding is not utf-8!")


print("Start loads source data...")
ykt_df = readfile(ykt)
ykt_df_filter = ykt_df.filter()
cjxx_df_loc = readfile(cjxx).loc[:, ['xh', 'xn', 'kccj', 'cjbh', 'sfcx']]
ykt_df_loc = readfile(ykt).loc[:, ['xh', 'jyrq', 'shmc', 'jyje', 'ljykcs']]
df_merge = pd.merge(cjxx_df_loc, ykt_df_loc, how='inner', on='xh')
cjxx_and_ykt = pd.concat([cjxx_df_loc, ykt_df_loc], axis=1, join_axes=[cjxx_df_loc.index])
ykt_df_filter = ykt_df_loc[ykt_df_loc['xh'].isin(['201415178', '201840031'])]
for i in range [iter(ykt_df_filter['xh'].values.min()), iter(ykt_df_filter['xh'].values.max())]:


print("Success to loads data.")

def PrepareData(sc):
    # 1.Loads data
    print("Start loads source data...")
    rawData = sc.textFile("/user/hadoop/input/bks_data/bks_cjxx_out.csv")
    header = rawData.first()
    rawData_rmHeader = rawData.fliter(lambda x: x != header)
    lines = rawData_rmHeader.map(lambda l: l.split(","))
    print("Total rows is :" + str(lines.count()))
    # 2.Create RDD
    categoriesMap = lines.map(lambda f: f[3]).\
        distinct().zipWithIndex().collectAsMap()
    labelpointRDD = lines.map(lambda r: LabeledPoint(

    ))
    # 3.RandomSplit
    (trainData, cvData, testData) = labelpointRDD.randomSplit([8, 1, 1])
    print("trainData:" + str(trainData.count()) +
          "\ncvData:" + str(cvData.count()) +
          "\ntestData:" + str(testData.count()))
    return (trainData, cvData, testData, categoriesMap)

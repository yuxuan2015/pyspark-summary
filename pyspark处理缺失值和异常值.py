pyspark处理缺失值
例：
df_miss = spark.createDataFrame([ (1, 143.5, 5.6, 28,'M', 100000),
(2, 167.2, 5.4, 45, 'M', None),
(3, None , 5.2, None, None, None),
(4, 144.5, 5.9, 33, 'M', None),
(5, 133.2, 5.7, 54, 'F', None),
(6, 124.1, 5.2, None, 'F', None),
(7, 129.2, 5.3, 42, 'M', 76000),
], ['id', 'weight', 'height', 'age', 'gender', 'income'])

统计每行缺失值的个数
df_miss.rdd.map(lambda row: (row['id'], sum([c == None for c in row]))).collect()

统计每列的缺失比例
import pyspark.sql.functions as fn
df_miss.agg(*[(1 - (fn.count(c) / fn.count('*'))).alias(c + '_missing') for c in df_miss.columns]).show()

去掉列income
df_miss_no_income = df_miss.select([c for c in df_miss.columns if c != 'income'])

删除缺失值的个数大于等于3的行
df_miss_no_income.dropna(thresh=3).show()

普通的列用平均值填充，类别型的指定为missing
means = df_miss_no_income.agg(*[fn.mean(c).alias(c) for c in df_miss_no_income.columns if c != 'gender']).toPandas().to_dict('records')[0]
means['gender'] = 'missing'
df_miss_no_income.fillna(means).show()

处理异常值outliers
判断异常值的方法
1.四分位数法
Q1−1.5IQR and Q3+1.5IQR range，其中IQR = Q3 - Q1

例：
df_outliers = spark.createDataFrame([
(1, 143.5, 5.3, 28),
(2, 154.2, 5.5, 45),
(3, 342.3, 5.1, 99),
(4, 144.5, 5.5, 33),
(5, 133.2, 5.4, 54),
(6, 124.1, 5.1, 21),
(7, 129.2, 5.3, 42),
], ['id', 'weight', 'height', 'age'])

from pyspark.sql.dataframe.DataFrame import approxQuantile
注：approxQuantile(col, probabilities, relativeError)
cols = ['weight', 'height', 'age']
bounds = {}
for col in cols:
    quantiles = df_outliers.approxQuantile(col, [0.25, 0.75], 0.05)
    IQR = quantiles[1] - quantiles[0]
    bounds[col] = [quantiles[0] - 1.5 * IQR, quantiles[1] + 1.5 * IQR]

outliers = df_outliers.select(*['id'] + [
(
(df_outliers[c] < bounds[c][0]) |
(df_outliers[c] > bounds[c][1])
).alias(c + '_o') for c in cols
])
outliers.show()

df_outliers = df_outliers.join(outliers, on='id')
df_outliers.filter('weight_o').select('id', 'weight').show()
df_outliers.filter('age_o').select('id', 'age').show()
# -*- coding:utf-8 -*-

from pyspark.context import SparkContext
from pyspark.ml.linalg import Vectors
from pyspark.sql import Row
from pyspark.sql.session import SparkSession
from pyspark.ml.classification import NaiveBayes
from pyspark.ml.evaluation import MulticlassClassificationEvaluator

# 使用本地spark
sc = SparkContext('local', 'pyspark')
#建立spark会话
spark = SparkSession.builder\
    .master('local')\
    .appName('spark_mllib')\
    .config('spark.sql.warehouse.dir', 'file:///F:/workspace/work/project/spark/spark-warehouse')\
    .getOrCreate()
'''
# Load training data
path = r'C:\spark-2.0.1-bin-2.6.0-cdh5.4.7\data\mllib'
data = spark.read.format("libsvm") \
    .load(path+'\\'+'sample_libsvm_data.txt')
print type(data)
#<class 'pyspark.sql.dataframe.DataFrame'>
'''

# Load and parse the data
def parseRow(line):
    row = line.split('\t')
    return Row(labelpoint=row[-1],
               features=Vectors.dense([float(x) for x in row[:-1]]))

path = r'F:\workspace\work\project\MLlib\classification'
data = sc.textFile(path+'\\'+'classify_data.txt')
#转成classifier需要的数据格式
parsedData = data.map(parseRow)
datas = parsedData.toDF()

#StringIndexer将一列labels转译成[0,labels基数)的index
from pyspark.ml.feature import StringIndexer

labeled = StringIndexer(inputCol="labelpoint", outputCol="label")
dataset = labeled.fit(datas).transform(datas)

# Split the data into train and test
train, test = dataset.randomSplit([0.6, 0.4], seed=11)

print type(train)
# create the trainer and set its parameters
nb = NaiveBayes(smoothing=1.0, modelType="multinomial")

# train the model
model = nb.fit(train)

# select example rows to display.
predictions = model.transform(test)
predictions.show()

# compute accuracy on the test set
evaluator = MulticlassClassificationEvaluator(labelCol="label", predictionCol="prediction",
                                            metricName="accuracy")
accuracy = evaluator.evaluate(predictions)
print "Test set accuracy = " + str(accuracy)
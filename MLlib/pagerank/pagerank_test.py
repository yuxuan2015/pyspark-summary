# -*- coding:utf-8 -*-

import os

os.environ["PYSPARK_SUBMIT_ARGS"] = (
    "--packages graphframes:graphframes:0.2.0-spark2.0-s_2.11 pyspark-shell"
)

from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# 使用本地spark
sc = SparkContext('local', 'pyspark')

# 建立spark会话
spark = SparkSession.builder \
    .master('local') \
    .appName('spark_mllib') \
    .config('spark.sql.warehouse.dir', 'file:///F:/workspace/work/project/spark/spark-warehouse') \
    .getOrCreate()

# Create a Vertex DataFrame with unique ID column "id"
v = spark.createDataFrame([
    ("a", "Alice", 34),
    ("b", "Bob", 36),
    ("c", "Charlie", 30),
], ["id", "name", "age"])
v.show()

# Create an Edge DataFrame with "src" and "dst" columns
e = spark.createDataFrame([
    ("a", "b", "friend"),
    ("b", "c", "follow"),
    ("c", "b", "follow"),
], ["src", "dst", "relationship"])
e.show()
# Create a GraphFrame
from graphframes import *

g = GraphFrame(v, e)

# Query: Get in-degree of each vertex.
g.inDegrees.show()

# Query: Count the number of "follow" connections in the graph.
g.edges.filter("relationship = 'follow'").count()

# Run PageRank algorithm, and show results.
results = g.pageRank(resetProbability=0.05, maxIter=10)
results.vertices.select("id", "pagerank").show()
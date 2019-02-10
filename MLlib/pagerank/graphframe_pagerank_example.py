#load package graphframes to spark
pyspark --packages graphframes:graphframes:0.5.0-spark2.0-s_2.11

--packages                  Comma-separated list of maven coordinates of jars to include
                              on the driver and executor classpaths. Will search the local
                              maven repo, then maven central and any additional remote
                              repositories given by --repositories. The format for the
                              coordinates should be groupId:artifactId:version.
--exclude-packages          Comma-separated list of groupId:artifactId, to exclude while
                              resolving the dependencies provided in --packages to avoid
                              dependency conflicts.

							  
from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

# 使用本地spark
#sc.stop()
sc = SparkContext('local', 'pyspark')	

#建立spark会话
spark = SparkSession.builder\
    .master('local')\
    .appName('spark_mllib')\
    .config('spark.sql.warehouse.dir', 'file:///F:/workspace/work/project/spark/spark-warehouse')\
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
 id|          pagerank|
+---+------------------+
|  b| 0.626687039889458|
|  a|              0.05|
|  c|0.6169126832811621|
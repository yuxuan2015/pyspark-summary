from pyspark import SparkConf, SparkContext

conf = SparkConf().setAppName('textfile_dir')
sc = SparkContext(conf=conf)
sc = SparkContext(app=PySparkShell, master=local[*])
clusters = sc.textFile("hdfs://ns1/user/u_lx_data/private/lyy/cdr/circle/user_lyy_voice_circle_cluster/")

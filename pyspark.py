pyspark
启动jupyter
jupyter notebook

Jupyter Notebook远程服务器配置 
1.生成密码
from IPython.lib import passwd
passwd()
'sha1:612163fecab0:89bd16e79f9db12e4d52eb17013355dce9c0822a'
2.接下来生成密钥
openssl req -x509 -nodes -days 365 -newkey rsa:1024 -keyout mycert.pem -out mycert.pem
3.创建服务器配置
ipython profile create nbserver
cd /root/.ipython/profile_nbserver
vim ipython_config.py
在最下面输入：
c.NotebookApp.password = u'sha1:612163fecab0:89bd16e79f9db12e4d52eb17013355dce9c0822a'
c.NotebookApp.certfile = u'/root/mycert.pem' 
c.NotebookApp.ip = '192.168.8.88' 
c.NotebookApp.port = 9999
4.启动服务器
jupyter notebook --profile=nbserver

jupyter连接spark
1.将以下代码添加到~/.bash_profile（或者~/.bashrc）
export SPARK_HOME=~/spark-1.5.0-bin-hadoop2.6
export PATH=$SPARK_HOME/bin:$PATH
export PYTHONPATH=$SPARK_HOME/python/:$PYTHONPATH
export PYTHONPATH=$SPARK_HOME/python/lib/py4j-0.8.2.1-src.zip:$PYTHONPATH
2.启动pyspark-jupyter
IPYTHON_OPTS="notebook"$SPARK_HOME/bin/pyspark

from pyspark import  SparkContext
sc = SparkContext( 'local', 'pyspark')

from pyspark.sql import SQLContext
sqlContext = SQLContext(sc)
<pyspark.sql.SQLContext object at 0x7f82cb17f610>

df = sqlContext.read.json("examples/src/main/resources/people.json")
# Displays the content of the DataFrame to stdout
df.show()
# pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark
# Environment entry object(SparkContext' instantiation)
from pyspark import SparkConf,SparkContext
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
sc = SparkContext(conf=conf)
print(sc.version)
sc.stop()
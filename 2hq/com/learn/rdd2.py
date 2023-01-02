import json
import os
os.environ['PYSPARK_PYTHON']='E:/install winen/Python10/python.exe'
from pyspark import SparkConf,SparkContext
# get a Environment entry object(SparkContext' instantiation)
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
# set the rdd object's Partitions =1 for just generating one data file
conf.set('spark.default.parallelism',1)
sc = SparkContext(conf=conf)
# 一、rdd to python object
# 1.rdd to list
rdd1=sc.parallelize([1,2,3,4])
list1:list=rdd1.collect()
print(list1)
# 2.aggregate and return a value
rdd2=sc.parallelize([1,2,3,4])
result1=rdd2.reduce(lambda a,b:a+b)
print(result1)
# 3.Get the first n elements and return a list
rdd3=sc.parallelize([1,2,3,4])
result2:list=rdd2.take(3)
print(result2)
# 4.get numbers of element and return a int
rdd4=sc.parallelize([1,2,3,4])
print(rdd4.count())
# 二、rdd to textfile
rdd5=sc.parallelize([1,2,3,4])
rdd5.saveAsTextFile('E:\programme\Python\practice\outputRdd')

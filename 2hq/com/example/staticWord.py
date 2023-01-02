import os
os.environ['PYSPARK_PYTHON']='E:/install winen/Python10/python.exe'
from pyspark import SparkConf,SparkContext
# 一、get a Environment entry object(SparkContext' instantiation)
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
sc = SparkContext(conf=conf)
rdd=sc.textFile('E:\programme\Python\practice\words.txt')
# 2，unnested
print(rdd.collect())
word_rdd=rdd.flatMap(lambda x : x.split(' '))
# word_rdd to kv
kv_word_rdd=word_rdd.map(lambda word:(word,1))
result=kv_word_rdd.reduceByKey(lambda a,b:a+b)
print(result.collect())
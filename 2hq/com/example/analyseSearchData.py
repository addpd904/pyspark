import json
import os
# 一、format the file data(file data to dict)
os.environ['PYSPARK_PYTHON']='E:/install winen/Python10/python.exe'
from pyspark import SparkConf,SparkContext
# 一、get a Environment entry object(SparkContext' instantiation)
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
# set the rdd object's Partitions =1 for just generating one data file
conf.set('spark.default.parallelism',1)
sc = SparkContext(conf=conf)
file_rdd=sc.textFile('E:\programme\Python\practice\search_log.txt')
# 二、finish requirement
# a.get the hot time top 3
# translate to model count words
#1.map from str to time str.2.now every element is a time str.just regard every element(time str) as word and static their numbers .
# 3.sort
result=file_rdd.map(lambda ele:ele.split('\t'))
print(result.collect())
# result1=file_rdd.map(lambda ele:ele.split('\t')).\
#      map(lambda ele:ele[0]).map(lambda ele:ele[0:2]).map(lambda ele:(ele,1)).\
#     reduceByKey(lambda a,b:a+b).sortBy(lambda ele:ele[1],ascending=False,numPartitions=1).\
#     take(3)
# print(result1)

# b.get the hot keyword top 3
#1.map from str to keyword  str.2.now every element is a keyword str.just regard every element(time str) as word and static their numbers .
# static words :word to model kv((hello,1)).group and aggregate(sum up)
# 3.sort
# result2=file_rdd.map(lambda ele:ele.split('\t')[2]).map(lambda ele:(ele,1)).reduceByKey(lambda a,b:a+b).\
#     sortBy(lambda ele:ele[1],ascending=False,numPartitions=1).take(3)
# print(result2)

# c.static the time of "黑马程序员" then out the top 3
#1.map from str to list([20,c语言]) then map from the list([20,c语言]) to list([20,黑马程序员]) then
#  map from the list([20,黑马程序员]) to time str
# 2.now every element is a time str.just regard every element(time str) as word and static their numbers .
# 3.sort
result3=file_rdd.map(lambda ele:ele.split('\t')).map(lambda ele :[ele[0][0:2],ele[2]]).filter(lambda ele:ele[1]=='黑马程序员').\
    map(lambda ele:(ele[0],1)).reduceByKey(lambda a,b:a+b).sortBy(lambda ele:ele[1],ascending=False,numPartitions=1).\
    take(3)
print(result3)
# d.file to jasonFile
# map from str to dict then save as textFile
result4=file_rdd.map(lambda ele:ele.split('\t')).\
    map(lambda ele:{'time':ele[0],'id':ele[1],'keyword':ele[2],'adress':ele[5]})
result4.saveAsTextFile('E:\programme\Python\practice\search')
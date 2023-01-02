import json
import os
# 一、format the file data(file data to dict)
os.environ['PYSPARK_PYTHON']='E:/install winen/Python10/python.exe'
from pyspark import SparkConf,SparkContext
# 一、get a Environment entry object(SparkContext' instantiation)
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
sc = SparkContext(conf=conf)
rdd=sc.textFile('E:\programme\Python\practice\orders.txt')
list_data_sale=rdd.flatMap(lambda ele:ele.split('|'))
# json to dict
dict_rdd=list_data_sale.map(lambda ele:json.loads(ele))
# 二、finish requirement
# 1.requirement:get the sale of city
# dict to model kv
kv_rdd=dict_rdd.map(lambda ele:(ele['areaName'],int(ele['money'])))
# group and aggregate
city_sale=kv_rdd.reduceByKey(lambda a,b:a+b)
# 2.requirement:get the merchandise categories
# get the category,then Remove Duplicates
cate_rdd=dict_rdd.map(lambda ele:ele['category']).distinct()
print(cate_rdd.collect())
# 3.requirement:get the merchandise categories of beijing
# dict to model kv,format:(beijing,服饰),(hangzhou,食品)...
kv_rdd2=dict_rdd.map(lambda ele:(ele['areaName'],ele['category']))
# filter and get beijing
beijing_rdd=kv_rdd2.filter(lambda ele:ele[0]=='北京')
# get the category,then Remove Duplicates,the element is tuple((beijing,服饰)) in beijing_rdd
cate_rdd2=beijing_rdd.map(lambda ele:ele[1]).distinct()
print(cate_rdd2.collect())
print(dict_rdd.collect())
sc.stop()

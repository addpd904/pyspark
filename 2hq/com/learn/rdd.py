# pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark

# rdd is a data set ,like tuple,list.When we input data,we will get a rdd object
# list , set,list,dir,dict to rdd
import os
os.environ['PYSPARK_PYTHON']='E:/install winen/Python10/python.exe'
from pyspark import SparkConf,SparkContext
# 一、get a Environment entry object(SparkContext' instantiation)
conf=SparkConf().setMaster("local[*]").setAppName("spark_app")
sc = SparkContext(conf=conf)
# 二、input data
rdd1=sc.parallelize([1,2,3,4,5])
rdd2=sc.parallelize((1,2,3,4,5))
rdd3=sc.parallelize({2,3,4,5})
print(rdd2.collect())
# textfile to rdd
# rdd=sc.textFile('E:\programme\Python\practice\Fesaledata.txt')
# print(rdd.collect())
# 三、rdd‘s function
# 1.map()
rdd4=sc.parallelize([88,2,3,4,5])
def func1(ele):
    return ele*10
rdd5=rdd4.map(func1)
# 2，unnest,flatMap.require:the value that lambda function return is a list
# and the flatMap will unnest
rdd6=sc.parallelize([[1,2,3],[4,5,5]])
rdd7=rdd6.flatMap(lambda x:x)
print(rdd7.collect())
# 3.reduceByKey,function:group and aggregate.data model:kv(key value)
rdd6=sc.parallelize([('nan',100),('nan',79),('nan',59),('nv',89),('nv',99)])
rdd7=rdd6.reduceByKey(lambda x,y : x+y)
print(rdd7.collect())
# 4.filter,filtration.If lambda function return True,the element will be reserved.
# foe example,reserve even number
rdd8=sc.parallelize([1,2,3,4,5,6])
rdd9=rdd8.filter(lambda num:num%2==0)
print(rdd9.collect())
# 5.distinct ,Remove Duplicates
rdd10=sc.parallelize([1,1,2,2,3,3,4,5,6,7])
rdd11=rdd10.distinct()
print(rdd11.collect())
# 6.sortBy,sort.according to specific element to sort
# numPartitions means partition
rdd12=sc.parallelize([('zs',66),('ls',99),('ws',86),('zjl',68)])
result=rdd12.sortBy(lambda ele:ele[1],ascending=True,numPartitions=1)
print(result.collect())
# 7.
sc.stop()
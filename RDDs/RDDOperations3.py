'''Examples in this modules'''
# 1. RDD.persist
# 2. RDD.reduceByKey


import pyspark
from pyspark import SparkContext

sc = SparkContext("local", "First App")
print(sc.appName)

pyspark.RDD.persist
rdd = sc.parallelize(["b", "a", "c"])
print(rdd.persist().is_cached)

from operator import add
print(sc.parallelize([1, 2, 3, 4, 5]).reduce(add))

# pyspark.RDD.reduceByKey
# Merge the values for each key using an associative and commutative reduce function.
# This will also perform the merging locally on each mapper before sending results to a reducer,
# similarly to a “combiner” in MapReduce.

from operator import add
rdd = sc.parallelize([("a", 1), ("b", 1), ("a", 1)])
print(sorted(rdd.reduceByKey(add).collect()))
# [('a', 2), ('b', 1)]

x = sc.parallelize([("a", 1), ("b", 4)])
y = sc.parallelize([("a", 2),("c",3)])
print(sorted(y.fullOuterJoin(x).collect()))
# [('a', (2, 1)), ('b', (None, 4))]
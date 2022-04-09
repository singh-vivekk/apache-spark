'''Examples in this modules'''
# 1. Map Operation
# 2. mapPartitions
# 3. mapValues
# 4. max
# 5. min
# 6. mean

from pyspark import SparkContext,SparkConf

def RDDOperations2():
    sc = SparkContext("local", "RDD app")

    rdd = sc.parallelize(["b", "a", "c"])
    sorted(rdd.map(lambda x: (x, 1)).collect())
    # [('a', 1), ('b', 1), ('c', 1)]

# RDD.mapPartitions(f, preservesPartitioning=False)
# Return a new RDD by applying a function to each partition of this RDD

    rdd = sc.parallelize([1, 2, 3, 4], 2)
    def f(iterator): yield sum(iterator)
    rdd.mapPartitions(f).collect()
    # [3, 7]

# RDD.mapValues(f)
# Pass each value in the key-value pair RDD through a map function without changing the keys;
# this also retains the original RDD’s partitioning.

    x = sc.parallelize([("a", ["apple", "banana", "lemon"]), ("b", ["grapes"])])
    def f(x): return len(x)
    x.mapValues(f).collect()
    # [('a', 3), ('b', 1)]

# RDD.max(key=None)[source]
# Find the maximum item in this RDD.

    rdd = sc.parallelize([1.0, 5.0, 43.0, 10.0])
    print(rdd.max())
    # 43.0
    # rdd.max(key=str)
    # 5.0

    rdd = sc.parallelize([2.0, 5.0, 43.0, 10.0])
    print(rdd.min())
    # 2.0
    # rdd.min(key=str)
    # 10.0

# RDD.mean()
# Compute the mean of this RDD’s elements
    print("Mean Values : ")
    print(sc.parallelize([1, 2, 8]).mean())


if __name__=='__main__':
    RDDOperations2()
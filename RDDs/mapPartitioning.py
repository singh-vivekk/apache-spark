from pyspark import SparkContext

print(" --------------  Start of execution ------------- ")

sc=SparkContext("local[*]", "My First App")

# sc.setLogLevel("INFO")
# sc.setLogLevel("WARN")
# sc.setLogLevel("ERROR")
# sc.setLogLevel("DEBUG")


# ------ To get the application ID of Running job --------
print("Application ID of the running job is : ",sc.applicationId)

# ----- To create RDD from a list ------
my_list=range(1,100)
rdd1=sc.parallelize(my_list)

# ------ To get the no. of partitions of RDD --------
print("The no. of partitions in rdd1 is : ", rdd1.getNumPartitions())
print("The partition size of rdd1 is : ", rdd1.partitions.size)

def fun1(index, itr) : yield (index, len(list(itr)))

rdd2=rdd1.mapPartitionsWithIndex(fun1)

print(rdd2.collect())


print(" --------------  End of execution ------------- ")
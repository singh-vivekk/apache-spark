from pyspark import SparkContext
from pyspark.sql.session import SparkSession
import argparse

parser = argparse.ArgumentParser(description='To get the count of records')
parser.add_argument("-n","--number", type=int, help=('enter the numbers of item to print'))
parser.add_argument("-s","--salary", type=int, help=('Emp salary'))
pars=parser.parse_args()

def BasicRDD():
    sc = SparkContext("local[*]","Spark-Application")
    print(sc.appName)
    sc.setLogLevel("ERROR")

# Scenario 1: create rdd from in memory data:
    data=range(2,100,2)
    print(type(data))
    print("*********** Creating RDD from in-memory object *************")
    in_mem_rdd = sc.parallelize(data)

    print(type(in_mem_rdd))
    print(in_mem_rdd.take(pars.number))     #Action
    print(in_mem_rdd.count())           #Action
    print("#"*pars.height)
    print("Height is - ", pars.height)

    # for i in in_mem_rdd.take(5):
    #     print(i)
    #
    # print(in_mem_rdd.collect())
    # print(type(in_mem_rdd))
    # print("*********** End of create RDD from in-memory object **********\n\n")

# Scenario 2: create rdd from a file:

    # print("***********  Creating RDD from file  *************")
    # file_rdd=sc.textFile("../data/2004.csv")       # wholeTextFile (directory)
    # file_rdd = sc.textFile("../data/for_DF.txt")  # wholeTextFile (directory)
    # print(file_rdd.take(10))
    # print(file_rdd.count())
    # for i in file_rdd.take(15):
    #     print(i)
    # print("*********** End of create RDD from file **********\n\n")


    print("***********  Creating RDD from file  *************")
    # file_rdd=sc.textFile("../RDDs/emp1.csv")       # wholeTextFile (directory)
    # file_rdd = sc.textFile("../data/for_DF.txt")  # wholeTextFile (directory)

    # for i in file_rdd.take(5):
    #     print(i)

    # spark = SparkSession.builder.appName("SparkSessionBuilder-Application").enableHiveSupport().getOrCreate()
    # df=spark.read.option('header',True).option('inferSchema',True).csv("../RDDs/emp1.csv")
    # df.show()   # by default - 20 lines

    # Hive query: select city from tablename

    #1. step 1 -> createOrReplaceTempView : convert a dataframe into in-memory table
    # df.createOrReplaceTempView("table1")
    # df.printSchema()
    #
    # spark.sql("show databases").show()


    # data = range(100)
    # print(data)
    # file_rdd = sc.parallelize(data)
    #
    # print(file_rdd.take(5))
    # print(file_rdd.count())
    # print(file_rdd.glom().collect())
    # # print(l1)
    # print("no. of partitions : ", file_rdd.getNumPartitions())
    #
    # rddc=file_rdd.coalesce(3)
    # print("no. of partitions after coalesce : ", rddc.getNumPartitions())
    # print(rddc.glom().collect())
    #
    # rddr=file_rdd.repartition(3)
    # print("no. of partitions after repartitions : ", rddr.getNumPartitions())
    # print(rddr.glom().collect())
    #
    # print(" ----------------------- Done")

    # rddm=file_rdd.map(lambda x: x.split(','))
    # print("count after map operation = ", rddm.count())                     # should have same 5 records
    # print(rddm.take(5))
    # print(rddm.glom().collect())

    # rddf=file_rdd.flatMap(lambda x: x.split(','))
    # print("count after flatMap operation = ",rddf.count())                     # should have 10 records now
    # print(rddf.take(5))
    # print("*********** End of create RDD from file **********\n\n")



# Scenario 3: create rdd from a another RDD:

    # print("***********  Creating RDD from another RDD  *************\n")
    # print(in_mem_rdd.collect())
    # new_rdd = in_mem_rdd.filter(lambda x: x!=10)
    # new_rdd1=new_rdd.map(lambda y: str(y))
    # new_rdd2 = new_rdd1.filter(lambda x: x.startswith('1'))
    # print(new_rdd2.collect())
    # print(type(new_rdd))
    # print(new_rdd2.toDebugString)

    # print("-------using transformation----")
    # new_rdd1=file_rdd.filter(lambda x: x.startswith('2005'))
    # print(new_rdd1.take(10))
    # print(type(new_rdd1))
    # print(len(new_rdd1))
    # print("*********** End of create RDD from another RDD **********\n\n")

# Basic operation on RDD:

    # print("\n***** To count the number of records *********\n")
    # print(" Total no. of records in in_mem_rdd RDD :", in_mem_rdd.count())
    # print(" Total no. of records in file_rdd RDD :", file_rdd.count())
    # print(" Total no. of records in new_rdd RDD :", new_rdd.count())
    #
    # print("\n***** number of partitions of RDD *********")
    # print(" No. of partitions of in_mem_rdd RDD :", in_mem_rdd.getNumPartitions())
    # print(" No. of partitions of file_rdd RDD :", file_rdd.getNumPartitions())
    # print(" No. of records in file_rdd RDD :", file_rdd.count())

    # file_rdd1=file_rdd.repartition(10)
    # print(" No. of partitions of file_rdd1 RDD :", file_rdd1.getNumPartitions())

    # file_rdd2=file_rdd.coalesce(10)
    # print(" No. of partitions of file_rdd2 RDD :", file_rdd2.getNumPartitions())
    #
    # print(file_rdd2.toDebugString())

    # xyz=file_rdd.collect()
    #
    # abc=[]
    # for row in xyz:
    #     abc.append(row['id'], row['1'], row['2'])
    #
    # print(type(abc))
    # print(abc)

BasicRDD()


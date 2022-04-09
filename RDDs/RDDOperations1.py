#Spark Basic  // spark-submit
# import pyspark
from pyspark import SparkContext,SparkConf

def BasicRDD():
    # sc = SparkContext("local", "RDD app")
    # spark=SparkSession.builder.appName("Basics of Spark").getOrCreate()

    #sc with custom configurations
    conf = (SparkConf().setMaster("local[*]").setAppName("Sweta_Spark_App"))
    sc = SparkContext(conf=conf)

    print(sc.appName)

    # print("-----------Creating RDD from in-memory data: Started--------------------")
    # data=range(1,10)
    # data_RDD = sc.parallelize(data)
    # print(data_RDD.take(5))
    # print("-----------Creating RDD from in-memory data: Finished--------------------")

#From Files: Example Word count
    log_file="file:///C:/Users/Administrator/PycharmProjects/pysparkProject/SparkBasics/data/wc.csv"
    rdd1=sc.textFile(log_file)

    for i in rdd1.collect():
        print(i)


    # Take out few columns:
    mapRdd=rdd1.map(lambda s: s.split(','))
    print(mapRdd)
    rddCol=mapRdd.map(lambda x: (x[1],x[3]))
    print(rddCol.collect())

    for i in rddCol.collect():
        print(i)

    # print(rdd1.flatMap(lambda s: s.split(',')).map(lambda m: (m,1)).reduceByKey(lambda v1,v2: v1+v2).collect())

    # rdd2=rdd1.flatMap(lambda s: s.split(',')).map(lambda x: x.strip())
    #
    # for i in rdd2.take(10):
    #     print(i)


    # print(" \n After flatMap operation ")
    # for i in rdd2.collect():
    #     print(i)

    #
    # rdd3=rdd2.map(lambda m: (m,1))
    #
    # for i in rdd3.collect():
    #     print(i)
    #
    # print("upto map operation - step 2")
    #
    # rdd4=rdd3.reduceByKey(lambda v1,v2: v1+v2)
    #
    # for i in rdd4.collect():
    #     print(i)

    # print("------Print RDD1----------")
    # print(rdd4.collect())
    # print("------Print RDD2 - After FlatMap and Split----------")
    # rdd3=rdd2.collect()
    # print(rdd3)
    # for i in rdd3:
    #     print(i)

    # print("-------------------print RDD3------------------")
    # print(rdd3.take(10))
    #
    # print("-----------Final output------------------------")
    # print(rdd4.take(10))
#From another RDDs:
    # rdd2=rdd1.take(3)
    # print(rdd2)
    # for i in rdd2:
    #     print(i)

#Filter operations:
    # rdd3 = rdd2.filter(lambda x: 'ID' in x)
    # print("No. of 1s in rdd2", rdd3)



    sc.stop()


if __name__=='__main__':
    BasicRDD()

from pyspark import SparkContext

def repartitioning():
    sc = SparkContext("local[2]", "Repartition App")
    print(sc.appName)

    print("***********  Creating RDD from file  *************")
    # file_rdd= sc.textFile("../data/2004.csv")
    # print(file_rdd.take(3))

    file2_rdd=sc.textFile("../data/*.txt",1)
    print(file2_rdd.collect())
    print("*********** End of create RDD from file **********\n\n")

    # print("The no. of partitions of file_rdd before Repartitioning is : ", file_rdd.getNumPartitions())
    print("The no. of partitions of file2_rdd before Repartitioning is : ", file2_rdd.getNumPartitions())
    # print(file2_rdd.partitions.length)
    # print(file2_rdd.partitions.size)

    # rp_file_rdd=file2_rdd.coalesce(2)
    # rp_file_rdd=file2_rdd.repartition(1)

    print("The no. of partitions of RDD after Repartitioning is : ", rp_file_rdd.getNumPartitions())



if __name__=='__main__':
    repartitioning()
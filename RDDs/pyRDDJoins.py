from pyspark import SparkContext,SparkConf


def rddJoin():
    conf = (SparkConf().setMaster("local[*]").setAppName("Sweta_Spark_App"))
    sc = SparkContext(conf=conf)

    people_rdd = sc.parallelize(["name, age, occupation",
                                 "john, 25, student",
                                 "jane, 30, scientist",
                                 "nancy, 45, teacher",
                                 "Nikhil, 21, employee"])

    transactions_rdd = sc.parallelize(["product, price, name",
                                       "toothpaste, 10, nancy",
                                       "notebook, 2, jane",
                                       "toothbrush, 4, nancy",
                                       "sandwich, 8, john"])



    # Expand each row from one long string into a list of discrete entries
    people_rdd = people_rdd.map(lambda line: line.split(', '))
    transactions_rdd = transactions_rdd.map(lambda line: line.split(', '))

    print(" ------------------ Original RDD ------------------ ")
    print(people_rdd.map(lambda x: (x[0],x[2])).collect())
    print(transactions_rdd.collect())

    # Remove header row, since RDD operations are low-level Spark operations that do not rely on schema
    people_header = people_rdd.first()
    transactions_header = transactions_rdd.first()

    print(" \n------------------ Header ------------------ \n")

    print(people_header)
    print(transactions_header)

    people_rdd = people_rdd.filter(lambda line: line != people_header)
    transactions_rdd = transactions_rdd.filter(lambda line: line != transactions_header)

    print(" ------------------ Without Header ------------------ ")
    print(people_rdd.collect())
    print(transactions_rdd.collect())
    # If you consult the Pyspark documentation, performing a .join() operation on RDDs uses a (Key, Value) paradigm to find the intersection between sets.
    # Therefore, priort to performing the join, we should format our datasets so that they conform to the (Key, Value) format required by Spark

    # Format each RDD as (K, V) to prepare for the join operation

    people_rdd = people_rdd.map(lambda line: (line[0], line[1] + ", " + line[2]))
    transactions_rdd = transactions_rdd.map(lambda line: (line[2], line[0] + ", " + line[1]))

    people_rdd.collect()
    # [('john', '25, student'), ('jane', '30, scientist'), ('nancy', '45, teacher')]

    transactions_rdd.collect()
    # [('nancy', 'toothpaste, 10'), ('jane', 'notebook, 2'), ('nancy', 'toothbrush, 4'), ('john', 'sandwich, 8')]


    # join_rdd = people_rdd.join(transactions_rdd)
    join_rdd = people_rdd.leftOuterJoin(transactions_rdd)
    print(join_rdd.toDebugString())
    print(join_rdd.collect())


if __name__=='__main__':
    rddJoin()
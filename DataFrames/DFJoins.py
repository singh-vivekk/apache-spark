#Spark Joins
import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id,lit,col,broadcast
from pyspark.sql.types import IntegerType, StructType, StructField, StringType


def basicJoin():
    spark=SparkSession.builder.appName("Basics of Spark").enableHiveSupport().getOrCreate()

    df_csv1=spark.read.option("header",True).option("InferSchema",True).format("csv").load("C:/Users/Administrator/PycharmProjects/pysparkProject/SparkBasics/data/emp1.csv")

    df_csv2=spark.read.option("header",True).option("InferSchema",True).format("csv").load("C:/Users/Administrator/PycharmProjects/pysparkProject/SparkBasics/data/emp3.csv")

    # df_csv1.show()
    # df_csv2.show()

    print("select with filter............")
    df_new=df_csv1.select(df_csv1.id.between(2,4))
    df_new.show()

    df_join=df_csv1.join(df_csv2, 'id','inner')    #Deafult is Inner. We can use any of the joins
    # Supported join types include: 'inner', 'outer', 'full', 'fullouter', 'full_outer', 'leftouter', 'left', 'left_outer', 'rightouter', 'right', 'right_outer', 'leftsemi', 'left_semi', 'semi', 'leftanti', 'left_anti', 'anti', 'cross'.
    # df_join = df_csv1.join(df_csv2, df_csv1.ID==df_csv2.ID).drop(df_csv2.ID)
    df_join.show()

#physical plan
    # print(df_join.explain())

# Broadcast Join
#     The syntax for PySpark Broadcast Join function is:
    d = df_csv1.join(broadcast(df_csv2),"id","leftouter")
    d.show()
    print(d.explain())

if __name__=='__main__':
    basicJoin()

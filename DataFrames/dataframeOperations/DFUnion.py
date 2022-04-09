'''
PySpark union() and unionAll() transformations
are used to merge two or more DataFrame’s of the same schema or structure
unionAll() is deprecated since Spark “2.0.0” version and replaced with union()
'''

from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder

class DFUnion():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        # self.sc.setLogLevel("INFO")
        print("\nfrom DF DFUnion class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)

    def createDataFrames(self):
        data1 = [('Watermelon','Green','Round'), ('Papaya','Yellow','Oval'), ('Mango','Yellow','Oval'), ('Watermelon', 'Green', 'Round')]
        data2 = [('Watermelon', 'Green', 'Round'), ('Papaya', 'Yellow', 'Oval'),('Plums', 'Red', 'Round'), ('Watermelon', 'Green', 'Round')]

        df1 = self.spark.createDataFrame(data1).toDF('name','color','shape')
        df2 = self.spark.createDataFrame(data2).toDF('name', 'color', 'shape')

        df1.show()
        df2.show()
        return df1, df2


    def dfUnion(self, df1, df2):
        ########## Intersect of two dataframe in pyspark
        print(" \n\n Union Operations : \n\n ")
        df_inter = df1.union(df2)
        df_inter.show()
        # df_inter.distinct().show()
        print(" \n\n Union All Operations : \n\n ")
        df_interAll = df1.unionAll(df2)
        df_interAll.show()


obj = DFUnion()
x, y = obj.createDataFrames()
obj.dfUnion(x,y)
'''
Intersect of two dataframe in pyspark can be accomplished using intersect() function.
Intersection in Pyspark returns the common rows of two or more dataframe.
Intersect removes the duplicate after combining.
Intersect all returns the common rows from the dataframe with duplicate.
'''

from SparkBasics.DataFrames.DataFrame_create.sparkSessionBuilder import SparkSessionBuilder

class DFIntersect():

    def __init__(self):
        # pass
        self.spark = SparkSessionBuilder().spark
        self.sc = SparkSessionBuilder().sc
        # self.sc.setLogLevel("INFO")
        print("\nfrom DF Intersect class...")
        print("Spark Session Object ID : ", self.spark)
        print("Spark Application ID : ", self.spark.sparkContext.applicationId)

    def createDataFrames(self):
        data1 = [('Watermelon','Green','Round'), ('Papaya','Yellow','Oval'), ('Mango','Yellow','Oval'), ('Watermelon', 'Green', 'Round')]
        data2 = [('Watermelon', 'Green', 'Round'), ('Papaya', 'Yellow1', 'Oval'),('Plums', 'Red', 'Round'), ('Watermelon', 'Green', 'Round')]

        df1 = self.spark.createDataFrame(data1).toDF('name','color','shape')
        df2 = self.spark.createDataFrame(data2).toDF('name', 'color', 'shape')

        df1.show()
        df2.show()
        return df1, df2


    def dfIntersect(self, df1, df2):
        ########## Intersect of two dataframe in pyspark
        print(" \n\n Interset Operations : \n\n ")
        df_inter = df1.intersect(df2)
        df_inter.show()
        print(" \n\n Interset All Operations : \n\n ")
        df_interAll = df1.intersectAll(df2)
        df_interAll.show()


####Achieve Same Result with Intersect and filter like Inner Joins:#######
        # df_inter = df1.select("name").intersect(df2.select("name"))
        # df_inter.show()
        # abc = df_inter.rdd.map(lambda x: x[0]).collect()
        # print(abc)
        # df1.filter(df1.name.isin(abc)).show()



obj = DFIntersect()
x, y = obj.createDataFrames()
obj.dfIntersect(x,y)
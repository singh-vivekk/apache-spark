from pyspark.context import SparkContext
from pyspark.sql.session import SparkSession

class Singleton():
    # classic implementation of Singleton Design pattern

    __shared_instance = 'GeeksforGeeks'

    @staticmethod
    def getInstance():

        """Static Access Method"""
        if Singleton.__shared_instance == 'GeeksforGeeks':
            Singleton()
        return Singleton.__shared_instance

    def __init__(self):

        """virtual private constructor"""
        print("init : ",Singleton.__shared_instance)
        if Singleton.__shared_instance != 'GeeksforGeeks':
            raise Exception("This class is a singleton class !")
        else:
            print("new instance")
            # Singleton.__shared_instance = self
        Singleton.__shared_instance = self

    def createContext(self):
        sc = SparkContext("local", "SparkContextBuilder-Application")
        spark = SparkSession.builder.appName("SparkSessionBuilder-Application").getOrCreate()

        self.sc, self.spark = sc, spark
        return spark


# main method
if __name__ == "__main__":
    # create object of Singleton Class
    obj = Singleton()
    print("main 1: " , obj)

    # pick the instance of the class
    obj1 = Singleton.getInstance()
    print("main 1-2 : " ,obj1)


    obj2=Singleton().getInstance()
    print("main 2: " ,obj2)

   # return self.spark, self.sc

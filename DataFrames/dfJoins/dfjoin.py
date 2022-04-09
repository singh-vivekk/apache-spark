from SparkBasics.DataFrames.DataFrame_create.createDataFrame import CreateDF
from pyspark.sql.functions import broadcast, sum, avg


class DFJoin():

    def dfInnerJoin(self, df1, df2):
        dfi=df1.join(df2, df1.age==df2.age)

        print("output of inner join")

    def dfLeftOuterJoin(self, df1, df2):
        dfl=df1.join(df2, df1.age==df2.age, 'left')
        print("output of Left Outer join")

    def dfRightOuterJoin(self, df1, df2):
        dfr=df1.join(df2, df1.age==df2.age, 'right')
        dfr.show()
        print("output of Right Outer join")

    def dfRightOuterJoinx(self, df1, df2):
        dfr=df1.join(df2, "age", 'right')
        dfr.show()
        print("output of Right Outer join")

    def dfRightOuterJoin1(self, df1, df2):
        dfr=df1.join(df2, df1.age==df2.age, 'right').select(df1.age,df2.name, df1.city)
        dfr.show()
        print("output of Right Outer join")
        dfr.select("age", "name").show()

    def dfRightOuterJoin2(self, df1, df2):
        dfr=df1.join(df2, df1.age==df2.age, 'right').drop(df1["name"],df2["city"])
        dfr.show()
        print("output of Right Outer join")
        dfr.select("age", "name").show()

    def dfRightOuterJoin3(self, df1, df2):
        df2 = df2.withColumnRenamed("age","eage").withColumnRenamed("name","ename").withColumnRenamed("city","ecity")
        df2.show()
        dfr=df1.join(df2, df1.age==df2.eage, 'right')
        dfr.show()
        print("output of Right Outer join")
        dfr.select("age", "name").show()

# 3 ways to deal with ambiguous column name error:
# if column name is same in both the dfs - use column name in condition as string.
# either select at the join time only.
# by Renaming Columns

# anti join:
    def dfAntiJoin(self, df1, df2):
        dfa=df1.join(df2, df1.age==df2.age, 'anti')
        dfa.show()

        dfa21=df2.join(df1, df1.age==df2.age, 'anti')
        dfa21.show()

        print("output of Anti join")

# Cross join:
        def dfCrossJoin(self, df1, df2):
            dfa = df1.crossJoin(df2)
            dfa.show()


# Broadcast Join: Optimization : MapSide Join : Distributed Cache
# 10 MB
    def dfBroadcastJoin(self, df1, df2):
        dfb=df1.join(broadcast(df2), df1.age==df2.age)
        dfb.show()









#  Method calls
objCreateDF = CreateDF()
df1, df2=objCreateDF.createTupleDF(1)
df1.show()
df2.show()

dfz=df1.agg(avg(df1.age))

dfz.show()


obj = DFJoin()
# obj.dfInnerJoin(df1,df2)
# obj.dfLeftOuterJoin(df1,df2)
# obj.dfCrossJoin(df1,df2)


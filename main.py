from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions  import *

conf = SparkConf().setAppName("simpleApp").setMaster("spark://spark-master:7077")
sc = SparkContext(conf=conf)
spark = SparkSession.builder.config(conf=sc.getConf()).getOrCreate()
schema = StructType(
    [
        # At the 3rd variable, set nullable or not
        StructField("Transaction", IntegerType(), True),
        StructField("Item", StringType(), True),
        StructField("date_time", StringType(), True),
        StructField("period_day", StringType(), True),
        StructField("weekday_weekend", StringType(), True)
    ]
)
spark.sql("set spark.sql.legacy.timeParserPolicy=LEGACY")

csv_path = "./data/bread_basket.csv"
csv_df = (
    spark.read.option("header", True)
    .option("mode", "PERMISSIVE")
    .option("sep", r",")
    .option("enforceSchema", False)
    .schema(schema)
    .csv(csv_path)
)

# convert date_time into timestamp type. add a column of format_date_time
df = csv_df.withColumn("format_date_time", to_timestamp(col("date_time"), "dd-MM-yy HH:mm"))
df.createOrReplaceTempView("BreadBasket")

df.printSchema()

query = f'''
            SELECT 
                *
            FROM 
                BreadBasket 
            WHERE 
                Item = 'Coffee'
            AND
                format_date_time BETWEEN '2016-11-01 00:00' AND '2016-12-01 00:00'
            LIMIT 10
        '''

# query = f'''
#             SELECT 
#                 Item,
#                 COUNT(*) AS item_count
#             FROM 
#                 BreadBasket
#             GROUP BY
#                 Item
#             ORDER BY
#                 item_count DESC
#         '''

df = spark.sql(query)
df.show()

# df.groupBy("Item").count().sort("count", ascending=False).show()

# spark.stop()

# if __name__ == "__main__":
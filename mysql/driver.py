from connections import SparkMySQLConnector
from pyspark import SparkContext as sc

if __name__ == "__main__":
    spark_context = sc('local','example')
    connection = SparkMySQLConnector(spark_context,
                                     "104.196.43.177",
                                     "spark",
                                     "spark",
                                     "spark",
                                     "senthil")

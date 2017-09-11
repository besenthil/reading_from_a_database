from connections import SparkMySQLConnector
from pyspark import SparkContext as sc
from connections import FileConnection
HEADER='R G B Count'

if __name__ == "__main__":
    spark_context = sc('local','example')
    '''
    connection = SparkMySQLConnector(spark_context,
                                     "104.196.43.177",
                                     "spark",
                                     "spark",
                                     "spark",
                                     "senthil")
    '''
    file_path = 'file:///home/senthil/code/pyspark/fraud/data/skin.txt'
    connection = FileConnection(spark_context,file_path)
    data_frame=connection.read_into_df(delimiter='\t',header=True,header_string=HEADER)
    print (data_frame.first())
from connections import SparkMySQLConnector
from pyspark import SparkContext as sc,SparkConf
from connections import SparkStreamingTCPSocketConnection as sstcp
from pyspark import SQLContext as sql_ctx
from connections import FileConnection
HEADER='R G B Count'

if __name__ == "__main__":
    spark_conf = SparkConf()
    spark_conf.setMaster('local[8]')
    spark_conf.setAppName('Quadrants')
    spark_conf.set('spark.streaming.backpressure.enabled','true')

    spark_context = sc(conf=spark_conf)
    spark_context.setLogLevel('ERROR')


    '''
    db_connection = SparkMySQLConnector(spark_context,
                                     "104.196.43.177",
                                     "spark",
                                     "spark",
                                     "spark",
                                     "skin")
    file_path = 'file:///home/senthil/code/pyspark/fraud/data/skin.txt'
    sql_context = sql_ctx(spark_context)
    connection = FileConnection(spark_context,sql_context,file_path)
    data_frame=connection.read_into_df(delimiter='\t',header=True,header_string=HEADER)
    #data_frame.registerTempTable("accidents")
    #print (data_frame.count())
    print(sql_context.sql("""
                            SELECT 
                                Year(TO_DATE(CAST(UNIX_TIMESTAMP(Date, 'dd/MM/yyyy')AS TIMESTAMP))) as DATE  , 
                                count(*) 
                            FROM 
                                accidents 
                            GROUP BY 
                                Year(TO_DATE(CAST(UNIX_TIMESTAMP(Date, 'dd/MM/yyyy')AS TIMESTAMP)))
                            ORDER BY
                                2 desc """
                          ).show())
    
    #db_connection.write_df_into_table(data_frame,'skin')
    df_skin=db_connection.get_dataframe()
    df_skin.registerTempTable("skin")
    print (sql_context.sql("""
            SELECT G,COUNT(*) FROM skin group by G order by 2 desc
            """).show())


    #print (data_frame.query("select sum(Count) from skins").show())
    '''
    sstcp_obj = sstcp(spark_context,'localhost',7077,10)
    sstcp_obj.ingest()



